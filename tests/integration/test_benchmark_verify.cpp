// 端到端验证测试：benchmark 发送 MoldUDP64 包 + 接收端完整解析链路
//
// 流程：
//   1. IoUringReceiver 收 UDP 包
//   2. 接收线程: 每收一包 → MoldUdpUnpacker 拆包(读 seq, 每条消息前加 seq)
//      → 写 fc->received(包数, 与 benchmark 限速同步)
//   3. 消费线程: 从共享 ring 读 [seq][len][体], 去掉 seq 拼成 [len][体] 流
//   4. 与源文件(裸 ITCH)逐字节比对
//
// 验证:
//   - 数据准确性: 去 seq 后的流 == 源文件(逐字节)
//   - 完整性: seq 全局连续(无丢包), 最大 seq 合理
//   - 限速: sent/received 包数积压(现有 FlowControl)

#include "core/ipc/flow_control.h"
#include "core/queue/queue_manager.h"
#include "core/queue/spsc_byte_ring.h"
#include "core/net/i_market_data_receiver.h"
#include "core/net/io_uring_receiver.h"
#include "market/pipeline/byte_ring_parser.h"
#include "market/pipeline/mold_udp_unpacker.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <thread>

#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static std::unique_ptr<IMarketDataReceiver> make_receiver(
    const std::string& backend, uint16_t port) {
    if (backend == "io_uring") {
        return std::make_unique<IoUringReceiver>(port);
    }
    printf("未知后端: %s\n", backend.c_str());
    return nullptr;
}

// 从共享 ring 读一条消息: [seq 2][len 2][体]。返回消息体(去 seq)。
// 返回 0 表示暂无完整消息。出参 seq_out 填消息序号。
static size_t read_one_msg(SPSCByteRing& ring, uint8_t* body_out, uint64_t& seq_out) {
    const uint8_t* p;
    size_t n = ring.read_acquire(reinterpret_cast<const void*&>(p), 4);
    if (n < 4) return 0;  // 头不足, 等
    uint16_t seq  = (static_cast<uint16_t>(p[0]) << 8) | p[1];
    uint16_t body_len = (static_cast<uint16_t>(p[2]) << 8) | p[3];
    if (body_len < 1 || body_len > 200) { ring.read_release(1); return 0; }  // 损坏

    size_t msg_len = 4 + body_len;
    size_t used = ring.tail() - ring.head();
    if (used < msg_len) return 0;  // 整条不足, 等

    n = ring.read_acquire(reinterpret_cast<const void*&>(p), msg_len);
    seq_out = seq;
    if (n == msg_len) {
        memcpy(body_out, p + 4, body_len);   // 去 seq: [len 已被读, 只要体]
        ring.read_release(msg_len);
        return body_len;
    }
    // 跨回绕
    uint8_t tmp[300];
    memcpy(tmp, p, n);
    memcpy(tmp + n, ring.raw_buffer(), msg_len - n);
    memcpy(body_out, tmp + 4, body_len);
    ring.read_release(msg_len);
    return body_len;
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        printf("usage: %s <itch_file> <port> <trader_benchmark> [backend]\n", argv[0]);
        return 1;
    }
    std::string itch_file = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    std::string bench_path = argv[3];
    std::string backend = (argc >= 5) ? argv[4] : "io_uring";

    // ── 读源文件(裸 ITCH 完整消息流) ──
    FILE* f = fopen(itch_file.c_str(), "rb");
    CHECK(f != nullptr);
    if (!f) return 1;
    fseek(f, 0, SEEK_END); long fsize = ftell(f); fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> source(fsize);
    CHECK(fread(source.data(), 1, fsize, f) == (size_t)fsize);
    fclose(f);
    printf("源文件 %ld 字节\n", fsize);

    auto receiver = make_receiver(backend, port);
    CHECK(receiver != nullptr);
    if (!receiver) return 1;
    CHECK(receiver->start());
    if (!receiver->start()) return 1;

    int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
    CHECK(shm_fd >= 0);
    if (shm_fd < 0) return 1;
    CHECK(ftruncate(shm_fd, sizeof(FlowControl)) == 0);
    auto* fc = (FlowControl*)mmap(nullptr, sizeof(FlowControl),
                                  PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    CHECK(fc != MAP_FAILED);
    if (fc == MAP_FAILED) return 1;

    // ── 管道: 共享 ring + 拆包器 ──
    auto ring_buf = std::make_unique<uint8_t[]>(1 << 20);
    size_t ring_id = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING,
                                          ring_buf.get(), 1 << 20);
    auto& shared_ring = QueueManager::get<SPSCByteRing>(ring_id);
    MoldUdpUnpacker unpacker(shared_ring);

    // 消费结果: 去 seq 的消息流 + seq 连续性
    std::vector<uint8_t> parsed_stream;   // 每个消息体拼接([len][体] 已在 body_out 后拼回)
    std::atomic<bool> seq_contiguous{true};
    std::atomic<uint64_t> last_seq{0};
    std::atomic<bool> stop{false};
    std::atomic<size_t> msg_count{0};

    struct State { std::atomic<bool> armed{false}; };
    State st;

    // ── 接收线程: recv → unpacker → fc->received++ ──
    std::thread recv_th([&]() {
        receiver->set_blocking(false);
        uint8_t pre[65536];
        receiver->recv(pre, sizeof(pre));
        receiver->set_blocking(true);
        st.armed.store(true, std::memory_order_release);

        uint8_t buf[65536];
        while (!stop.load(std::memory_order_acquire)) {
            ssize_t n = receiver->recv(buf, sizeof(buf));
            if (n > 0) {
                unpacker.feed(buf, (size_t)n);
                fc->received.fetch_add(1, std::memory_order_release);
            } else break;
        }
    });

    // ── 消费线程: 从共享 ring 读 [seq][len][体], 去 seq 拼流 ──
    std::thread consume_th([&]() {
        uint8_t body[200];
        uint64_t seq;
        while (!stop.load(std::memory_order_acquire) || !shared_ring.empty()) {
            size_t n = read_one_msg(shared_ring, body, seq);
            if (n > 0) {
                // 拼回 [len][体](len 2字节 + 体 n 字节)
                parsed_stream.push_back(static_cast<uint8_t>(n >> 8));
                parsed_stream.push_back(static_cast<uint8_t>(n & 0xFF));
                parsed_stream.insert(parsed_stream.end(), body, body + n);
                ++msg_count;
                // seq 连续性
                uint64_t l = last_seq.load();
                if (l != 0 && seq != l + 1)
                    seq_contiguous.store(false, std::memory_order_relaxed);
                last_seq.store(seq, std::memory_order_relaxed);
            } else {
                if (!stop.load()) std::this_thread::yield();
            }
        }
    });

    // 主线程: 等就绪 → fork benchmark
    while (!st.armed.load(std::memory_order_acquire)) sched_yield();
    fc->ready.store(true, std::memory_order_release);
    printf("接收端就绪, fork benchmark...\n");

    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        char port_arg[16], backlog_arg[16];
        snprintf(port_arg, sizeof(port_arg), "%u", port);
        snprintf(backlog_arg, sizeof(backlog_arg), "%d", 5000);
        execl(bench_path.c_str(), bench_path.c_str(),
              "--file", itch_file.c_str(),
              "--port", port_arg,
              "--backlog", backlog_arg,
              (char*)nullptr);
        _exit(127);
    }

    // 等接收 + 消费完成
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (fc->received.load() == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stop.store(true, std::memory_order_release);
    receiver->stop();
    recv_th.join();
    consume_th.join();

    int wstatus = 0;
    waitpid(pid, &wstatus, 0);
    fc->ready.store(false, std::memory_order_release);
    bool bench_ok = WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0;
    printf("benchmark 子进程 exit: %s\n", bench_ok ? "0 (OK)" : "失败");

    // ── 验证: 去 seq 流 vs 源文件 ──
    printf("\n=== 端到端验证 ===\n");
    printf("接收包数:   %llu\n", (unsigned long long)fc->received.load());
    printf("消费消息数: %zu\n", msg_count.load());
    printf("拼接流:     %zu 字节\n", parsed_stream.size());
    printf("源文件:     %zu 字节\n", source.size());
    printf("seq 连续:   %s\n", seq_contiguous.load() ? "yes" : "NO");

    CHECK(bench_ok);
    CHECK(seq_contiguous.load());
    CHECK(parsed_stream.size() == source.size());
    if (parsed_stream.size() == source.size()) {
        size_t mismatch = 0, first_bad = (size_t)-1;
        for (size_t i = 0; i < source.size(); ++i) {
            if (parsed_stream[i] != source[i]) {
                ++mismatch;
                if (first_bad == (size_t)-1) first_bad = i;
            }
        }
        CHECK(mismatch == 0);
        if (mismatch > 0)
            printf("失配 %zu 字节, 首个偏移 %zu\n", mismatch, first_bad);
    }

    receiver->stop();
    if (g_failures == 0) {
        printf("\n端到端验证 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
