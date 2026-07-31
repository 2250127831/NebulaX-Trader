// 端到端验证测试：真实压测脚本 trader_benchmark 发送 + IoUringReceiver 接收
//
// 流程：
//   1. 用 IoUringReceiver 绑定测试端口，置 flow_control ready
//   2. fork + exec trader_benchmark 子进程，发送 ITCH 小文件
//   3. 接收全部字节，与"benchmark 期望流"逐字节比对
//   4. 期望流 = 用与 benchmark 相同的 MSG_BODY_LEN 解析逻辑从源文件重建
//
// 期望流由 CMake 测试前用脚本生成，或本测试运行时用内置逻辑重建。
// 本测试直接重建期望流，避免依赖额外生成文件。

#include "core/net/i_market_data_receiver.h"
#include "core/net/io_uring_receiver.h"
#include "core/ipc/flow_control.h"

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

// ── 用与 benchmark 相同的长度前缀逻辑重建期望流 ──
// ITCH 5.0: 2 字节 big-endian 长度前缀 = 含 type 的消息体长度（不含前缀）。
// 完整消息 = 2 字节前缀 + length。与 trader_benchmark 发送逻辑保持一致。
static std::vector<uint8_t> rebuild_expected(const std::vector<uint8_t>& src) {
    std::vector<uint8_t> out;
    size_t pos = 0;
    while (pos + 2 <= src.size()) {
        uint16_t body_len = (static_cast<uint16_t>(src[pos]) << 8) | src[pos+1];
        if (body_len < 1 || body_len > 200) { ++pos; continue; }  // 损坏前缀，跳过
        size_t msg_len = 2 + body_len;
        if (pos + msg_len > src.size()) break;
        out.insert(out.end(), src.begin()+pos, src.begin()+pos+msg_len);
        pos += msg_len;
    }
    return out;
}

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

// ── 接收端工厂：按后端名实例化，测试主体只操作抽象接口 ──
// 换后端（如 V4 的 AF_XDPReceiver / V6 的 DPDKReceiver）只需改这里，
// 测试其余代码通过 IMarketDataReceiver* 操作，一行不改。
static std::unique_ptr<IMarketDataReceiver> make_receiver(
    const std::string& backend, uint16_t port) {
    if (backend == "io_uring") {
        return std::make_unique<IoUringReceiver>(port);
    }
    printf("未知后端: %s\n", backend.c_str());
    return nullptr;
}

int main(int argc, char* argv[]) {
    // 参数：<itch_file> <itch_port> <benchmark_path>
    if (argc < 4) {
        printf("usage: %s <itch_file> <port> <trader_benchmark> [backend]\n"
               "  backend: io_uring (默认) | ...\n", argv[0]);
        return 1;
    }
    std::string itch_file = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    std::string bench_path = argv[3];
    std::string backend = (argc >= 5) ? argv[4] : "io_uring";

    // ── 读源文件 + 重建期望流 ──
    FILE* f = fopen(itch_file.c_str(), "rb");
    CHECK(f != nullptr);
    if (!f) return 1;
    fseek(f, 0, SEEK_END); long fsize = ftell(f); fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> source(fsize);
    CHECK(fread(source.data(), 1, fsize, f) == (size_t)fsize);
    fclose(f);
    std::vector<uint8_t> expected = rebuild_expected(source);
    printf("源文件 %ld 字节, benchmark 期望发送 %zu 字节\n", fsize, expected.size());
    CHECK(expected.size() > 0);

    // ── 接收端：通过抽象接口 IMarketDataReceiver 操作 ──
    // 阻塞模式：阻塞 recv 无 SQE 提交空窗期，避免高速丢包。
    auto receiver = make_receiver(backend, port);
    CHECK(receiver != nullptr);
    if (!receiver) return 1;
    CHECK(receiver->start());
    if (!receiver->start()) return 1;

    // ── 共享内存 flow_control ──
    int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
    CHECK(shm_fd >= 0);
    if (shm_fd < 0) return 1;
    CHECK(ftruncate(shm_fd, sizeof(FlowControl)) == 0);
    auto* fc = (FlowControl*)mmap(nullptr, sizeof(FlowControl),
                                  PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    CHECK(fc != MAP_FAILED);
    if (fc == MAP_FAILED) return 1;

    // ── 接收线程 + 就绪屏障 ──
    // 接收线程先以非阻塞模式预热一次 recv：submit SQE 进内核后，
    // 置 ready 屏障。主线程等屏障后才 fork 子进程——保证无论接收端
    // 准备多久，子进程都不可能早于"内核已在收包"之前发包。
    struct RecvState {
        std::vector<uint8_t> received;
        size_t packets = 0;
        std::atomic<bool> armed{false};  // 就绪屏障：SQE 已提交进内核
        std::atomic<bool> done{false};   // 接收完成（收满或被打断）
    };
    RecvState st;
    st.received.reserve(expected.size() + 1024);

    std::thread recv_th([&]() {
        // 预热：非阻塞提交 recv SQE，确保内核已开始收包
        receiver->set_blocking(false);
        uint8_t pre[65536];
        receiver->recv(pre, sizeof(pre));   // submit SQE，无数据返回 0
        receiver->set_blocking(true);

        st.armed.store(true, std::memory_order_release);  // 放行主线程 fork

        // 阻塞收满期望长度；stop() 打断时 recv 返回 0 而退出
        uint8_t buf[65536];
        while (st.received.size() < expected.size()) {
            ssize_t n = receiver->recv(buf, sizeof(buf));
            if (n > 0) {
                ++st.packets;
                st.received.insert(st.received.end(), buf, buf + n);
            } else {
                break;   // n==0: stop() 打断；n<0: 错误
            }
        }
        st.done.store(true, std::memory_order_release);
    });

    // ── 主线程：等接收就绪（屏障）后再 fork ──
    while (!st.armed.load(std::memory_order_acquire)) sched_yield();

    fc->ready.store(true, std::memory_order_release);
    printf("接收端就绪（SQE 已提交），fork benchmark...\n");

    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        // 子进程：exec benchmark（会等 ready，读到后立即发送）
        char port_arg[16], backlog_arg[16];
        snprintf(port_arg, sizeof(port_arg), "%u", port);
        snprintf(backlog_arg, sizeof(backlog_arg), "%d", 5000);
        execl(bench_path.c_str(), bench_path.c_str(),
              "--file", itch_file.c_str(),
              "--port", port_arg,
              "--backlog", backlog_arg,
              (char*)nullptr);
        _exit(127);  // exec 失败
    }

    // ── 主线程：等接收线程完成（超时则 stop() 打断阻塞 recv）──
    auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (!st.done.load(std::memory_order_acquire)
           && std::chrono::steady_clock::now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!st.done.load()) {
        printf("接收超时，强制停止接收线程\n");
        receiver->stop();   // 打断阻塞中的 recv（返回 0），接收线程随之退出
    }
    recv_th.join();

    // 收集 benchmark 子进程退出状态
    int wstatus = 0;
    waitpid(pid, &wstatus, 0);
    fc->ready.store(false, std::memory_order_release);
    bool bench_ok = WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0;
    printf("benchmark 子进程 exit: %s\n", bench_ok ? "0 (OK)" : "失败");

    std::vector<uint8_t>& received = st.received;
    size_t packets = st.packets;

    // ── 逐字节比对 ──
    printf("\n=== 端到端比对 ===\n");
    printf("UDP 包数:   %zu\n", packets);
    printf("接收字节:   %zu\n", received.size());
    printf("期望字节:   %zu\n", expected.size());

    size_t min_len = (received.size() < expected.size()) ? received.size() : expected.size();
    size_t mismatch = 0, first_bad = (size_t)-1;
    for (size_t i = 0; i < min_len; ++i) {
        if (received[i] != expected[i]) {
            ++mismatch;
            if (first_bad == (size_t)-1) first_bad = i;
        }
    }
    size_t len_diff = (received.size() > expected.size())
        ? received.size() - expected.size()
        : expected.size() - received.size();

    CHECK(bench_ok);
    CHECK(mismatch == 0);
    CHECK(len_diff == 0);

    if (mismatch > 0) {
        printf("失配: %zu, 首个失配偏移: %zu\n", mismatch, first_bad);
        printf("  期望: ");
        for (size_t i = 0; i < 16 && first_bad+i < expected.size(); ++i)
            printf("%02x ", expected[first_bad+i]);
        printf("\n  收到: ");
        for (size_t i = 0; i < 16 && first_bad+i < received.size(); ++i)
            printf("%02x ", received[first_bad+i]);
        printf("\n");
    }

    receiver->stop();
    if (g_failures == 0) {
        printf("\n端到端验证 PASS ✓ (%zu 字节逐字节一致)\n", expected.size());
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
