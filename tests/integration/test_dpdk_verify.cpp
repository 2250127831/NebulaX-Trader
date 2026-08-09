// V4 DPDK 网络后端端到端正确性测试
//   链路: benchmark(真实 UDP socket) → 内核路由到 dtap0 → net_tap vdev(rx_burst 收帧)
//         → receiver 内部剥帧头 → recv() 返回纯载荷 → MoldUdpUnpacker 拆包
//         → ByteRingParser 解析 → SPSC 消费
//   验证: 解析数据零丢失(parsed == 文件非 R 消息数) + seq 全局连续 + benchmark 完整发送
//
// DPDK 收包特性(与 io_uring UDP socket 不同):
//   收到【完整以太网帧】(与真实网卡一致, 含 IP 头之前的 MAC 头), 但 DPDKReceiver::recv()
//   内部自动剥帧头(extract_udp_payload), 返回纯 UDP 载荷 —— 测试直接用 unpacker.feed。
//
// PMD = net_tap(vdev 软件模拟真实收包): DPDK 建 vdev 时创建内核 tap(dtap0),
//   内核路由到 dtap0 的帧被 rx_burst 读到。换真实 PMD(如 mlx5/i40e) 只改 EAL 参数,
//   应用代码零改动 —— 硬件差异通过参数体现(见 docs/V4_PLAN.md)。
//
// 权限: 需要 root(DPDK 运行 + 配置 tap)。非 root 运行 → SKIP(返回 0)。
#include "core/net/dpdk_receiver.h"
#include "core/net/i_market_data_receiver.h"
#include "core/ipc/flow_control.h"
#include "core/dispatch/dispatcher.h"
#include "core/queue/spsc_byte_ring.h"
#include "core/queue/spsc_event_ring.h"
#include "market/pipeline/mold_udp_unpacker.h"
#include "market/pipeline/byte_ring_parser.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <string>
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

static constexpr uint16_t PORT = 8082;
static constexpr const char* SRC_IP = "10.0.0.2";      // dtap0 地址(内核 tap 侧)
static constexpr const char* DST_IP = "10.0.0.99";     // 静态邻居目标(路由到 dtap0)
static constexpr const char* TAP_IF = "dtap0";         // net_tap0 vdev 创建的内核 tap

static size_t count_expected_msgs(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    off_t size = lseek(fd, 0, SEEK_END);
    auto* buf = static_cast<uint8_t*>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (buf == MAP_FAILED) return 0;
    size_t pos = 0, count = 0;
    while (pos + 2 <= static_cast<size_t>(size)) {
        uint16_t body_len = static_cast<uint16_t>((buf[pos] << 8) | buf[pos + 1]);
        if (body_len < 1 || body_len > 200) { ++pos; continue; }
        size_t msg_len = 2 + body_len;
        if (pos + msg_len > static_cast<size_t>(size)) break;
        if (buf[pos + 2] == 'R') { pos += msg_len; continue; }
        ++count;
        pos += msg_len;
    }
    munmap(buf, size);
    return count;
}

int main(int argc, char* argv[]) {
    if (geteuid() != 0) {
        printf("SKIP: DPDK 验证需要 root(DPDK 运行 + 配置 tap)。非 root 环境跳过。\n");
        return 0;
    }
    if (argc < 3) {
        printf("usage: %s <itch_file> <trader_benchmark>\n", argv[0]);
        return 1;
    }
    const char* itch_file = argv[1];
    const char* bench_path = argv[2];

    // ── 清理环境: 杀残留 benchmark + 删残留共享内存 + 删残留 tap ──
    (void)!system("pkill -x trader_benchmark");
    shm_unlink(FLOW_SHM_PATH);
    (void)!system("ip link del dtap0 2>/dev/null");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // ── DPDK 接收器: net_tap0 vdev(软件 PMD, 建内核 tap dtap0) ──
    // EAL 参数: 只跑纯软件 vdev, 不扫 PCI(本机 r8169 无 DPDK PMD), 免大页。
    // --file-prefix 隔离 DPDK 运行时文件(避免测试间残留冲突)。
    auto rcv = std::make_unique<DPDKReceiver>("net_tap0", PORT,
        std::vector<std::string>{"-l", "0", "--no-pci", "--no-huge", "-m", "128",
                                 "--file-prefix", "nxtest"});
    CHECK(rcv->start());
    if (!rcv->start()) {
        printf("FAIL: DPDK start(检查 EAL 日志)\n");
        return 1;
    }

    // ── 配置 dtap0: IP + 静态邻居(免 ARP) + 关 IPv6(免噪声帧) ──
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "ip addr add %s/24 dev %s", SRC_IP, TAP_IF);
    if (system(cmd) != 0) { printf("FAIL: dtap0 配 IP 失败\n"); return 1; }
    snprintf(cmd, sizeof(cmd), "ip neigh add %s lladdr 02:00:00:00:00:01 dev %s nud permanent",
             DST_IP, TAP_IF);
    if (system(cmd) != 0) { printf("FAIL: dtap0 静态邻居失败\n"); return 1; }
    snprintf(cmd, sizeof(cmd), "sysctl -w net.ipv6.conf.%s.disable_ipv6=1 >/dev/null 2>&1",
             TAP_IF);
    (void)!system(cmd);

    // ── 共享内存 flow_control(限速握手) ──
    int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
    if (shm_fd < 0) { perror("shm_open"); return 1; }
    ftruncate(shm_fd, sizeof(FlowControl));
    auto* fc = static_cast<FlowControl*>(mmap(nullptr, sizeof(FlowControl),
        PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
    close(shm_fd);
    fc->ready.store(false, std::memory_order_release);
    fc->sent.store(0, std::memory_order_release);
    fc->received.store(0, std::memory_order_release);

    // ── 管线: 字节 ring + 拆包器 + 解析器 + 单 worker 分发器 ──
    auto ring_buf = std::make_unique<uint8_t[]>(1 << 22);
    SPSCByteRing shared_ring(ring_buf.get(), 1 << 22);
    auto* ev_slots = new MarketEvent[1 << 20];
    int wake_fd = eventfd(0, EFD_NONBLOCK);
    SPSCEventRing spsc(ev_slots, 1 << 20, wake_fd);
    auto* retry_slots = new MarketEvent[1 << 20];
    int retry_fd = eventfd(0, EFD_NONBLOCK);
    RetryBucket retry(retry_slots, 1 << 20, retry_fd);
    SPSCEventRing* spsc_arr[1] = {&spsc};
    RetryBucket* retry_arr[1] = {&retry};
    Dispatcher dispatcher(spsc_arr, retry_arr, 1);
    std::atomic<uint64_t> cared[1]{0};
    std::atomic<uint64_t> registered[1]{0};

    MoldUdpUnpacker<SPSCByteRing> unpacker(shared_ring);
    ByteRingParser parser(shared_ring, dispatcher, cared, registered);

    size_t expected = count_expected_msgs(itch_file);
    printf("期望解析消息数(文件非R消息): %zu\n", expected);

    std::atomic<bool> stop{false};
    std::atomic<bool> parse_done{false};
    std::atomic<size_t> unpacked_total{0};
    std::atomic<size_t> parsed_total{0};
    std::atomic<uint64_t> consumed{0};
    std::atomic<bool> seq_ok{true};
    std::atomic<uint64_t> last_seq{UINT64_MAX};

    // ── 接收线程: DPDK recv() 已剥帧头(内部跳过非目标帧), 返回纯载荷 → 拆包 → 计数 ──
    std::thread recv_th([&] {
        uint8_t buf[65536];
        while (!stop.load(std::memory_order_acquire)) {
            ssize_t n = rcv->recv(buf, sizeof(buf));
            if (n <= 0) { if (stop.load()) break; continue; }
            unpacked_total += unpacker.feed(buf, (size_t)n);   // buf 即 MoldUDP64 载荷
            fc->received.fetch_add(1, std::memory_order_release);   // 限速: 收一包放行一包
        }
    });

    // ── 解析线程 ──
    std::thread parse_th([&] {
        while (!stop.load(std::memory_order_acquire)) {
            size_t n = parser.parse_available();
            parsed_total += n;
            if (!parser.ring().empty()) continue;
            if (!stop.load()) parser.wait_for_data(200);
        }
        parsed_total += parser.parse_available();
        parse_done.store(true, std::memory_order_release);
    });

    // ── 消费线程: 验证零丢失 + seq 全局连续 ──
    std::thread consume_th([&] {
        MarketEvent ev;
        while (!parse_done.load(std::memory_order_acquire) || spsc.pending() > 0) {
            if (spsc.pop(ev)) {
                ++consumed;
                uint64_t l = last_seq.load(std::memory_order_relaxed);
                if (l != UINT64_MAX && ev.seq_id != l + 1) {
                    seq_ok.store(false, std::memory_order_relaxed);
                    printf("FAIL seq 不连续: 期望 %llu 实际 %llu\n",
                           (unsigned long long)(l + 1), (unsigned long long)ev.seq_id);
                }
                last_seq.store(ev.seq_id, std::memory_order_relaxed);
            } else if (!parse_done.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            } else break;
        }
    });

    // ── fork benchmark → 10.0.0.99:PORT(经 dtap0 路由到 net_tap) ──
    fc->ready.store(true, std::memory_order_release);
    printf("DPDK 就绪, fork benchmark → %s:%u...\n", DST_IP, (unsigned)PORT); fflush(stdout);

    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        char port_arg[16];
        snprintf(port_arg, sizeof(port_arg), "%u", (unsigned)PORT);
        // --pack-max 8: 控制单帧 < mbuf 数据区(默认 RTE_MBUF_DEFAULT_BUF_SIZE ~2KB)。
        //   8 条 × 202B(最大) ≈ 1.6KB + 42B 头, 与真实 MTU 语义一致。
        execl(bench_path, bench_path, "--file", itch_file,
              "--host", DST_IP, "--port", port_arg, "--pack-max", "8", (char*)nullptr);
        _exit(127);
    }

    int wstatus = 0;
    auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (waitpid(pid, &wstatus, WNOHANG) == 0
           && std::chrono::steady_clock::now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (waitpid(pid, &wstatus, WNOHANG) == 0) {
        printf("benchmark 超时, kill\n"); fflush(stdout);
        kill(pid, SIGKILL);
        waitpid(pid, &wstatus, 0);
    }
    bool bench_ok = WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0;
    printf("benchmark 子进程 exit: %s\n", bench_ok ? "0 (OK)" : "失败");
    fflush(stdout);

    // ── 停止: 打断接收 → 解析排空 → 消费排空 ──
    rcv->stop();
    stop.store(true, std::memory_order_release);
    recv_th.join();
    parser.notify();
    parse_th.join();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    consume_th.join();

    fc->ready.store(false, std::memory_order_release);

    // ── 验证 ──
    printf("\n=== DPDK 端到端验证 ===\n");
    printf("期望消息: %zu\n", expected);
    printf("拆包消息: %zu\n", unpacked_total.load());
    printf("解析消息: %zu\n", parsed_total.load());
    printf("消费消息: %llu\n", (unsigned long long)consumed.load());
    printf("seq 连续: %s\n", seq_ok.load() ? "yes" : "NO");

    CHECK(bench_ok);
    CHECK(parsed_total.load() == expected);
    CHECK(consumed.load() == expected);
    CHECK(seq_ok.load());

    delete[] ev_slots;
    close(wake_fd);
    delete[] retry_slots;
    close(retry_fd);
    munmap(fc, sizeof(FlowControl));
    shm_unlink(FLOW_SHM_PATH);
    (void)!system("ip link del dtap0 2>/dev/null");

    if (g_failures == 0) {
        printf("\nDPDK 端到端验证 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
