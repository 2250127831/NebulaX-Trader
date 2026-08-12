#include "core/ipc/flow_control.h"
#include "oms/ouch_order_codec.h"

#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <thread>
#include <vector>
#include <cstdlib>
#include <ctime>

#include <sched.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <endian.h>
#include <cerrno>

// ── ITCH 5.0 消息边界 ──
// 每条消息以 2 字节 big-endian 长度前缀开头（length = 含 type 的消息体长度，不含前缀本身）。
// 完整消息 = 2 字节前缀 + length。
// 发送按此边界切分，逐条作为独立 UDP 包发出。
struct Config {
    const char* file  = "test_data/itch_sample.bin";
    const char* host  = "127.0.0.1";
    int port          = 8080;
    int order_port    = 9090;   // 模拟交易所端口：收订单、回成交回报
    int order_ret_port = 9091;  // 成交回报发送目标端口(--no-shm 时用)
    uint64_t max_backlog = 10000;
    size_t pack_max  = 100;  // 每包消息条数上限（实际每包 1~pack_max 条随机）
    uint64_t rate_01s = 0;   // 每 0.1 秒发送包数(窗口限速，0 = 全速)
    int core          = 3;   // 发送线程绑核(选低中断 P 核, 避开 P0/偶数核噪声)
    bool no_shm = false;     // 不挂共享内存(压测脚本模式，与 trader 无握手)
    bool help    = false;
};

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        if      (strcmp(argv[i], "--file")    == 0 && i+1 < argc) cfg.file = argv[++i];
        else if (strcmp(argv[i], "--host")    == 0 && i+1 < argc) cfg.host = argv[++i];
        else if (strcmp(argv[i], "--port")    == 0 && i+1 < argc) cfg.port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--order-port") == 0 && i+1 < argc) cfg.order_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--order-ret-port") == 0 && i+1 < argc) cfg.order_ret_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--backlog") == 0 && i+1 < argc) cfg.max_backlog = atol(argv[++i]);
        else if (strcmp(argv[i], "--pack-max") == 0 && i+1 < argc) cfg.pack_max = atol(argv[++i]);
        else if (strcmp(argv[i], "--rate") == 0 && i+1 < argc) cfg.rate_01s = atol(argv[++i]);
        else if (strcmp(argv[i], "--core") == 0 && i+1 < argc) cfg.core = atoi(argv[++i]);
        else if (strcmp(argv[i], "--no-shm") == 0) cfg.no_shm = true;
        else cfg.help = true;
    }
    return cfg;
}

static void usage() {
    printf("Usage: trader_benchmark [options]\n"
           "  --file  <path>     ITCH binary file\n"
           "  --host <ip>        Target IP (default: 127.0.0.1)\n"
           "  --port <port>      Target UDP port (default: 8080)\n"
           "  --order-port <port> Simulated exchange port (default: 9090)\n"
           "  --backlog <n>      Max backlog before slowing down (default: 10000)\n"
           "  --pack-max <n>     Max messages per UDP packet (default: 100)\n"
           "                     实际每包 1~pack-max 条随机，模拟真实行情打包\n"
           "  --rate <n>         Per-0.1s packets sent (smooth throttle, 0=full speed)\n"
           "  --core <n>         Pin main thread to CPU n (default: 3, 低中断 P 核)\n"
           "  --no-shm           No shared-memory handshake (pressure script mode).\n"
           "                     Throttle by --rate, no recv feedback.\n");
}

// ── 模拟交易所线程(V5: TCP 全双工 + OUCH) ──
// TCP server: listen/accept trader 连接, 同一 fd 全双工。
//   读 49B 'O'(定长流分帧) → decode → 回 'A' Accepted → 回 'E' Executed(全额成交)。
// OUCH 语义: 模拟交易所对每个有效订单回 A + E(trader 侧只有 E 驱动 OMS 成交)。
static void run_sim_exchange(int order_port, FlowControl* fc, uint16_t ret_port,
                             const OuchOrderCodec& codec,
                             std::atomic<bool>& stop,
                             std::atomic<uint64_t>& orders_received) {
    int lsock = socket(AF_INET, SOCK_STREAM, 0);
    if (lsock < 0) { perror("sim_exchange socket"); return; }
    int opt = 1;
    setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in oaddr{};
    oaddr.sin_family = AF_INET;
    oaddr.sin_port   = htons(static_cast<uint16_t>(order_port));
    oaddr.sin_addr.s_addr = INADDR_ANY;
    if (bind(lsock, reinterpret_cast<sockaddr*>(&oaddr), sizeof(oaddr)) < 0) {
        fprintf(stderr, "模拟交易所 bind %u 失败: %s (端口被占用?)\n",
                order_port, strerror(errno));
        close(lsock);
        return;
    }
    if (listen(lsock, 4) < 0) {
        fprintf(stderr, "模拟交易所 listen %u 失败: %s\n", order_port, strerror(errno));
        close(lsock);
        return;
    }

    while (!stop.load(std::memory_order_acquire)) {
        // accept(带 200ms 超时, 让 stop 能及时退出)
        int csock;
        {
            struct timeval tv{0, 200000};
            setsockopt(lsock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            sockaddr_in from{};
            socklen_t flen = sizeof(from);
            csock = accept(lsock, reinterpret_cast<sockaddr*>(&from), &flen);
        }
        if (csock < 0) continue;   // 超时/无连接

        // 连接建立: 同一 fd 全双工读订单/写回报, 直到停止或对端关闭
        uint8_t buf[256];
        while (!stop.load(std::memory_order_acquire)) {
            // 读完整 'O'(定长 49B, 流分帧)
            size_t got = 0;
            while (got < OuchOrderCodec::kOrderMsgLen) {
                ssize_t n = recv(csock, buf + got, OuchOrderCodec::kOrderMsgLen - got, 0);
                if (n <= 0) break;   // 对端关闭/超时
                got += static_cast<size_t>(n);
            }
            if (got < OuchOrderCodec::kOrderMsgLen) break;   // 连接断/停止

            Order o{};
            if (!codec.decode_order(buf, got, o)) continue;
            ++orders_received;

            // OUCH 回执: 先 'A' Accepted 再 'E' Executed(全额成交, 按订单价)
            uint8_t ack[OuchOrderCodec::kAckMsgLen];
            size_t ack_len = 0;
            uint8_t exec[OuchOrderCodec::kExecMsgLen];
            size_t exec_len = 0;
            if (!codec.encode_ack(o, ack, sizeof(ack), ack_len)) continue;
            if (!codec.encode_exec(o.order_id, o.quantity, o.price, exec, sizeof(exec), exec_len))
                continue;
            ssize_t w1 = send(csock, ack, ack_len, MSG_NOSIGNAL);
            ssize_t w2 = send(csock, exec, exec_len, MSG_NOSIGNAL);
            (void)w1; (void)w2;
        }
        close(csock);
    }
    close(lsock);
    (void)fc; (void)ret_port;   // OUCH 全双工不再需要独立回报端口
}

int main(int argc, char* argv[]) {
    auto cfg = parse_args(argc, argv);
    if (cfg.help) { usage(); return 0; }

    // ── 绑核：压测客户端绑定低中断独立核(默认 CPU3), 避免与 trader 抢核(学撮合引擎) ──
    // 选核依据: 该机奇数核中断低(0.3-1M), 偶数核高(3-4M), CPU0(P0)高达4.3M噪声大。
    // 压测客户端独占一核, 发送计时稳定, 不干扰 trader 各线程。
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cfg.core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    // ── mmap 文件（小样本 / 完整日数据通吃）──
    int fd = open(cfg.file, O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }
    off_t file_size = lseek(fd, 0, SEEK_END);
    auto* buf = static_cast<uint8_t*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (buf == MAP_FAILED) { perror("mmap"); return 1; }

    // ── 共享内存 FlowControl（--no-shm 时不挂，压测脚本模式）──
    FlowControl* fc = nullptr;
    if (!cfg.no_shm) {
        int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
        if (shm_fd < 0) { perror("shm_open"); return 1; }
        if (ftruncate(shm_fd, sizeof(FlowControl)) < 0) { perror("ftruncate"); return 1; }
        fc = static_cast<FlowControl*>(mmap(nullptr, sizeof(FlowControl),
            PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
        close(shm_fd);
        if (fc == MAP_FAILED) { perror("mmap shm"); return 1; }
        // 重置共享内存计数器(避免复用上次残留值导致限速死锁)。
        // 注意: 不碰 ready——ready 由调用方(trader/测试)设置，这里只清计数。
        fc->sent.store(0, std::memory_order_release);
        fc->received.store(0, std::memory_order_release);
        fc->done.store(false, std::memory_order_release);
    }

    // ── UDP Socket ──
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(cfg.port);
    inet_pton(AF_INET, cfg.host, &addr.sin_addr);

    // ── 模拟交易所线程：收订单、立即回全额成交 ──
    // V5 协议解耦: codec 与 trader 侧一致(默认自定义 'O'/'F' 协议)。
    OuchOrderCodec order_codec;   // 与 trader 侧一致(OUCH 4.2)
    std::atomic<bool> order_stop{false};
    std::atomic<uint64_t> orders_received{0};
    std::thread sim_exchange(run_sim_exchange, cfg.order_port, fc,
                             cfg.order_ret_port, std::cref(order_codec),
                             std::ref(order_stop), std::ref(orders_received));

    // ── 等待 NX-Trader 就绪（--no-shm 时直接发，无握手）──
    if (fc) {
        printf("Waiting for NX-Trader ... (ready=%d)\n",
               (int)fc->ready.load(std::memory_order_acquire));
        fflush(stdout);
        int ready_wait_cnt = 0;
        while (!fc->ready.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            if (++ready_wait_cnt % 100 == 0)
                printf("  still waiting ready... (cnt=%d)\n", ready_wait_cnt), fflush(stdout);
        }
        printf("NX-Trader ready.  Start replay...\n");
        fflush(stdout);
    }

    // ── 全量发送（模拟真实交易所 MoldUDP64 封装）──
    // 每包 = [MoldUDP64头: session(10) + seq(8) + count(2)] + [消息们]
    // 内部维护全局消息序号 global_msg_seq（每发一条消息 +1）。
    // 包头 seq = 包内第一条消息的全局序号（真实 MoldUDP64 语义，见 docs）。
    // 每包 1~pack_max 条消息（随机），包边界在消息之间，不截断。
    srand(static_cast<unsigned>(time(nullptr)));
    size_t pos = 0;
    uint64_t total_sent = 0;
    uint64_t extra_bytes = 0;
    uint64_t global_msg_seq = 0;   // 全局消息序号：每发一条消息 +1
    auto t_start = std::chrono::steady_clock::now();

    // 平滑限速: 每包 sleep 到目标节奏(匀速发, 瞬时≈平均)。
    auto next_pkt_time = std::chrono::steady_clock::now();

    // 包缓冲：20 字节头 + 消息们
    std::vector<uint8_t> pkt;
    pkt.reserve(4096);
    pkt.resize(20);  // MoldUDP64 头预留

    while (pos + 2 <= static_cast<size_t>(file_size)) {
        // 每包随机定条数（1 ~ pack_max）
        size_t msgs_this_packet = 1 + static_cast<size_t>(rand() % cfg.pack_max);

        // 记录包内第一条消息的全局序号（用于包头 seq）
        uint64_t pkt_first_seq = global_msg_seq;

        // 攒满 msgs_this_packet 条或文件结束
        size_t pkt_count = 0;
        for (size_t i = 0; i < msgs_this_packet && pos + 2 <= static_cast<size_t>(file_size); ++i) {
            // ITCH 5.0: 2 字节 big-endian 长度前缀 = 含 type 的消息体长度
            uint16_t body_len = ntohs(*(const uint16_t*)(buf + pos));
            if (body_len < 1 || body_len > 200) {
                // 损坏的前缀：跳过 1 字节继续尝试对齐
                ++pos; ++extra_bytes;
                --i;  // 本条不算，继续攒
                continue;
            }
            size_t msg_len = 2 + static_cast<size_t>(body_len);
            if (pos + msg_len > static_cast<size_t>(file_size)) break;
            // 过滤 R 消息（Stock Directory）：交易系统不订阅，不发送
            if (buf[pos + 2] == 'R') {
                pos += msg_len;
                --i;  // 本条不算，继续攒满 msgs_this_packet 条
                continue;
            }
            pkt.insert(pkt.end(), buf + pos, buf + pos + msg_len);
            pos += msg_len;
            ++pkt_count;
            ++global_msg_seq;
        }

        // 填 MoldUDP64 头（20 字节）: session(10) + seq(8) + count(2)
        if (pkt_count > 0) {
            uint8_t* hdr = pkt.data();
            memset(hdr, 0, 10);                          // session: 10 字节（0）
            uint64_t be_seq = htobe64(pkt_first_seq);    // seq: 8 字节，包内第一条消息序号
            memcpy(hdr + 10, &be_seq, 8);
            uint16_t be_cnt = htobe16(pkt_count);        // count: 2 字节
            memcpy(hdr + 18, &be_cnt, 2);

            // 发送前限速:
            //   有共享内存(测试模式): 忙等上一包被接收(发一个等一个确认)，
            //              发送端永不领先>1包，内核 UDP 缓冲永不溢出 → 零丢包。
            //   无共享内存(--no-shm 压测模式): 窗口限速——每 0.1 秒窗口发 rate_01s 包，
            //              达到后 sleep 到窗口结束(固定速率，无实时反馈)。
            if (fc) {
                while (fc->received.load(std::memory_order_acquire) < fc->sent.load(std::memory_order_relaxed)) {
                    std::this_thread::yield();
                }
            } else if (cfg.rate_01s > 0) {
                // 平滑限速: 按平均速率匀速发(而非窗口 burst)。
                // 每 0.1s 目标 rate_01s 包 → 每包间隔 = 0.1s/rate_01s。
                // 用 steady_clock 跟踪下一包应发时刻, 未到就 sleep 到点。
                // 与旧窗口限速(攒满 rate_01s 包瞬间发)不同: 平滑限速的瞬时
                // 速率≈平均速率, 不会瞬间灌满内核缓冲而溢出丢包, 能测真实临界。
                auto now = std::chrono::steady_clock::now();
                if (next_pkt_time > now)
                    std::this_thread::sleep_for(next_pkt_time - now);
                next_pkt_time += std::chrono::microseconds(100000 / cfg.rate_01s);
            }

            ssize_t r = sendto(sock, pkt.data(), pkt.size(), 0,
                               reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            pkt.clear();
            pkt.resize(20);  // 复位头预留
            if (r > 0) {
                if (fc) fc->sent.fetch_add(1, std::memory_order_release);
                ++total_sent;
            }
        } else {
            pkt.clear();
            pkt.resize(20);
        }
    }

    auto t_end = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t_end - t_start).count();

    printf("\n=== Done ===\n");
    printf("  Packets sent:     %lu\n", total_sent);
    printf("  Messages sent:    %llu\n", (unsigned long long)global_msg_seq);
    printf("  Extra bytes skipped: %lu\n", extra_bytes);
    printf("  Total time:      %.3f s\n", sec);
    printf("  Send rate:       %.0f pkt/s (%.0f msg/s)\n",
           total_sent / sec, (double)global_msg_seq / sec);

    // 通知交易系统行情发完（有共享内存时，trader 据此退出）
    if (fc) fc->done.store(true, std::memory_order_release);

    // 行情发完，给交易系统留时间把订单发过来并回执。
    // 理想状态：模拟交易所全额成交，回报在交易系统侧落地。
    printf("  Waiting for order fills (2s)...\n");
    std::this_thread::sleep_for(std::chrono::seconds(2));
    order_stop.store(true, std::memory_order_release);
    sim_exchange.join();
    printf("  Sim exchange received orders: %llu\n",
           (unsigned long long)orders_received.load());

    munmap(buf, file_size);
    if (fc) munmap(fc, sizeof(FlowControl));
    close(sock);
    return 0;
}
