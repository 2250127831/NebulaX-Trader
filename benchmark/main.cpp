#include "core/ipc/flow_control.h"
#include "oms/order_protocol.h"

#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <thread>
#include <vector>
#include <cstdlib>
#include <ctime>

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
    uint64_t pace_us = 0;    // 每包间隔(微秒)，--no-shm 限速用(默认 0 = 全速)
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
        else if (strcmp(argv[i], "--pace-us") == 0 && i+1 < argc) cfg.pace_us = atol(argv[++i]);
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
           "  --pace-us <us>     Per-packet sleep (us). Throttle without shm.\n"
           "  --no-shm           No shared-memory handshake (pressure script mode).\n"
           "                     Trader started first, wait, then fire at full/pace.\n");
}

// ── 模拟交易所线程 ──
// 理想状态：收到订单立即全额成交，回报发回交易系统的成交回报端口。
// 回报端口来源: 有共享内存→读 fc->order_ret_port; 无共享内存(--no-shm)→参数 ret_port。
// 定长协议见 oms/order_protocol.h。
static void run_sim_exchange(int order_port, FlowControl* fc, uint16_t ret_port,
                             std::atomic<bool>& stop,
                             std::atomic<uint64_t>& orders_received) {
    int osock = socket(AF_INET, SOCK_DGRAM, 0);
    if (osock < 0) return;
    sockaddr_in oaddr{};
    oaddr.sin_family = AF_INET;
    oaddr.sin_port   = htons(static_cast<uint16_t>(order_port));
    oaddr.sin_addr.s_addr = INADDR_ANY;
    if (bind(osock, reinterpret_cast<sockaddr*>(&oaddr), sizeof(oaddr)) < 0) {
        close(osock);
        return;
    }
    // 200ms recv 超时，让 stop 能及时退出
    struct timeval tv{0, 200000};
    setsockopt(osock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    uint8_t buf[256];
    while (!stop.load(std::memory_order_acquire)) {
        sockaddr_in from{};
        socklen_t flen = sizeof(from);
        ssize_t n = recvfrom(osock, buf, sizeof(buf), 0,
                             reinterpret_cast<sockaddr*>(&from), &flen);
        if (n < 0) continue;   // 超时(无订单)
        if (n < (ssize_t)kOrderMsgLen || buf[0] != kMsgOrder) continue;

        Order o{};
        if (!decode_order(buf, static_cast<size_t>(n), o)) continue;
        ++orders_received;

        // 理想状态：全额成交，按订单价回报
        uint8_t fill[kFillMsgLen];
        encode_fill(o.order_id, o.quantity, o.price, fill);

        uint64_t rp = (fc != nullptr)
            ? fc->order_ret_port.load(std::memory_order_acquire) : ret_port;
        if (rp == 0) continue;
        sockaddr_in ret{};
        ret.sin_family = AF_INET;
        ret.sin_port   = htons(static_cast<uint16_t>(rp));
        inet_pton(AF_INET, "127.0.0.1", &ret.sin_addr);
        sendto(osock, fill, kFillMsgLen, 0,
               reinterpret_cast<sockaddr*>(&ret), sizeof(ret));
    }
    close(osock);
}

int main(int argc, char* argv[]) {
    auto cfg = parse_args(argc, argv);
    if (cfg.help) { usage(); return 0; }

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
    std::atomic<bool> order_stop{false};
    std::atomic<uint64_t> orders_received{0};
    std::thread sim_exchange(run_sim_exchange, cfg.order_port, fc,
                             cfg.order_ret_port,
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
            //   有共享内存: 忙等上一包被接收(发一个等一个确认)，发送端永不领先>1包，
            //              内核 UDP 缓冲永不溢出 → 零丢包，吞吐 = 接收确认 RTT。
            //   无共享内存(--no-shm): 固定每包 sleep(pace_us)，压测脚本模式，
            //              与 trader 无握手，发完即退。
            if (fc) {
                while (fc->received.load(std::memory_order_acquire) < fc->sent.load(std::memory_order_relaxed)) {
                    std::this_thread::yield();
                }
            } else if (cfg.pace_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(cfg.pace_us));
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
    printf("  Messages sent:   %lu\n", total_sent);
    printf("  Extra bytes skipped: %lu\n", extra_bytes);
    printf("  Total time:      %.3f s\n", sec);
    printf("  Send rate:       %.0f msg/s\n", total_sent / sec);

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
