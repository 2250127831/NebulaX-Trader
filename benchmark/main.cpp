#include "core/ipc/flow_control.h"

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
    uint64_t max_backlog = 10000;
    size_t pack_max  = 100;  // 每包消息条数上限（实际每包 1~pack_max 条随机）
    bool help         = false;
};

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        if      (strcmp(argv[i], "--file")    == 0 && i+1 < argc) cfg.file = argv[++i];
        else if (strcmp(argv[i], "--host")    == 0 && i+1 < argc) cfg.host = argv[++i];
        else if (strcmp(argv[i], "--port")    == 0 && i+1 < argc) cfg.port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--backlog") == 0 && i+1 < argc) cfg.max_backlog = atol(argv[++i]);
        else if (strcmp(argv[i], "--pack-max") == 0 && i+1 < argc) cfg.pack_max = atol(argv[++i]);
        else cfg.help = true;
    }
    return cfg;
}

static void usage() {
    printf("Usage: trader_benchmark [options]\n"
           "  --file  <path>     ITCH binary file\n"
           "  --host <ip>        Target IP (default: 127.0.0.1)\n"
           "  --port <port>      Target UDP port (default: 8080)\n"
           "  --backlog <n>      Max backlog before slowing down (default: 10000)\n"
           "  --pack-max <n>     Max messages per UDP packet (default: 100)\n"
           "                     实际每包 1~pack-max 条随机，模拟真实行情打包\n");
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

    // ── 共享内存 FlowControl ──
    int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
    if (shm_fd < 0) { perror("shm_open"); return 1; }
    if (ftruncate(shm_fd, sizeof(FlowControl)) < 0) { perror("ftruncate"); return 1; }
    auto* fc = static_cast<FlowControl*>(mmap(nullptr, sizeof(FlowControl),
        PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
    close(shm_fd);
    if (fc == MAP_FAILED) { perror("mmap shm"); return 1; }

    // ── UDP Socket ──
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(cfg.port);
    inet_pton(AF_INET, cfg.host, &addr.sin_addr);

    // ── 等待 NX-Trader 就绪 ──
    printf("Waiting for NX-Trader ...\n");
    while (!fc->ready.load(std::memory_order_acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    printf("NX-Trader ready.  Start replay...\n");

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

            // 发送前: 轮询计数器, 等对面收完到 backlog 允许范围。
            // backlog = sent - received; 超过 max_backlog 就等 received 追上来。
            uint64_t sent = fc->sent.load(std::memory_order_relaxed);
            uint64_t recv = fc->received.load(std::memory_order_acquire);
            while (static_cast<int64_t>(sent - recv) >= static_cast<int64_t>(cfg.max_backlog)) {
                std::this_thread::yield();
                recv = fc->received.load(std::memory_order_acquire);
            }

            ssize_t r = sendto(sock, pkt.data(), pkt.size(), 0,
                               reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            pkt.clear();
            pkt.resize(20);  // 复位头预留
            if (r > 0) {
                fc->sent.fetch_add(1, std::memory_order_release);
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

    munmap(buf, file_size);
    munmap(fc, sizeof(FlowControl));
    close(sock);
    return 0;
}
