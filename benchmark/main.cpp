#include "core/ipc/flow_control.h"

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <thread>

#include <sys/mman.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cerrno>

// ── ITCH 5.0 消息体长度（不含 9 字节头）──
static constexpr int MSG_BODY_LEN(char t) {
    switch (t) {
        case 'A': return 40; case 'B': return 35; case 'C': return 35;
        case 'D': return 11; case 'E': return 31; case 'F': return 39;
        case 'H': return 19; case 'L': return 25; case 'N': return 21;
        case 'O': return 19; case 'P': return 43; case 'Q': return 39;
        case 'R': return 37; case 'S': return 7;  case 'T': return 19;
        case 'U': return 11; case 'X': return 19; case 'Y': return 19;
        default:  return 0;
    }
}

struct Config {
    const char* file  = "test_data/itch_sample.bin";
    const char* host  = "127.0.0.1";
    int port          = 8080;
    uint64_t max_backlog = 10000;
    bool help         = false;
};

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        if      (strcmp(argv[i], "--file")    == 0 && i+1 < argc) cfg.file = argv[++i];
        else if (strcmp(argv[i], "--host")    == 0 && i+1 < argc) cfg.host = argv[++i];
        else if (strcmp(argv[i], "--port")    == 0 && i+1 < argc) cfg.port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--backlog") == 0 && i+1 < argc) cfg.max_backlog = atol(argv[++i]);
        else cfg.help = true;
    }
    return cfg;
}

static void usage() {
    printf("Usage: trader_benchmark [options]\n"
           "  --file  <path>     ITCH binary file\n"
           "  --host <ip>        Target IP (default: 127.0.0.1)\n"
           "  --port <port>      Target UDP port (default: 8080)\n"
           "  --backlog <n>      Max backlog before slowing down (default: 10000)\n");
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

    // ── 全量发送 ──
    size_t pos = 0;
    uint64_t total_sent = 0;
    uint64_t extra_bytes = 0;
    auto t_start = std::chrono::steady_clock::now();
    uint64_t slowdown_ns = 0;

    while (pos + 9 <= static_cast<size_t>(file_size)) {
        uint8_t type = buf[pos + 8];
        int body_len = MSG_BODY_LEN(char(type));
        if (body_len == 0) {  // 未知类型，滑动窗口
            ++pos; ++extra_bytes;
            continue;
        }
        size_t msg_len = 9 + static_cast<size_t>(body_len);
        if (pos + msg_len > static_cast<size_t>(file_size)) break;

        ssize_t r = sendto(sock, buf + pos, msg_len, 0,
                           reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (r > 0) {
            fc->sent.fetch_add(1, std::memory_order_release);
            ++total_sent;

            if ((total_sent & 0xFF) == 0) {
                uint64_t sent = fc->sent.load(std::memory_order_relaxed);
                uint64_t recv = fc->received.load(std::memory_order_acquire);
                int64_t backlog = static_cast<int64_t>(sent - recv);

                if (backlog > static_cast<int64_t>(cfg.max_backlog))
                    slowdown_ns += 100;
                else if (backlog < 100 && slowdown_ns > 0)
                    slowdown_ns -= slowdown_ns > 100 ? 100 : slowdown_ns;
            }

            if (slowdown_ns > 0)
                std::this_thread::sleep_for(std::chrono::nanoseconds(slowdown_ns));
        }

        pos += msg_len;
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
