// IoUringReceiver / IoUringSender 集成测试
// 通过 loopback UDP 真实收发，验证 io_uring 接收与零拷贝发送链路。
//
// 注意：Release 构建定义 NDEBUG，assert() 会被编译掉，故用自定义 CHECK 宏。

#include "core/net/io_uring_receiver.h"
#include "core/net/io_uring_sender.h"

#include <cstdio>
#include <cstring>
#include <memory>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static constexpr uint16_t RECV_PORT = 45671;
static constexpr uint16_t SEND_PORT = 45672;

static void test_receiver()
{
    printf("[test_receiver] bind %u ...\n", RECV_PORT);
    // 堆分配：IoUringReceiver 内含 ~256KB 固定缓冲池
    auto receiver = std::make_unique<IoUringReceiver>(RECV_PORT);
    CHECK(receiver->start());

    // 用普通 UDP socket 发已知字节
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    CHECK(sock >= 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(RECV_PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    const char msg[] = "hello io_uring recv";
    ssize_t sent = sendto(sock, msg, sizeof(msg), 0,
                          reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    CHECK(sent == static_cast<ssize_t>(sizeof(msg)));

    uint8_t buf[512]{};
    ssize_t n = receiver->recv(buf, sizeof(buf));
    CHECK(n == static_cast<ssize_t>(sizeof(msg)));
    CHECK(std::memcmp(buf, msg, sizeof(msg)) == 0);
    printf("[test_receiver] OK: received %zd bytes\n", n);

    close(sock);
    receiver->stop();
}

static void test_sender()
{
    printf("[test_sender] send to %u ...\n", SEND_PORT);
    // 先建监听端
    int listen_sock = socket(AF_INET, SOCK_DGRAM, 0);
    CHECK(listen_sock >= 0);

    sockaddr_in laddr{};
    laddr.sin_family = AF_INET;
    laddr.sin_port = htons(SEND_PORT);
    laddr.sin_addr.s_addr = INADDR_ANY;
    CHECK(bind(listen_sock, reinterpret_cast<sockaddr*>(&laddr), sizeof(laddr)) == 0);

    // 堆分配：IoUringSender 内含 8MB SPSC ring（必须堆上）
    auto sender = std::make_unique<IoUringSender>("127.0.0.1", SEND_PORT);
    CHECK(sender->start());

    const char msg[] = "zero-copy send";
    ssize_t r = sender->send(reinterpret_cast<const uint8_t*>(msg), sizeof(msg));
    CHECK(r == static_cast<ssize_t>(sizeof(msg)));

    uint8_t buf[512]{};
    ssize_t n = recv(listen_sock, buf, sizeof(buf), 0);
    CHECK(n == static_cast<ssize_t>(sizeof(msg)));
    CHECK(std::memcmp(buf, msg, sizeof(msg)) == 0);
    printf("[test_sender] OK: received %zd bytes\n", n);

    sender->stop();
    close(listen_sock);
}

int main()
{
    test_receiver();
    test_sender();
    if (g_failures > 0) {
        printf("%d check(s) failed\n", g_failures);
        return 1;
    }
    printf("All io_uring tests passed\n");
    return 0;
}
