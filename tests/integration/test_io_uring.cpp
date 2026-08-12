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

// V5: IoUringSender TCP 全双工。监听端 = TCP server, 验证 send + recv 同一连接。
static void test_sender()
{
    printf("[test_sender] TCP send/recv to %u ...\n", SEND_PORT);
    // 先建 TCP 监听端
    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(listen_sock >= 0);
    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in laddr{};
    laddr.sin_family = AF_INET;
    laddr.sin_port = htons(SEND_PORT);
    laddr.sin_addr.s_addr = INADDR_ANY;
    CHECK(bind(listen_sock, reinterpret_cast<sockaddr*>(&laddr), sizeof(laddr)) == 0);
    CHECK(listen(listen_sock, 4) == 0);

    // IoUringSender 的 ring 存储空间由用户分配(TCP 连上来)
    auto send_ring_buf = std::make_unique<uint8_t[]>(1 << 20);
    auto sender = std::make_unique<IoUringSender>("127.0.0.1", SEND_PORT,
                                                  send_ring_buf.get(), 1 << 20);
    CHECK(sender->start());

    int csock = accept(listen_sock, nullptr, nullptr);
    CHECK(csock >= 0);

    // send: 发送字节到对端
    const char msg[] = "zero-copy send";
    ssize_t r = sender->send(reinterpret_cast<const uint8_t*>(msg), sizeof(msg));
    CHECK(r == static_cast<ssize_t>(sizeof(msg)));
    uint8_t rbuf[512]{};
    ssize_t n = recv(csock, rbuf, sizeof(rbuf), 0);
    CHECK(n == static_cast<ssize_t>(sizeof(msg)));
    CHECK(std::memcmp(rbuf, msg, sizeof(msg)) == 0);

    // recv: 同一连接读对端回报(TCP 全双工)
    const char resp[] = "fill-ack";
    CHECK(send(csock, resp, sizeof(resp), 0) == static_cast<ssize_t>(sizeof(resp)));
    uint8_t sbuf[512]{};
    ssize_t m = sender->recv(sbuf, sizeof(sbuf));
    CHECK(m == static_cast<ssize_t>(sizeof(resp)));
    CHECK(std::memcmp(sbuf, resp, sizeof(resp)) == 0);
    printf("[test_sender] OK: send %zd + recv %zd bytes\n", n, m);

    sender->stop();
    close(csock);
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
