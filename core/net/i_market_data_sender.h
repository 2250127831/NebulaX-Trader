#pragma once

#include <cstddef>
#include <cstdint>
#include <sys/types.h>

// ── 信号/订单发送抽象层 ──
// 所有网络后端（io_uring / AF_XDP / DPDK）实现此接口，与 IMarketDataReceiver 配套。
// ExecutionEngine 等业务代码只依赖此接口，不依赖具体后端。
//
// 使用模式与接收侧对称（线程/事件循环由使用方组织）：
//   1. 阻塞发送（V1）: send() 阻塞直到数据发出
//   2. 非阻塞轮询      : set_blocking(false) 后 send() 无法立即发出时返回 0
//   3. epoll 多路复用  : fd() + set_blocking(false)，等 fd 可写后再 send()

class IMarketDataSender {
public:
    virtual ~IMarketDataSender() = default;

    // 启动发送通道（绑定 / 连接 / 注册发送缓冲区）。
    // 返回 true = 就绪；false = 失败（errno 说明原因）。
    // 启动后默认处于阻塞模式。
    virtual bool start() = 0;

    // 停止发送并释放资源。
    // 契约：必须能打断阻塞中的 send()，使其立即返回 0。幂等，可多次调用。
    virtual void stop() = 0;

    // 切换 send 的阻塞/非阻塞模式，运行期可随时调用。
    //   true  = 阻塞模式：send() 阻塞直到数据发出或 stop()
    //   false = 非阻塞模式：send() 无法立即发出时返回 0
    virtual void set_blocking(bool blocking) = 0;

    // 发送一条原始信号/订单消息。
    //   buf : 调用方提供的缓冲区（实现读取）
    //   len : 数据长度
    // 返回：
    //   >0  : 实际发送的字节数（<= len；UDP 实现通常全量发送）
    //    0  : 阻塞模式下=通道已停止/关闭；非阻塞模式下=当前无法发送
    //   -1  : 错误（errno 说明原因）
    virtual ssize_t send(const uint8_t* buf, size_t len) = 0;

    // 从同一连接接收数据(TCP 全双工: 订单发 + 回报收共用一连接)。
    // 与 IMarketDataReceiver::recv 语义一致:
    //   >0 : 收到的字节数(<= len)
    //    0 : 阻塞模式=通道已停止/关闭; 非阻塞=当前无数据
    //   -1 : 错误(errno 说明原因)
    virtual ssize_t recv(uint8_t* buf, size_t len) = 0;

    // 发送通道的文件描述符，供 epoll 监听可写事件。
    //   io_uring → ring fd；DPDK → -1。
    virtual int fd() const = 0;
};
