#pragma once

#include <cstddef>
#include <cstdint>
#include <sys/types.h>

// ── 行情数据接收抽象层 ──
// 所有网络后端（io_uring / AF_XDP / DPDK）实现此接口。
// Parser、Dispatcher、Strategy 等业务代码只依赖此接口，不依赖具体后端。
//
// 三种使用模式（线程/事件循环由使用方组织，接口只提供收发能力）：
//   1. 阻塞单线程循环（V1）   : recv() 阻塞直到收到数据
//   2. 非阻塞轮询             : set_blocking(false) 后 recv() 无数据立即返回 0
//   3. epoll 多路复用         : fd() + set_blocking(false)，等 fd 可读后再 recv()

class IMarketDataReceiver {
public:
    virtual ~IMarketDataReceiver() = default;

    // 启动接收通道（绑定端口 / 注册缓冲区 / 初始化 ring）。
    // 返回 true = 就绪；false = 失败（errno 说明原因）。
    // 启动后默认处于阻塞模式。
    virtual bool start() = 0;

    // 停止接收并释放资源。
    // 契约：必须能打断阻塞中的 recv()，使其立即返回 0。幂等，可多次调用。
    virtual void stop() = 0;

    // 切换 recv 的阻塞/非阻塞模式，运行期可随时调用。
    //   true  = 阻塞模式：recv() 阻塞直到收到数据或 stop()
    //   false = 非阻塞模式：recv() 无数据立即返回 0，配合 fd() + epoll 使用
    virtual void set_blocking(bool blocking) = 0;

    // 接收一条原始行情消息。
    //   buf : 调用方提供的缓冲区（实现写入）
    //   len : 缓冲区容量
    // 返回：
    //   >0  : 收到的字节数（<= len）
    //    0  : 阻塞模式下=通道已停止/关闭（stop() 被调用）；
    //         非阻塞模式下=当前无数据（等价 EAGAIN）
    //   -1  : 错误（errno 说明原因）
    virtual ssize_t recv(uint8_t* buf, size_t len) = 0;

    // 接收通道的文件描述符，供 epoll/poll 多路复用。
    //   io_uring → ring fd；AF_XDP → xsk fd；DPDK → -1（无 fd，纯轮询）。
    virtual int fd() const = 0;
};
