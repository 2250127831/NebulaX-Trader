#pragma once

#include <cstddef>
#include <cstdint>

// ── 行情数据接收抽象层 ──
// 所有网络后端（io_uring / AF_XDP / DPDK）实现此接口。
// Parser、Dispatcher、Strategy 等业务代码只依赖此接口，不依赖具体后端。

class IMarketDataReceiver {
public:
    virtual ~IMarketDataReceiver() = default;

    // 启动接收（绑定端口/队列、注册缓冲区等）
    virtual bool start() = 0;

    // 优雅关闭
    virtual void stop() = 0;

    // 接收一条原始行情消息（阻塞或异步，由具体实现决定）
    // buf   : 写入缓冲区
    // len   : 缓冲区大小
    // 返回  : 实际接收字节数，0 表示无数据，负值表示错误
    virtual ssize_t recv(uint8_t* buf, size_t len) = 0;

    // 接收队列的文件描述符（用于 eventfd/epoll 多路复用）
    // io_uring 返回 ring fd，AF_XDP 返回 xsk fd
    virtual int fd() const = 0;
};
