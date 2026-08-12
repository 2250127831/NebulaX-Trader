#pragma once

#include "i_market_data_sender.h"
#include "io_uring_poller.h"
#include "../queue/spsc_byte_ring.h"

#include <liburing.h>

#include <atomic>
#include <cstdint>
#include <string>

// ── io_uring 零拷贝发送后端（V5 TCP 化）──
// 迁移自 NebulaX 的 send_uring（SEND_ZC_FIXED）。
// 数据先拷入内部 SPSCByteRing（注册为 io_uring 固定 buffer），
// 再从 ring 零拷贝发送到网卡，免去用户态→内核的数据拷贝。
//
// V5: socket UDP(SOCK_DGRAM) → TCP(SOCK_STREAM), 同一连接全双工。
//   send: io_uring SEND_ZC_FIXED 零拷贝发送(TCP 短写由循环处理)。
//   recv: 内嵌 IoUringPoller 复用 IoUringReceiver::recv 模式, 回报在同一 fd 读。
class IoUringSender : public IMarketDataSender {
public:
    // 构造传入发送中转 ring 的存储空间 + 容量（用户分配，见 SPSCByteRing）。
    // ring 的原始 buffer 会被注册为 io_uring 固定缓冲区（SEND_ZC）。
    IoUringSender(const std::string& host, uint16_t port,
                  uint8_t* ring_buf, size_t ring_capacity);

    bool start() override;
    void stop() override;
    void set_blocking(bool blocking) override;
    ssize_t send(const uint8_t* buf, size_t len) override;
    ssize_t recv(uint8_t* buf, size_t len) override;
    int fd() const override;

private:
    // 从内部 ring 零拷贝发送 len 字节（迁移自 send_zc_all）
    ssize_t send_zc_all(size_t len);

    std::string host_;
    uint16_t port_;
    int fd_ = -1;

    struct io_uring uring_{};
    bool uring_ok_ = false;

    // 接收侧(V5 TCP 全双工): 独立 ring + 固定 buffer, 回报从同一 fd 读
    IoUringPoller recv_poller_;
    uint32_t recv_buf_idx_ = UINT32_MAX;
    bool recv_pending_ = false;   // recv SQE 是否在途

    // 发送中转 ring，其原始 buffer 注册为固定缓冲区
    SPSCByteRing ring_;

    std::atomic<bool> running_{false};
    std::atomic<bool> blocking_{true};
};
