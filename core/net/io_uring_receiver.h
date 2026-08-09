#pragma once

#include "i_market_data_receiver.h"
#include "io_uring_poller.h"

#include <atomic>
#include <cstdint>

// ── io_uring 后端实现（V1 默认）──
// 迁移自 NebulaX 的 IoUringPoller，UDP 化。
// UDP socket + io_uring recv（带 POLL_FIRST + 固定缓冲区）。
class IoUringReceiver : public IMarketDataReceiver {
public:
    explicit IoUringReceiver(uint16_t port);

    bool start() override;
    void stop() override;
    void set_blocking(bool blocking) override;
    ssize_t recv(uint8_t* buf, size_t len) override;
    int fd() const override;

    // 多在途 recv(V2.4): 主行情 recv_th 用。内核并行收包。
    // begin_batch(): 预提交多个在途 recv SQE(不阻塞, 必须在 recv_batch 前调用)。
    // recv_batch(): 阻塞 reap 一个已完成的包(数据在固定 buffer, 处理后循环复用)。
    // 与单包 recv() 二选一, 不混用。返回包长, 0=stop 打断。
    void begin_batch();
    ssize_t recv_batch(uint8_t* buf, size_t len);

private:
    uint16_t port_;
    int fd_ = -1;
    IoUringPoller poller_;
    uint32_t buf_idx_ = UINT32_MAX;
    bool recv_pending_ = false;   // 单包路径: 是否有在途 recv SQE
    bool batch_started_ = false;  // 批量路径: 是否已预提交在途 SQE

    std::atomic<bool> running_{false};
    std::atomic<bool> blocking_{true};
};
