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

private:
    uint16_t port_;
    int fd_ = -1;
    IoUringPoller poller_;
    uint32_t buf_idx_ = UINT32_MAX;
    bool recv_pending_ = false;   // 是否有在途 recv SQE

    std::atomic<bool> running_{false};
    std::atomic<bool> blocking_{true};
};
