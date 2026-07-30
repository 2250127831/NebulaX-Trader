#pragma once

#include "i_market_data_receiver.h"

// ── io_uring 后端实现（V1 默认） ──
class IoUringReceiver : public IMarketDataReceiver {
public:
    explicit IoUringReceiver(uint16_t port);

    bool start() override;
    void stop() override;
    ssize_t recv(uint8_t* buf, size_t len) override;
    int fd() const override;

private:
    uint16_t port_;
    int fd_ = -1;
    // TODO: io_uring 成员（迁移自 NebulaX）
};
