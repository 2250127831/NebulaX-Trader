#pragma once

#include "core/types.h"
#include "core/queue/spmc_ring.h"

// 行情网关：接收原始行情数据，解析为 Tick 结构体后推入 SPMC 队列
class MarketDataGateway {
public:
    MarketDataGateway(SPMCByteRing<1 << 24>& ring) : ring_(ring) {}
    void start();
    void stop();
private:
    SPMCByteRing<1 << 24>& ring_;
};
