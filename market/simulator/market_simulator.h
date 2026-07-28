#pragma once

#include "core/types.h"
#include "core/queue/spmc_ring.h"

// 市场模拟器：用于本地测试，生成模拟 tick 数据
class MarketSimulator {
public:
    MarketSimulator(SPMCByteRing<1 << 24>& ring) : ring_(ring) {}
    void generate(size_t count);
private:
    SPMCByteRing<1 << 24>& ring_;
};
