#pragma once

#include "strategy/base/strategy.h"

// 趋势跟踪策略（示例）
class TrendStrategy : public Strategy {
public:
    void on_tick(const Tick& tick) override;
    OrderSide signal() const override;
};
