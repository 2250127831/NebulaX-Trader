#pragma once

#include "strategy/base/strategy.h"

// 动量策略（示例）
class MomentumStrategy : public Strategy {
public:
    void on_tick(const Tick& tick) override;
    OrderSide signal() const override;
};
