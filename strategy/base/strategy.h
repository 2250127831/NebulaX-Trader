#pragma once

#include "core/types.h"

// 策略基类：所有策略继承此类，实现 on_tick 方法
class Strategy {
public:
    virtual ~Strategy() = default;
    virtual void on_tick(const Tick& tick) = 0;
    virtual OrderSide signal() const = 0;
};
