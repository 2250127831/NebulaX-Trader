#pragma once

#include "core/market_event.h"
#include "core/types.h"
#include "strategy/base/signal.h"

// 策略基类：所有策略继承此类，实现 on_event 处理行情事件，signal 返回信号。
// 统一消费 MarketEvent（成交/委托/盘口），内部按 type 处理自己关心的类型。
class Strategy {
public:
    virtual ~Strategy() = default;
    virtual void on_event(const MarketEvent& ev) = 0;  // 处理行情事件
    virtual Signal signal() const = 0;                 // 当前信号(方向+标的+参考价)
};
