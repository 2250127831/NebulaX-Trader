#pragma once

#include "strategy/base/strategy.h"

// ── 成交方向策略 ──
// 直接消费逐笔成交(tick)。用 trade.side(主动方)判断买卖主导。
//   最近一笔成交是主动买(SELL 被吃/主动买) → BUY
//   主动卖 → SELL
//   无方向(E) → NONE
// 真正的 tick 消费者，用于验证 SPMC 多消费者广播。
class TradeDirectionStrategy : public Strategy {
public:
    void on_event(const MarketEvent& ev) override {
        if (ev.type != MarketEvent::Type::TRADE &&
            ev.type != MarketEvent::Type::EXECUTE) return;
        // trade.side 表示主动方：BUY = 主动买，SELL = 主动卖
        if (ev.trade.side == OrderSide::BUY)      current_ = OrderSide::BUY;
        else if (ev.trade.side == OrderSide::SELL) current_ = OrderSide::SELL;
        else                                       current_ = OrderSide::NONE;
    }

    OrderSide signal() const override { return current_; }

private:
    OrderSide current_ = OrderSide::NONE;
};
