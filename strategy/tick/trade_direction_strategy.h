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
        locate_ = ev.locate;
        last_ts_ = ev.timestamp;
        if (ev.trade.price >= 0) last_price_ = ev.trade.price;
        // trade.side 表示主动方：BUY = 主动买，SELL = 主动卖
        if (ev.trade.side == OrderSide::BUY)      current_ = OrderSide::BUY;
        else if (ev.trade.side == OrderSide::SELL) current_ = OrderSide::SELL;
        else                                       current_ = OrderSide::NONE;
        // 方向策略无强度概念：有明确方向即满强度
        strength_ = (current_ == OrderSide::NONE) ? 0 : Signal::kStrengthScale;
    }

    Signal signal() const override {
        return Signal{.side = current_, .locate = locate_,
                      .price = last_price_, .timestamp = last_ts_,
                      .strength = strength_};
    }

private:
    OrderSide current_ = OrderSide::NONE;
    uint64_t  locate_ = 0;
    int64_t   last_price_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
