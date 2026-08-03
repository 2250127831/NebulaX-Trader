#pragma once

#include "strategy/base/strategy.h"

#include <deque>

// ── 价格突破策略 ──
// 直接消费逐笔成交(tick)。当前价突破最近 N 笔的高点 → 上破 → BUY；
// 跌破低点 → 下破 → SELL。
// 真正的 tick 消费者，用于验证 SPMC 多消费者广播。
class PriceBreakoutStrategy : public Strategy {
public:
    explicit PriceBreakoutStrategy(size_t window = 50)
        : window_(window) {}

    void on_event(const MarketEvent& ev) override {
        if (ev.type != MarketEvent::Type::TRADE &&
            ev.type != MarketEvent::Type::EXECUTE) return;
        int64_t price = ev.trade.price;
        if (price < 0) return;  // E 无价格

        if (prices_.size() >= window_) {
            int64_t hi = *std::max_element(prices_.begin(), prices_.end());
            int64_t lo = *std::min_element(prices_.begin(), prices_.end());
            if (price > hi)      current_ = OrderSide::BUY;
            else if (price < lo) current_ = OrderSide::SELL;
            else                 current_ = OrderSide::NONE;
        }
        prices_.push_back(price);
        if (prices_.size() > window_) prices_.pop_front();
    }

    OrderSide signal() const override { return current_; }

private:
    size_t window_;
    std::deque<int64_t> prices_;
    OrderSide current_ = OrderSide::NONE;
};
