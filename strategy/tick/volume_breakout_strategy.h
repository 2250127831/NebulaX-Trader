#pragma once

#include "strategy/base/strategy.h"

#include <deque>

// ── 成交量突破策略 ──
// 直接消费逐笔成交(tick)。最近 N 笔累计成交量超过阈值 → 放量突破 → 信号。
//   累计量 > threshold → BUY(放量上攻)
//   否则 → NONE
// 这是真正的 tick 消费者，用于验证 SPMC 多消费者广播。
class VolumeBreakoutStrategy : public Strategy {
public:
    explicit VolumeBreakoutStrategy(size_t window = 100, uint64_t threshold = 5000)
        : window_(window), threshold_(threshold) {}

    void on_event(const MarketEvent& ev) override {
        if (ev.type != MarketEvent::Type::TRADE &&
            ev.type != MarketEvent::Type::EXECUTE) return;
        vols_.push_back(ev.trade.volume);
        if (vols_.size() > window_) vols_.pop_front();

        uint64_t sum = 0;
        for (uint64_t v : vols_) sum += v;
        current_ = (sum > threshold_) ? OrderSide::BUY : OrderSide::NONE;
    }

    OrderSide signal() const override { return current_; }

private:
    size_t window_;
    uint64_t threshold_;
    std::deque<uint64_t> vols_;
    OrderSide current_ = OrderSide::NONE;
};
