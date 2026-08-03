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
        locate_ = ev.locate;
        last_ts_ = ev.timestamp;
        if (ev.trade.price >= 0) last_price_ = ev.trade.price;
        vols_.push_back(ev.trade.volume);
        if (vols_.size() > window_) vols_.pop_front();

        uint64_t sum = 0;
        for (uint64_t v : vols_) sum += v;
        current_ = (sum > threshold_) ? OrderSide::BUY : OrderSide::NONE;

        // 强度：放量倍数(超出阈值的部分 / 阈值)，万分比，封顶满强度。
        //   sum = 1×threshold → 0，2×threshold → 满强度
        if (current_ == OrderSide::BUY) {
            if (threshold_ == 0) strength_ = Signal::kStrengthScale;
            else {
                int64_t s = (int64_t)((sum - threshold_)
                                      * (uint64_t)Signal::kStrengthScale / threshold_);
                strength_ = s > Signal::kStrengthScale ? Signal::kStrengthScale : s;
            }
        } else {
            strength_ = 0;
        }
    }

    Signal signal() const override {
        return Signal{.side = current_, .locate = locate_,
                      .price = last_price_, .timestamp = last_ts_,
                      .strength = strength_};
    }

private:
    size_t window_;
    uint64_t threshold_;
    std::deque<uint64_t> vols_;
    OrderSide current_ = OrderSide::NONE;
    uint64_t  locate_ = 0;
    int64_t   last_price_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
