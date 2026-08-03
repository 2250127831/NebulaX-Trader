#pragma once

#include "strategy/base/strategy.h"

#include <deque>

// ── 简单动量策略(tick 版) ──
// 直接消费逐笔成交(tick)。最近 N 笔成交的加权价格方向：
//   近期价 - 较早价 > threshold → 上行动量 → BUY
//   下行动量 → SELL
// 用 VWAP(成交量加权均价)表示近期/较早价格，避免单笔噪音。
// 真正的 tick 消费者，用于验证 SPMC 多消费者广播。
class TickMomentumStrategy : public Strategy {
public:
    explicit TickMomentumStrategy(size_t window = 20, int64_t threshold = 10)
        : window_(window), threshold_(threshold) {}

    void on_event(const MarketEvent& ev) override {
        if (ev.type != MarketEvent::Type::TRADE &&
            ev.type != MarketEvent::Type::EXECUTE) return;
        int64_t price = ev.trade.price;
        if (price < 0) return;

        prices_.push_back(price);
        vols_.push_back(ev.trade.volume);
        if (prices_.size() > window_) { prices_.pop_front(); vols_.pop_front(); }

        if (prices_.size() < 2) { current_ = OrderSide::NONE; return; }

        // 前半 vs 后半 VWAP
        size_t half = prices_.size() / 2;
        int64_t sum_px1 = 0; uint64_t sum_v1 = 0;
        for (size_t i = 0; i < half; ++i) { sum_px1 += prices_[i] * vols_[i]; sum_v1 += vols_[i]; }
        int64_t sum_px2 = 0; uint64_t sum_v2 = 0;
        for (size_t i = half; i < prices_.size(); ++i) { sum_px2 += prices_[i] * vols_[i]; sum_v2 += vols_[i]; }
        if (sum_v1 == 0 || sum_v2 == 0) { current_ = OrderSide::NONE; return; }

        int64_t vwap1 = sum_px1 / static_cast<int64_t>(sum_v1);
        int64_t vwap2 = sum_px2 / static_cast<int64_t>(sum_v2);

        if (vwap2 - vwap1 > threshold_)      current_ = OrderSide::BUY;
        else if (vwap1 - vwap2 > threshold_) current_ = OrderSide::SELL;
        else                                 current_ = OrderSide::NONE;
    }

    OrderSide signal() const override { return current_; }

private:
    size_t window_;
    int64_t threshold_;
    std::deque<int64_t> prices_;
    std::deque<uint64_t> vols_;
    OrderSide current_ = OrderSide::NONE;
};
