#pragma once

#include "strategy/base/strategy.h"

#include <algorithm>   // std::max_element/min_element (g++-12+ 不再传递包含, 需显式引入)
#include <deque>

// ── 价格突破策略 ──
// 直接消费逐笔成交(tick)。当前价突破最近 N 笔的高点 → 上破 → BUY；
// 跌破低点 → 下破 → SELL。
// 真正的 tick 消费者，用于验证 SPMC 多消费者广播。
class PriceBreakoutStrategy : public StrategyT<PriceBreakoutStrategy> {
public:
    explicit PriceBreakoutStrategy(size_t window = 50)
        : window_(window) {}

    // 框架统一入口(CRTP): 只消费成交事件, 不需要 BookContext(仅转发)。
    void on_market(const MarketEvent& ev, const BookContext& ctx) {
        (void)ctx;
        on_event(ev);
    }

    void on_event(const MarketEvent& ev) {
        if (ev.type != MarketEvent::Type::TRADE &&
            ev.type != MarketEvent::Type::EXECUTE) return;
        int64_t price = ev.trade.price;
        if (price < 0) return;  // E 无价格
        locate_ = ev.locate;
        last_ts_ = ev.timestamp;
        last_price_ = price;

        if (prices_.size() >= window_) {
            int64_t hi = *std::max_element(prices_.begin(), prices_.end());
            int64_t lo = *std::min_element(prices_.begin(), prices_.end());
            if (price > hi)      current_ = OrderSide::BUY;
            else if (price < lo) current_ = OrderSide::SELL;
            else                 current_ = OrderSide::NONE;

            // 强度：突破幅度 / 窗口内波动幅度(hi-lo)，万分比，满 1 倍波动即满强度。
            int64_t range = hi - lo;
            if (current_ == OrderSide::BUY) {
                if (range <= 0) strength_ = Signal::kStrengthScale;
                else {
                    int64_t s = (price - hi) * Signal::kStrengthScale / range;
                    strength_ = s > Signal::kStrengthScale ? Signal::kStrengthScale : s;
                }
            } else if (current_ == OrderSide::SELL) {
                if (range <= 0) strength_ = Signal::kStrengthScale;
                else {
                    int64_t s = (lo - price) * Signal::kStrengthScale / range;
                    strength_ = s > Signal::kStrengthScale ? Signal::kStrengthScale : s;
                }
            } else {
                strength_ = 0;
            }
        }
        prices_.push_back(price);
        if (prices_.size() > window_) prices_.pop_front();
    }

    Signal signal() const {
        return Signal{.side = current_, .locate = locate_,
                      .price = last_price_, .timestamp = last_ts_,
                      .strength = strength_};
    }

private:
    size_t window_;
    std::deque<int64_t> prices_;
    OrderSide current_ = OrderSide::NONE;
    uint64_t  locate_ = 0;
    int64_t   last_price_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
