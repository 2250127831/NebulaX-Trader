#pragma once

#include "strategy/kline/kline_aggregator.h"
#include "core/types.h"

#include <deque>

// ── 趋势跟踪策略 ──
// 消费 K线，用双均线交叉判断趋势方向。
//   短均线(如 MA5)上穿长均线(如 MA20) → 上升趋势 → BUY
//   短均线下穿长均线 → 下降趋势 → SELL
//   否则 → NONE(观望)
//
// 用法：KLineAggregator 产出的 K线直接调 on_bar()。
// 这是低频策略：消费聚合后的 K线，不看逐笔 tick。
class TrendStrategy {
public:
    explicit TrendStrategy(size_t short_period = 5, size_t long_period = 20)
        : short_period_(short_period), long_period_(long_period) {}

    // 每根完成的 K线
    void on_bar(const KLine& bar) {
        closes_.push_back(bar.close);
        if (closes_.size() > long_period_) closes_.pop_front();

        if (closes_.size() < long_period_) { current_ = OrderSide::NONE; return; }

        double short_ma = ma(short_period_);
        double long_ma  = ma(long_period_);

        if (short_ma > long_ma)      current_ = OrderSide::BUY;
        else if (short_ma < long_ma) current_ = OrderSide::SELL;
        else                         current_ = OrderSide::NONE;
    }

    OrderSide signal() const { return current_; }

private:
    double ma(size_t period) const {
        double sum = 0;
        for (size_t i = closes_.size() - period; i < closes_.size(); ++i)
            sum += static_cast<double>(closes_[i]);
        return sum / period;
    }

    size_t short_period_, long_period_;
    std::deque<int64_t> closes_;   // 最近 close 序列
    OrderSide current_ = OrderSide::NONE;
};
