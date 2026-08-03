#pragma once

#include "strategy/kline/kline_aggregator.h"
#include "strategy/base/signal.h"
#include "core/types.h"

#include <algorithm>
#include <cmath>
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
        symbol_id_ = bar.symbol_id;
        last_ts_ = bar.timestamp;
        last_close_ = bar.close;
        closes_.push_back(bar.close);
        if (closes_.size() > long_period_) closes_.pop_front();

        if (closes_.size() < long_period_) { current_ = OrderSide::NONE; return; }

        double short_ma = ma(short_period_);
        double long_ma  = ma(long_period_);

        if (short_ma > long_ma)      current_ = OrderSide::BUY;
        else if (short_ma < long_ma) current_ = OrderSide::SELL;
        else                         current_ = OrderSide::NONE;

        // 强度：均线距离 / 长均线，万分比，1% 距离即满强度
        if (current_ != OrderSide::NONE) {
            double ratio = std::abs(short_ma - long_ma) / std::max(1.0, long_ma);
            double s = ratio * 100.0 * (double)Signal::kStrengthScale;  // 0.01 → 10000
            strength_ = (int64_t)(s > Signal::kStrengthScale ? Signal::kStrengthScale : s);
        } else {
            strength_ = 0;
        }
    }

    Signal signal() const {
        return Signal{.side = current_, .locate = symbol_id_,
                      .price = last_close_, .timestamp = last_ts_,
                      .strength = strength_};
    }

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
    uint64_t  symbol_id_ = 0;
    int64_t   last_close_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
