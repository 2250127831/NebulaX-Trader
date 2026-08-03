#pragma once

#include "strategy/kline/kline_aggregator.h"
#include "core/types.h"

#include <deque>

// ── 动量策略 ──
// 消费 K线，用过去 N 根 K线的收益率判断动量方向。
//   过去 N 根收益率 > +threshold → 上涨动量 → BUY
//   过去 N 根收益率 < -threshold → 下跌动量 → SELL
//   否则 → NONE(观望)
//
// 收益率 = (当前收盘 - N 根前收盘) / N 根前收盘
// 用法：KLineAggregator 产出的 K线直接调 on_bar()。
class MomentumStrategy {
public:
    explicit MomentumStrategy(size_t lookback = 10, double threshold = 0.01)
        : lookback_(lookback), threshold_(threshold) {}

    // 每根完成的 K线
    void on_bar(const KLine& bar) {
        closes_.push_back(bar.close);
        if (closes_.size() > lookback_ + 1) closes_.pop_front();

        if (closes_.size() < lookback_ + 1) { current_ = OrderSide::NONE; return; }

        int64_t prev = closes_.front();          // N 根前收盘
        int64_t cur  = closes_.back();           // 当前收盘
        if (prev <= 0) { current_ = OrderSide::NONE; return; }

        double ret = (static_cast<double>(cur) - static_cast<double>(prev))
                   / static_cast<double>(prev);

        if      (ret >  threshold_) current_ = OrderSide::BUY;
        else if (ret < -threshold_) current_ = OrderSide::SELL;
        else                        current_ = OrderSide::NONE;
    }

    OrderSide signal() const { return current_; }

private:
    size_t lookback_;
    double threshold_;
    std::deque<int64_t> closes_;   // 最近 close 序列
    OrderSide current_ = OrderSide::NONE;
};
