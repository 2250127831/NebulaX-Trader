#pragma once

#include "strategy/kline/kline_aggregator.h"
#include "strategy/base/signal.h"
#include "core/types.h"

#include <cmath>
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
        symbol_id_ = bar.symbol_id;
        last_ts_ = bar.timestamp;
        last_close_ = bar.close;
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

        // 强度：收益率 / 阈值，万分比，收益率=阈值即满强度
        if (current_ != OrderSide::NONE) {
            if (threshold_ <= 0) strength_ = Signal::kStrengthScale;
            else {
                double s = std::abs(ret) / threshold_ * (double)Signal::kStrengthScale;
                strength_ = (int64_t)(s > Signal::kStrengthScale ? Signal::kStrengthScale : s);
            }
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
    size_t lookback_;
    double threshold_;
    std::deque<int64_t> closes_;   // 最近 close 序列
    OrderSide current_ = OrderSide::NONE;
    uint64_t  symbol_id_ = 0;
    int64_t   last_close_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
