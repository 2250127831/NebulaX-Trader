#pragma once

#include "strategy/base/strategy.h"

// 订单簿失衡策略（Order Book Imbalance, OBI）
//
// 经典盘口信号：用买一/卖一的挂单量衡量买卖压力。
//
//   OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)   ∈ [-1, +1]
//     OBI >  +threshold  →  买压显著占优 → BUY
//     OBI <  -threshold  →  卖压显著占优 → SELL
//     其余               →  盘口均衡     → NONE（观望）
//
// 数据来源：订单簿重建后填充的 Tick::bid_volume / ask_volume（买一/卖一量）。
// 只消费盘口数据，不依赖成交流，是"订单簿 → 策略"链路的最简经典信号。
class OrderBookImbalanceStrategy : public Strategy {
public:
    explicit OrderBookImbalanceStrategy(double threshold = 0.3)
        : threshold_(threshold) {}

    void on_tick(const Tick& tick) override {
        uint64_t b = tick.bid_volume;
        uint64_t a = tick.ask_volume;
        if (b + a == 0) {            // 盘口无挂单，无信号
            current_ = OrderSide::NONE;
            return;
        }
        double obi = (static_cast<double>(b) - static_cast<double>(a))
                   / static_cast<double>(b + a);
        if      (obi >=  threshold_) current_ = OrderSide::BUY;
        else if (obi <= -threshold_) current_ = OrderSide::SELL;
        else                         current_ = OrderSide::NONE;
    }

    OrderSide signal() const override { return current_; }

private:
    double threshold_;
    OrderSide current_ = OrderSide::NONE;
};
