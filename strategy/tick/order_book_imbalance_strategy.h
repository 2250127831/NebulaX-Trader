#pragma once

#include "strategy/base/strategy.h"
#include "market/book/order_book_consumer.h"

#include <cstdint>

// ── 订单簿失衡策略(逐笔委托/盘口) ──
// 消费通道 B 的委托事件(ADD/DELETE/CANCEL/REPLACE)，重建盘口，
// 用买卖盘口失衡度(Order Book Imbalance, OBI)判断方向。
//   OBI = (买一量 - 卖一量) / (买一量 + 卖一量) ∈ [-1, 1]
//     >  +threshold → 买方主导 → BUY
//     <  -threshold → 卖方主导 → SELL
//     否则 → NONE(观望)
//
// 高频策略：消费逐笔委托(通道B)，不看成交，比成交更早感知方向。
// 依赖 OrderBookConsumer 重建的盘口，通过 on_book() 喂盘口快照。
class OrderBookImbalanceStrategy : public Strategy {
public:
    explicit OrderBookImbalanceStrategy(double threshold = 0.3)
        : threshold_(threshold) {}

    // 盘口变化时由消费者调用：喂当前盘口快照
    void on_book(uint64_t locate, int64_t bid_price, uint64_t bid_vol,
                 int64_t ask_price, uint64_t ask_vol, uint64_t timestamp) {
        locate_     = locate;
        bid_price_  = bid_price;
        ask_price_  = ask_price;
        timestamp_  = timestamp;

        uint64_t sum = bid_vol + ask_vol;
        if (sum == 0) { current_ = OrderSide::NONE; strength_ = 0; return; }

        // OBI = (买量 - 卖量) / (买量 + 卖量)，定点 [−1,1] → 万分比 [−10000,10000]
        int64_t obi = (int64_t)(bid_vol - ask_vol) * Signal::kStrengthScale
                    / (int64_t)sum;
        int64_t th = (int64_t)(threshold_ * Signal::kStrengthScale);

        if (obi > th)        current_ = OrderSide::BUY;
        else if (obi < -th)  current_ = OrderSide::SELL;
        else                 current_ = OrderSide::NONE;

        // 强度：|OBI| 超过阈值的部分，封顶满强度
        int64_t mag = (obi > 0) ? obi : -obi;
        strength_ = (mag >= th) ? Signal::kStrengthScale
                    : mag * Signal::kStrengthScale / (th + 1);
    }

    // 逐笔委托事件（通道B）：由消费者重建盘口后调 on_book
    void on_event(const MarketEvent& ev) override {
        // 本策略不做事件内联处理，盘口重建由 OrderBookConsumer 完成，
        // 消费方在重建后调用 on_book()。这里保留接口契约。
        (void)ev;
    }

    Signal signal() const override {
        return Signal{.side = current_, .locate = locate_,
                      .price = (current_ == OrderSide::BUY) ? bid_price_ : ask_price_,
                      .timestamp = timestamp_, .strength = strength_};
    }

private:
    double threshold_;
    OrderSide current_ = OrderSide::NONE;
    uint64_t  locate_ = 0;
    int64_t   bid_price_ = 0;
    int64_t   ask_price_ = 0;
    uint64_t  timestamp_ = 0;
    int64_t   strength_ = 0;
};
