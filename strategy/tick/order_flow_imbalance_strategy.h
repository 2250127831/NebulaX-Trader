#pragma once

#include "strategy/base/strategy.h"
#include "market/book/order_book_consumer.h"

#include <cstdint>

// ── OFI 订单流失衡策略(逐笔委托，高频) ──
// 消费通道 B 的逐笔委托事件(ADD/DELETE/CANCEL/REPLACE/EXECUTE)，
// 累加订单流方向强度(Order Flow Imbalance, OFI)，判断买卖主导。
//
// OFI(经典 Cont et al.，适配 ITCH 事件类型)：
//   事件              方向来源        对 OFI 贡献
//   A 买单加量        A 自带 side     +shares
//   A 卖单加量        A 自带 side     -shares
//   D 买侧整撤        查簿 side       -shares   (买盘撤 = 卖方压力)
//   D 卖侧整撤        查簿 side       +shares   (卖盘撤 = 买方压力)
//   X 买侧部分撤      查簿 side       -shares
//   X 卖侧部分撤      查簿 side       +shares
//   E 买单被吃        查簿 side       -shares   (买侧挂单被成交吃 = 卖方主动)
//   E 卖单被吃        查簿 side       +shares
//
// 方向信息：D/X/E 事件只带 order_ref 不带 side，方向要靠订单簿查。
//   因此 OFI 策略与订单簿协同：回调里先用 OrderBookConsumer 查方向再累加。
//   这就是为什么 OFI 需要订单簿(盘口)支撑——它是逐笔委托的高频消费者。
//
// 强度：累计 OFI 超出阈值的部分，万分比，封顶满强度。
class OrderFlowImbalanceStrategy : public Strategy {
public:
    explicit OrderFlowImbalanceStrategy(int64_t threshold = 500)
        : threshold_(threshold) {}

    // 消费一个通道B事件。direction 由调用方从订单簿查得(仅 D/X/E 需要)。
    // A/U 事件自带 side，direction 传 side 即可。
    void on_event(const MarketEvent& ev, OrderSide side) {
        locate_ = ev.locate;
        last_ts_ = ev.timestamp;
        int64_t delta = 0;
        switch (ev.type) {
            case MarketEvent::Type::ADD:
                delta = (side == OrderSide::BUY) ? (int64_t)ev.order.shares
                                                 : -(int64_t)ev.order.shares;
                break;
            case MarketEvent::Type::DELETE:
            case MarketEvent::Type::CANCEL:
                delta = (side == OrderSide::BUY) ? -(int64_t)ev.order.shares
                                                 : +(int64_t)ev.order.shares;
                break;
            case MarketEvent::Type::REPLACE:
                delta = (side == OrderSide::BUY) ? (int64_t)ev.order.shares
                                                 : -(int64_t)ev.order.shares;
                break;
            case MarketEvent::Type::EXECUTE:
            case MarketEvent::Type::TRADE:
                delta = (side == OrderSide::BUY) ? -(int64_t)ev.trade.volume
                                                 : +(int64_t)ev.trade.volume;
                break;
            default:
                return;
        }
        ofi_ += delta;

        // 信号：|OFI| 超阈值 → 方向；强度 = |OFI|/阈值 封顶
        if (ofi_ > threshold_)      current_ = OrderSide::BUY;
        else if (ofi_ < -threshold_) current_ = OrderSide::SELL;
        else                         current_ = OrderSide::NONE;

        int64_t mag = (ofi_ > 0) ? ofi_ : -ofi_;
        int64_t s = mag * Signal::kStrengthScale / (threshold_ + 1);
        strength_ = s > Signal::kStrengthScale ? Signal::kStrengthScale : s;
        if (current_ == OrderSide::NONE) strength_ = 0;
    }

    // Strategy 接口：单参 on_event（OFI 需要方向，走上面的双参重载）
    void on_event(const MarketEvent& ev) override {
        // 无法确定方向：查簿逻辑在消费侧完成，这里不实现
        (void)ev;
    }

    Signal signal() const override {
        return Signal{.side = current_, .locate = locate_,
                      .price = last_price_, .timestamp = last_ts_,
                      .strength = strength_};
    }

    // 查询/重置
    int64_t ofi() const { return ofi_; }
    void reset() { ofi_ = 0; current_ = OrderSide::NONE; strength_ = 0; }

    void set_last_price(int64_t p) { last_price_ = p; }

private:
    int64_t threshold_;
    int64_t  ofi_ = 0;                 // 累计订单流失衡
    OrderSide current_ = OrderSide::NONE;
    uint64_t  locate_ = 0;
    int64_t   last_price_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
