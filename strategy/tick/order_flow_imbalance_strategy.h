#pragma once

#include "strategy/base/strategy.h"
#include "market/book/order_book_consumer.h"

#include <array>
#include <cstdint>

// ── OFI 订单流失衡策略(逐笔委托，高频) ──
// 消费单通道的逐笔委托事件(ADD/DELETE/CANCEL/REPLACE/EXECUTE)，
// 滑动窗口累计订单流方向强度(Order Flow Imbalance, OFI)，判断买卖主导。
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
//
// 窗口化: ofi_ = 最近 kWindow 笔委托的净流(环形缓冲, 满则减最旧)。
//   之前无限累计导致信号一旦超阈值就恒为 BUY/SELL 永不回摆, 高频下单锁死。
//   窗口化后信号随行情回摆, 方向能翻转, 高频下单才有意义。
//
// 强度：窗口 OFI 超阈值后分段线性(净流越强强度越高, 到 kSaturate×threshold 封顶满)。
//   连续化强度让"同方向强度爬坡"可被仲裁的强度阈值触发捕捉(牛市多次加仓);
//   旧公式(超阈值即满)导致同向强度恒满, 强度触发退化为只方向翻转。
class OrderFlowImbalanceStrategy : public StrategyT<OrderFlowImbalanceStrategy> {
public:
    explicit OrderFlowImbalanceStrategy(int64_t threshold = 500)
        : threshold_(threshold) {}

    // 框架统一入口(CRTP): 方向为 NONE(查不到)的事件不喂窗口, 盘口有效才更新现价。
    void on_market(const MarketEvent& ev, const BookContext& ctx) {
        if (ctx.side != OrderSide::NONE) on_event(ev, ctx.side);
        if (ctx.bid >= 0 && ctx.ask >= 0) set_last_price(ctx.mid);
    }

    // 强度饱和倍数: 窗口净流达到 kSaturate×threshold 时强度封顶满。
    //   2 = 两倍阈值满强度。调大 → 强度更平缓(更多中间档, 触发更频繁);
    //   调小 → 更快封顶(更少加仓)。5% 满强度为一档(kStrengthStep 在仲裁侧)。
    static constexpr int64_t kSaturate = 2;

    // 消费一个通道事件。direction 由调用方从订单簿查得(仅 D/X/E 需要)。
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
        // 滑动窗口: 加入新 delta, 满则减去最旧
        if (widx_ < kWindow) win_[widx_] = delta;
        else { ofi_ -= win_[widx_ % kWindow]; win_[widx_ % kWindow] = delta; }
        ofi_ += delta;
        ++widx_;

        // 信号：|窗口 OFI| 超阈值 → 方向；强度 = 分段线性, 到 kSaturate×threshold 封顶满
        if (ofi_ > threshold_)      current_ = OrderSide::BUY;
        else if (ofi_ < -threshold_) current_ = OrderSide::SELL;
        else                         current_ = OrderSide::NONE;

        if (current_ == OrderSide::NONE) {
            strength_ = 0;
        } else {
            // 强度 ∝ 超阈值的净流: (|ofi_| - threshold) / ((kSaturate-1)×threshold) → [0, 满]。
            int64_t mag = (ofi_ > 0) ? ofi_ : -ofi_;
            int64_t over = mag - threshold_;                 // 超阈值量
            int64_t span = threshold_ * (kSaturate - 1);     // 从阈值到饱和的跨度
            if (over >= span) strength_ = Signal::kStrengthScale;              // 饱和: 满强度
            else              strength_ = over * Signal::kStrengthScale / span; // 分段线性
        }
    }

    // 单参 on_event（历史测试/调用方用；方向由框架查好经双参/on_market 传入）
    void on_event(const MarketEvent& ev) {
        // 无法确定方向：查簿逻辑在消费侧完成，这里不实现
        (void)ev;
    }

    Signal signal() const {
        return Signal{.side = current_, .locate = locate_,
                      .price = last_price_, .timestamp = last_ts_,
                      .strength = strength_};
    }

    // 查询/重置
    int64_t ofi() const { return ofi_; }
    void reset() { ofi_ = 0; current_ = OrderSide::NONE; strength_ = 0;
                   for (auto& v : win_) v = 0; widx_ = 0; }

    void set_last_price(int64_t p) { last_price_ = p; }

private:
    // 滑动窗口大小(最近 N 笔委托的净流)。按行情尺度调, 默认 1024 笔。
    static constexpr size_t kWindow = 1024;
    int64_t threshold_;
    int64_t  ofi_ = 0;                 // 窗口内净订单流失衡
    std::array<int64_t, kWindow> win_{};   // 窗口内每笔 delta(环形)
    size_t   widx_ = 0;                // 窗口写入位置
    OrderSide current_ = OrderSide::NONE;
    uint64_t  locate_ = 0;
    int64_t   last_price_ = 0;
    uint64_t  last_ts_ = 0;
    int64_t   strength_ = 0;
};
