#pragma once

#include "core/market_event.h"
#include "core/types.h"
#include "strategy/base/signal.h"

// ── 策略上下文: 框架算一次, 传给各策略(避免每策略重复查簿) ──
// BookWorker::process 每条事件算一次 side + BBO + 现价, 填进 ctx 传所有策略。
//   side: 事件方向(A/U 自带, D/X/E 查簿); 查不到为 NONE(调用方按此门控)。
//   bid/ask: best bid/ask, 盘口无效为 -1(策略按此判断有无对手盘)。
//   book: 前向声明的订单簿指针; 策略本身上下文里不解引用, 只读 BBO 字段。
struct BookContext {
    const class OrderBook* book = nullptr;
    OrderSide side = OrderSide::NONE;
    int64_t mid = -1;
    int64_t bid = -1;
    uint64_t bid_vol = 0;
    int64_t ask = -1;
    uint64_t ask_vol = 0;
    uint64_t seq = 0;
};

// ── 策略契约(CRTP 编译期多态) ──
// on_market/signal 在编译期绑定到具体实现(可内联, 无虚调用, 无间接跳转)。
// 热路径(每条行情事件)走 on_market(ev, ctx); signal() 供框架读当前信号。
// 框架侧门控契约(策略实现必须遵守):
//   - 方向查不到的 D/X/E 事件, ctx.side == NONE —— 需要方向的策略必须跳过
//     (否则会把它当"卖侧撤=买方压力"污染窗口)。
//   - 盘口无效时 ctx.book == nullptr / bid/ask == -1 —— 需要盘口的策略必须跳过。
// reset() 默认 no-op; 需要段边界重置的策略覆写(如 OFI 的窗口清零)。
template <class Impl>
class StrategyT {
public:
    void on_market(const MarketEvent& ev, const BookContext& ctx) {
        static_cast<Impl*>(this)->on_market(ev, ctx);
    }
    Signal signal() const { return static_cast<const Impl*>(this)->signal(); }
    void reset() {}
};
