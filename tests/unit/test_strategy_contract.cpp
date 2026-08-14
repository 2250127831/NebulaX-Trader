// 可插拔策略契约单测(V5 重构): StrategyT(CRTP) 统一入口的门控契约。
// 验证:
//   - OFI: ctx.side==NONE 的事件不喂窗口(方向查不到不污染 OFI)
//   - OFI: ctx.side 已知才喂; ctx.bid/ask 有效才更新现价
//   - OBI: ctx.book==nullptr 的事件不喂(无效盘口不产生信号)
//   - StrategyT reset() no-op 默认(无 reset 实现的策略调用不报错)
//   - 多态: 各策略经 StrategyT 基类调用 on_market/signal(CRTP 编译期绑定)

#include "strategy/base/strategy.h"
#include "strategy/tick/order_flow_imbalance_strategy.h"
#include "strategy/tick/order_book_imbalance_strategy.h"

#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_add(uint64_t locate, OrderSide side, int64_t price,
                            uint64_t shares, uint64_t ref) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::ADD;
    ev.locate = locate;
    ev.timestamp = 1;
    ev.order.side = side;
    ev.order.price = price;
    ev.order.shares = shares;
    ev.order.order_ref = ref;
    return ev;
}

static MarketEvent make_del(uint64_t locate, uint64_t shares, uint64_t ref) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::DELETE;
    ev.locate = locate;
    ev.timestamp = 1;
    ev.order.shares = shares;
    ev.order.order_ref = ref;
    return ev;
}

int main() {
    // ── 1. OFI 门控: side==NONE(D/X/E 方向查不到)不喂窗口, 不污染 OFI ──
    {
        OrderFlowImbalanceStrategy ofi(100);
        MarketEvent del = make_del(1, 50, 1);
        BookContext ctx;
        ctx.book = nullptr;
        ctx.side = OrderSide::NONE;   // 方向查不到
        ctx.bid = -1; ctx.ask = -1;   // 无盘口
        ofi.on_market(del, ctx);
        CHECK(ofi.ofi() == 0);   // 方向未知 → 跳过, 窗口不变
        CHECK(ofi.signal().side == OrderSide::NONE);
    }

    // ── 2. OFI 门控: side 已知才喂; 盘口有效才更新现价 ──
    {
        OrderFlowImbalanceStrategy ofi(100);
        MarketEvent add = make_add(1, OrderSide::BUY, 10000, 100, 2);
        BookContext ctx;
        ctx.book = nullptr;
        ctx.side = OrderSide::BUY;
        ctx.bid = 9900; ctx.ask = 10000; ctx.mid = 9950;
        ofi.on_market(add, ctx);
        CHECK(ofi.ofi() == 100);            // A 买 +100 → 窗口被喂
        CHECK(ofi.signal().price == 9950);  // 现价由 ctx.mid 驱动(set_last_price)

        // 盘口无效 → 不更新现价(维持上次)
        MarketEvent add2 = make_add(1, OrderSide::SELL, 10000, 100, 3);
        BookContext ctx_no_book;
        ctx_no_book.book = nullptr;
        ctx_no_book.side = OrderSide::SELL;
        ctx_no_book.bid = -1; ctx_no_book.ask = -1;   // 盘口无效
        ofi.on_market(add2, ctx_no_book);
        CHECK(ofi.signal().price == 9950);  // 现价不变
    }

    // ── 3. OBI 门控: book==nullptr(无盘口)不喂, 信号保持 NONE ──
    {
        OrderBookImbalanceStrategy obi(0.3);
        MarketEvent add = make_add(2, OrderSide::BUY, 10000, 100, 3);
        BookContext ctx;
        ctx.book = nullptr;                 // 无盘口
        ctx.bid = -1; ctx.ask = -1;
        obi.on_market(add, ctx);
        CHECK(obi.signal().side == OrderSide::NONE);   // 不产生信号
        CHECK(obi.signal().strength == 0);
    }

    // ── 4. reset no-op: OBI 无 reset 实现, 经 StrategyT 调用不报错(默认 no-op) ──
    {
        OrderBookImbalanceStrategy obi(0.3);
        obi.reset();   // 编译通过即契约成立
        OrderFlowImbalanceStrategy ofi(100);
        ofi.reset();   // OFI 覆写 reset
    }

    // ── 5. OFI reset 生效: 窗口清零 ──
    {
        OrderFlowImbalanceStrategy ofi(100);
        BookContext ctx;
        ctx.book = nullptr;
        ctx.side = OrderSide::BUY;
        ofi.on_market(make_add(1, OrderSide::BUY, 10000, 100, 1), ctx);
        CHECK(ofi.ofi() == 100);
        ofi.reset();
        CHECK(ofi.ofi() == 0);
    }

    if (g_failures == 0) {
        printf("策略契约单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
