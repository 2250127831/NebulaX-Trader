// 委托簿单测(V5): OrderManager 完整订单生命周期状态机 + 索引查询。
// 注意: 这是"自己订单的委托簿"(自有订单状态跟踪), 与市场订单簿
// (OrderBook, test_order_book.cpp 测的高性能价格档)是不同概念, 勿混淆。
// 验证:
//   - 状态机全流转: PENDING→A→SUBMITTED→request_cancel→PENDING_CANCEL→C→CANCELLED
//   - 半成交: E 部分→PARTIAL_FILL, remaining 递减, avg_fill_price 加权
//   - 填满: E 累积→FILLED(不可再撤)
//   - 非法流转拒收: FILLED 撤 / REJECTED 撤 / 非 PENDING 拒
//   - 索引: open_orders(symbol) / open_orders_by_strategy 只返回活态
//   - status(未知 id) == NOT_FOUND
//   - 终态订单从活态索引移除

#include "oms/order_manager.h"
#include "oms/order.h"
#include "core/types.h"

#include <cstdio>
#include <vector>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static Order mk_order(uint64_t sym, uint64_t strat, OrderSide side,
                      int64_t price, uint64_t qty) {
    Order o{};
    o.symbol_id = sym;
    o.strategy_id = strat;
    o.side = side;
    o.price = price;
    o.quantity = qty;
    o.type = OrderType::LIMIT;
    return o;
}

int main() {
    OrderManager om;

    // ── 1. 新单 → PENDING, status 正确 ──
    Order o1 = mk_order(1, 10, OrderSide::BUY, 10000, 100);
    uint64_t id1 = om.new_order(o1);
    CHECK(id1 != 0);
    CHECK(om.status(id1) == OrderStatus::PENDING);
    CHECK(om.status(9999) == OrderStatus::NOT_FOUND);   // 未知 id

    // ── 2. A → SUBMITTED, 活态索引含它 ──
    om.on_accept(id1);
    CHECK(om.status(id1) == OrderStatus::SUBMITTED);
    CHECK(om.open_orders(1).size() == 1);
    CHECK(om.open_orders_by_strategy(10).size() == 1);
    CHECK(om.open_orders(2).empty());   // 其他 symbol 无

    // ── 3. E 半成交 60 → PARTIAL_FILL, remaining 40, avg 10000 ──
    om.on_fill(id1, 60, 10000);
    CHECK(om.status(id1) == OrderStatus::PARTIAL_FILL);
    const OrderManager::Entry* e1 = om.entry(id1);
    CHECK(e1 != nullptr);
    CHECK(e1->filled == 60);
    CHECK(e1->remaining == 40);
    CHECK(e1->avg_fill_price == 10000);

    // ── 4. E 再成交 40 → FILLED(终态, 从活态索引移除) ──
    om.on_fill(id1, 40, 12000);
    CHECK(om.status(id1) == OrderStatus::FILLED);
    CHECK(om.open_orders(1).empty());   // 终态不在活态索引

    // ── 5. FILLED 订单不可撤(非法流转拒收) ──
    CHECK(!om.request_cancel(id1));
    CHECK(om.status(id1) == OrderStatus::FILLED);

    // ── 6. 撤单全流程: A → request_cancel → PENDING_CANCEL → C → CANCELLED ──
    Order o2 = mk_order(2, 20, OrderSide::SELL, 5000, 50);
    uint64_t id2 = om.new_order(o2);
    om.on_accept(id2);
    CHECK(om.request_cancel(id2));                      // 活态可撤
    CHECK(om.status(id2) == OrderStatus::PENDING_CANCEL);
    CHECK(om.on_cancel(id2));                           // C → CANCELLED
    CHECK(om.status(id2) == OrderStatus::CANCELLED);
    CHECK(om.open_orders(2).empty());                   // 终态移除索引

    // ── 7. 非 PENDING_CANCEL 时 C 拒收(非法流转) ──
    Order o3 = mk_order(3, 30, OrderSide::BUY, 8000, 30);
    uint64_t id3 = om.new_order(o3);
    om.on_accept(id3);                                  // SUBMITTED
    CHECK(!om.on_cancel(id3));                          // 没 request_cancel, C 拒收
    CHECK(om.status(id3) == OrderStatus::SUBMITTED);

    // ── 8. 非 PENDING 拒单拒收 ──
    Order o4 = mk_order(4, 40, OrderSide::BUY, 9000, 20);
    uint64_t id4 = om.new_order(o4);
    om.on_accept(id4);                                  // SUBMITTED
    om.on_reject(id4);                                  // 非 PENDING, 拒收
    CHECK(om.status(id4) == OrderStatus::SUBMITTED);

    // ── 9. PENDING 拒单 → REJECTED ──
    Order o5 = mk_order(5, 50, OrderSide::SELL, 7000, 15);
    uint64_t id5 = om.new_order(o5);
    om.on_reject(id5);
    CHECK(om.status(id5) == OrderStatus::REJECTED);
    CHECK(om.open_orders(5).empty());

    // ── 10. 同 symbol 多订单不同状态, 索引只返回活态 ──
    Order a = mk_order(6, 1, OrderSide::BUY, 100, 10);
    Order b = mk_order(6, 1, OrderSide::SELL, 110, 20);
    Order c = mk_order(6, 1, OrderSide::BUY, 105, 30);
    uint64_t ia = om.new_order(a);
    uint64_t ib = om.new_order(b);
    uint64_t ic = om.new_order(c);
    om.on_accept(ia);                 // 活
    om.on_accept(ib);                 // 活
    om.on_accept(ic);                 // 需先 accept 才能成交(状态机校验)
    om.on_fill(ic, 30, 105);          // FILLED(终态)
    CHECK(om.status(ia) == OrderStatus::SUBMITTED);
    CHECK(om.status(ib) == OrderStatus::SUBMITTED);
    CHECK(om.status(ic) == OrderStatus::FILLED);
    auto opens = om.open_orders(6);
    CHECK(opens.size() == 2);         // 只返回 ia/ib 两个活态
    bool has_ia = false, has_ib = false, has_ic = false;
    for (uint64_t id : opens) {
        if (id == ia) has_ia = true;
        if (id == ib) has_ib = true;
        if (id == ic) has_ic = true;
    }
    CHECK(has_ia && has_ib && !has_ic);

    // ── 11. iterate 全量迭代 ──
    size_t n = 0;
    om.iterate([&](uint64_t, const OrderManager::Entry&) { ++n; });
    CHECK(n == om.order_count());

    // ── 12. 半成交撤单: 部分成交后 request_cancel → CANCELLED ──
    Order o6 = mk_order(7, 60, OrderSide::BUY, 2000, 100);
    uint64_t id6 = om.new_order(o6);
    om.on_accept(id6);
    om.on_fill(id6, 40, 2000);        // PARTIAL_FILL
    CHECK(om.status(id6) == OrderStatus::PARTIAL_FILL);
    CHECK(om.request_cancel(id6));    // 部分成交仍可撤
    CHECK(om.status(id6) == OrderStatus::PENDING_CANCEL);
    CHECK(om.on_cancel(id6));
    CHECK(om.status(id6) == OrderStatus::CANCELLED);

    if (g_failures == 0) {
        printf("委托簿单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
