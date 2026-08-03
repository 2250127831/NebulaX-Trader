// 高性能订单簿单测（迁移自 NebulaX matching OrderBook）
// 验证: 链表式价格档 + 池化挂单 + OrderMap 索引
//   - ADD/REMOVE/CANCEL/EXECUTE 生命周期
//   - TopOfBook 盘口重建
//   - 池化零堆分配(池用量守恒)

#include "market/book/order_book.h"
#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

int main() {
    OrderPool pool(1024);
    OrderMap  index(1024);
    OrderBook book(pool, index);

    // ── ADD: 买 100@1000, 买 200@990, 卖 300@1010, 卖 400@1020 ──
    CHECK(book.add(1, OrderSide::BUY,  1000, 100, 1) != UINT32_MAX);
    CHECK(book.add(2, OrderSide::BUY,  990,  200, 2) != UINT32_MAX);
    CHECK(book.add(3, OrderSide::SELL, 1010, 300, 3) != UINT32_MAX);
    CHECK(book.add(4, OrderSide::SELL, 1020, 400, 4) != UINT32_MAX);

    CHECK(book.best_bid() == 1000);
    CHECK(book.best_ask() == 1010);
    CHECK(book.best_bid_volume() == 100);
    CHECK(book.best_ask_volume() == 300);
    CHECK(book.bid_levels() == 2);
    CHECK(book.ask_levels() == 2);
    CHECK(book.bid_volume_at(990) == 200);
    CHECK(book.ask_volume_at(1020) == 400);

    // ── 查询挂单视图 ──
    OrderBook::OrderSlotView v;
    CHECK(book.get(1, v));
    CHECK(v.order_ref == 1 && v.side == OrderSide::BUY && v.price == 1000);
    CHECK(v.remaining == 100 && v.shares == 100);
    printf("best_bid=%lld@%lld best_ask=%lld@%lld\n",
           (long long)book.best_bid(), (long long)book.best_bid_volume(),
           (long long)book.best_ask(), (long long)book.best_ask_volume());

    // ── EXECUTE: 挂单1 成交 40 → 剩余 60, 买一量 60 ──
    CHECK(book.execute(1, 40));
    CHECK(book.best_bid_volume() == 60);
    CHECK(book.get(1, v));
    CHECK(v.remaining == 60);
    // 池用量: 4 个挂单, 0 释放
    CHECK(book.pool_usage() == 4);

    // ── CANCEL: 挂单2 部分撤 80 → 剩余 120, 990 档量 120 ──
    CHECK(book.cancel(2, 80));
    CHECK(book.bid_volume_at(990) == 120);
    CHECK(book.get(2, v));
    CHECK(v.remaining == 120);

    // ── REMOVE: 挂单4 整笔撤 → 1020 档消失 ──
    CHECK(book.remove(4));
    CHECK(book.best_ask() == 1010);   // 只剩 1010
    CHECK(book.ask_volume_at(1020) == 0);
    CHECK(!book.get(4, v));           // 已不存在
    CHECK(book.ask_levels() == 1);

    // ── REPLACE: 挂单3 改到 1050 ──
    CHECK(book.replace(3, 33, OrderSide::SELL, 1050, 300, 5));
    CHECK(book.best_ask() == 1050);
    CHECK(!book.get(3, v));           // 旧 ref 没了
    CHECK(book.get(33, v));           // 新 ref 在
    CHECK(v.price == 1050 && v.remaining == 300);

    // ── 池用量守恒: ──
    // 4 add(4在池) → execute/cancel 不释放(4)
    // → remove 挂单4(3) → replace: remove 挂单3(2) + add 33(3) = 3 在池
    CHECK(book.pool_usage() == 3);

    // ── 不存在的挂单操作 ──
    CHECK(!book.remove(999));
    CHECK(!book.cancel(999, 10));
    CHECK(!book.execute(999, 10));

    // ── 空簿 ──
    OrderPool epool(16);
    OrderMap  eindex(16);
    OrderBook empty_book(epool, eindex);
    CHECK(empty_book.best_bid() == -1);
    CHECK(empty_book.best_ask() == -1);
    CHECK(empty_book.empty());

    if (g_failures == 0) {
        printf("\n高性能订单簿单测 PASS ✓ (池用量=%zu)\n", book.pool_usage());
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
