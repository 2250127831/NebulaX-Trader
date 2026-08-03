// 执行引擎单测：策略信号 → Order → 风控 → OMS → (模拟)成交 全链路
// 验证:
//   - 信号→数量换算(强度比例)
//   - 风控拦截(持仓上限/禁止裸卖空/日亏损上限)
//   - OMS 订单生命周期(PENDING→FILLED / REJECTED)
//   - 模拟成交更新 RiskManager 持仓与已实现盈亏

#include "execution/execution_engine.h"
#include "oms/order_manager.h"
#include "risk/risk_manager.h"
#include "strategy/base/signal.h"

#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static Signal make_sig(OrderSide side, uint64_t locate, int64_t price,
                       int64_t strength, uint64_t ts = 1) {
    Signal s{};
    s.side = side;
    s.locate = locate;
    s.price = price;
    s.timestamp = ts;
    s.strength = strength;
    return s;
}

int main() {
    OrderManager om;
    RiskManager rm;
    ExecutionEngine ex(om, rm);
    ex.set_base_qty(100);

    // ── 全链路: 满强度 BUY → qty=100 → FILLED, 持仓 100, 成本 10000 ──
    uint64_t id = ex.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale), 1);
    CHECK(id != 0);
    CHECK(om.status(id) == OrderStatus::FILLED);
    CHECK(rm.position(1) == 100);
    CHECK(rm.avg_cost(1) == 10000);
    printf("buy id=%llu pos=%llu avg_cost=%lld\n",
           (unsigned long long)id, (unsigned long long)rm.position(1),
           (long long)rm.avg_cost(1));

    // ── 半强度 → qty=50 ──
    OrderManager om2; RiskManager rm2; ExecutionEngine ex2(om2, rm2);
    ex2.set_base_qty(100);
    id = ex2.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale / 2), 1);
    CHECK(om2.status(id) == OrderStatus::FILLED);
    CHECK(rm2.position(1) == 50);
    printf("半强度 qty: %llu\n", (unsigned long long)rm2.position(1));

    // ── 零强度 / NONE → 不下单 ──
    OrderManager om3; RiskManager rm3; ExecutionEngine ex3(om3, rm3);
    ex3.set_base_qty(100);
    CHECK(ex3.submit_signal(make_sig(OrderSide::BUY, 1, 10000, 0), 1) == 0);
    CHECK(ex3.submit_signal(make_sig(OrderSide::NONE, 1, 10000, Signal::kStrengthScale), 1) == 0);

    // ── 禁止裸卖空: 无持仓 SELL → REJECTED ──
    OrderManager om4; RiskManager rm4; ExecutionEngine ex4(om4, rm4);
    ex4.set_base_qty(100);
    id = ex4.submit_signal(make_sig(OrderSide::SELL, 1, 10000, Signal::kStrengthScale), 1);
    CHECK(om4.status(id) == OrderStatus::REJECTED);
    CHECK(rm4.position(1) == 0);

    // ── 持仓上限: max_position=150, 已持 100, 再满量 BUY 100 → 超限 REJECTED ──
    OrderManager om5; RiskManager rm5; ExecutionEngine ex5(om5, rm5);
    ex5.set_base_qty(100);
    rm5.set_max_position(150);
    CHECK(om5.status(ex5.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale), 1))
          == OrderStatus::FILLED);            // 100 ≤ 150 → 成交
    id = ex5.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale), 1);
    CHECK(om5.status(id) == OrderStatus::REJECTED);  // 100+100 > 150 → 拒
    CHECK(rm5.position(1) == 100);                    // 持仓不变

    // ── 日亏损上限: 高买低卖亏损 → 后续新单全拒 ──
    OrderManager om6; RiskManager rm6; ExecutionEngine ex6(om6, rm6);
    ex6.set_base_qty(100);
    rm6.set_max_daily_loss(50000);            // 500 元
    ex6.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale), 1);   // 买 100 @100
    ex6.submit_signal(make_sig(OrderSide::SELL, 1, 9000, Signal::kStrengthScale), 1);   // 卖 100 @90 → -1000元
    CHECK(rm6.realized_pnl() == (9000 - 10000) * 100);
    CHECK(rm6.daily_loss_breached());
    id = ex6.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale), 1);
    CHECK(om6.status(id) == OrderStatus::REJECTED);   // 亏损超限 → 拒
    printf("日亏损: %lld 分, 触发拒单 ✓\n", (long long)rm6.realized_pnl());

    // ── 订单登记可回查: 全部已登记 ──
    CHECK(om.order_count() == 1);

    if (g_failures == 0) {
        printf("\n执行引擎单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
