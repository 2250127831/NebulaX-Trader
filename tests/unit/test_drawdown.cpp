// 盯市回撤风控单测(V5): RiskManager 盯市净值 + 峰值 + 分档触发。
// 验证:
//   - 盯市净值: 初始资金 + 已实现 + Σ(持仓×(现价-成本))
//   - 峰值跟踪: mark 先升后跌, equity_peak 锁定高位, drawdown 正确
//   - 暂停触发(第一档): 回撤破档 → check_order 拒绝新单(REJECTED)
//   - 平仓触发(第二档): 回撤更深 → 活态订单被撤(PENDING_CANCEL)
//   - 分档: pause 先触发(活单还在), flatten 后活单全撤
//   - 未破阈值: 正常交易不受影响

#include "risk/risk_manager.h"
#include "oms/order_manager.h"
#include "execution/execution_engine.h"
#include "core/config.h"
#include "core/types.h"
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
    // ── 1. 盯市净值: 初始 100 万 + BUY 100@10000, mark 11000 → 浮盈 10000 ──
    {
        OrderManager om;
        RiskManager rm;
        rm.set_initial_equity(100000000);   // 100 万元
        rm.set_max_position(10000);
        ExecutionEngine ex(om, rm);
        ex.set_base_qty(100);
        CHECK(rm.equity() == 100000000);    // 初始净值
        CHECK(rm.equity_peak() == 100000000);
        // 无 sender → 模拟成交(accept + fill 全额)
        uint64_t id = ex.submit_signal(make_sig(OrderSide::BUY, 1, 10000,
                                                Signal::kStrengthScale), 1);
        CHECK(om.status(id) == OrderStatus::FILLED);
        CHECK(rm.position(1) == 100);
        // 盯市: mark 11000 → 浮盈 100股×(11000-10000)=100000 分
        rm.mark(1, 11000);
        CHECK(rm.equity() == 100000000 + 100000);
        CHECK(rm.equity_peak() == 100000000 + 100000);
        // mark 跌回 9000 → 浮亏 100×(9000-10000)=-100000, 净值低于初始
        rm.mark(1, 9000);
        CHECK(rm.equity() == 100000000 - 100000);
        CHECK(rm.equity_peak() == 100000000 + 100000);   // 峰值锁定高位
        CHECK(rm.drawdown() == 200000);   // 峰值-当前 = 100000+100000
    }

    // ── 2. 暂停触发(第一档): 回撤 > pause → 新单 REJECTED ──
    {
        OrderManager om;
        RiskManager rm;
        rm.set_initial_equity(100000000);
        rm.set_max_position(10000);
        rm.set_max_drawdown_pause(5000);      // 回撤 5000 分暂停
        rm.set_max_drawdown_flatten(100000);  // 平仓档设很大, 不触发
        ExecutionEngine ex(om, rm);
        ex.set_base_qty(100);
        ex.submit_signal(make_sig(OrderSide::BUY, 1, 10000, Signal::kStrengthScale), 1);
        CHECK(rm.position(1) == 100);
        CHECK(!rm.drawdown_paused());
        // mark 下跌 6000 → 浮亏, 回撤 6000 > 5000 → 暂停
        rm.mark(1, 4000);   // 100×(4000-10000) = -600000, 回撤 600000 > 5000
        CHECK(rm.drawdown() > 5000);
        CHECK(rm.drawdown_paused());
        // 新单被拒
        uint64_t id = ex.submit_signal(make_sig(OrderSide::BUY, 1, 10000,
                                                Signal::kStrengthScale), 1);
        CHECK(om.status(id) == OrderStatus::REJECTED);
        CHECK(rm.position(1) == 100);   // 未变
    }

    // ── 3. 平仓触发(第二档): 回撤破 flatten → 活单全撤 ──
    {
        OrderManager om;
        RiskManager rm;
        rm.set_initial_equity(1000000);       // 100 万元(分)... 用小基数便于触发
        rm.set_max_position(10000);
        rm.set_max_drawdown_pause(50000);     // 5 万元
        rm.set_max_drawdown_flatten(100000);  // 10 万元
        ExecutionEngine ex(om, rm);
        ex.set_base_qty(100);
        // 活单(部分成交)
        Order o{};
        o.symbol_id = 1;
        o.side = OrderSide::BUY;
        o.type = OrderType::LIMIT;
        o.price = 10000;
        o.quantity = 100;
        uint64_t id = om.new_order(o);
        om.on_accept(id);
        ex.on_order_fill(id, 40, 10000);   // 经 EE: OMS PARTIAL + risk 持仓 40
        CHECK(om.status(id) == OrderStatus::PARTIAL_FILL);
        CHECK(om.open_orders(1).size() == 1);
        CHECK(rm.position(1) == 40);
        // 已实现: 无; 浮亏 mark 大跌: 40×(1000-10000) = -360000
        // 净值 = 1000000 - 360000 = 640000, 回撤 360000 > 100000 → flatten
        rm.mark(1, 1000);
        CHECK(rm.drawdown() > 100000);
        CHECK(rm.drawdown_flatten());
        // 触发平仓: 通过一次成交(或用 fill 驱动评估)
        // 直接调 flatten_on_drawdown(内部会撤活单)
        ex.flatten_on_drawdown();
        CHECK(om.status(id) == OrderStatus::PENDING_CANCEL);   // 活单转撤单在途
        // PENDING_CANCEL 仍在活态索引(等 'C' 回报才真取消), 但没有可撮合活单了
        auto opens = om.open_orders(1);
        CHECK(opens.size() == 1);
        CHECK(om.status(opens[0]) == OrderStatus::PENDING_CANCEL);
    }

    // ── 4. 分档: pause 先触发(活单还在), flatten 后全撤 ──
    {
        OrderManager om;
        RiskManager rm;
        rm.set_initial_equity(1000000);
        rm.set_max_position(10000);
        rm.set_max_drawdown_pause(50000);     // 5 万
        rm.set_max_drawdown_flatten(200000);  // 20 万
        ExecutionEngine ex(om, rm);
        ex.set_base_qty(100);
        Order o{};
        o.symbol_id = 1; o.side = OrderSide::BUY;
        o.type = OrderType::LIMIT; o.price = 10000; o.quantity = 100;
        uint64_t id = om.new_order(o);
        om.on_accept(id);
        ex.on_order_fill(id, 40, 10000);   // 经 EE: OMS PARTIAL + risk 持仓 40
        // mark 中跌: 浮亏 40×(7000-10000)=-120000 → 回撤 120000 > pause(50000)
        rm.mark(1, 7000);
        CHECK(rm.drawdown() > 50000);
        CHECK(rm.drawdown_paused());            // 暂停触发
        CHECK(!rm.drawdown_flatten());          // 未到平仓
        CHECK(om.status(id) == OrderStatus::PARTIAL_FILL);   // 活单还在
        // mark 更跌: 回撤破 flatten → 平仓
        rm.mark(1, 3000);   // 浮亏 40×(3000-10000)=-280000 > 200000
        CHECK(rm.drawdown_flatten());
        ex.flatten_on_drawdown();
        CHECK(om.status(id) == OrderStatus::PENDING_CANCEL);   // 活单被撤
    }

    // ── 5. 未破阈值: 正常交易 ──
    {
        OrderManager om;
        RiskManager rm;
        rm.set_initial_equity(1000000);
        rm.set_max_position(10000);
        rm.set_max_drawdown_pause(500000);   // 大阈值
        rm.set_max_drawdown_flatten(1000000);
        ExecutionEngine ex(om, rm);
        ex.set_base_qty(100);
        uint64_t id = ex.submit_signal(make_sig(OrderSide::BUY, 1, 10000,
                                                Signal::kStrengthScale), 1);
        CHECK(om.status(id) == OrderStatus::FILLED);
        rm.mark(1, 9000);   // 小浮亏, 不破
        CHECK(!rm.drawdown_paused());
        uint64_t id2 = ex.submit_signal(make_sig(OrderSide::BUY, 1, 10000,
                                                 Signal::kStrengthScale), 1);
        CHECK(om.status(id2) == OrderStatus::FILLED);   // 正常下单
    }

    if (g_failures == 0) {
        printf("盯市回撤风控单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
