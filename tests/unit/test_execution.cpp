// 执行引擎单测：策略信号 → Order → 风控 → OMS → (模拟)成交 全链路
// 验证:
//   - 信号→数量换算(强度比例)
//   - 风控拦截(持仓上限/禁止裸卖空/日亏损上限)
//   - OMS 订单生命周期(PENDING→FILLED / REJECTED)
//   - 模拟成交更新 RiskManager 持仓与已实现盈亏

#include "execution/execution_engine.h"
#include "oms/order_manager.h"
#include "oms/ouch_order_codec.h"
#include "risk/risk_manager.h"
#include "strategy/base/signal.h"

#include <atomic>
#include <cstdio>
#include <thread>

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

// 假发送器: send 返回 len(假装发出), 置 sent 标志供测试同步。
// 不实际回 A/E, 订单经真实发送路径停在 PENDING。
struct MockSender : public IMarketDataSender {
    std::atomic<bool> sent{false};
    bool start() override { return true; }
    void stop() override {}
    void set_blocking(bool) override {}
    ssize_t send(const uint8_t*, size_t len) override {
        sent.store(true, std::memory_order_release);   // 测试同步点: send 已调用
        return static_cast<ssize_t>(len);
    }
    ssize_t recv(uint8_t*, size_t) override { return 0; }
    int fd() const override { return -1; }
};

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

    // ── 盘口查询(V5): thread 跑 query_book, 等 'Q' 发出后 on_book_quote 唤醒 ──
    {
        OrderManager om7; RiskManager rm7; ExecutionEngine ex7(om7, rm7);
        OuchOrderCodec codec7;
        MockSender ms7;
        ex7.set_codec(&codec7);
        ex7.set_sender(&ms7);
        BookQuote result;
        std::atomic<bool> got{false};
        std::thread qth([&]() {
            got.store(ex7.query_book(7, result, 2000), std::memory_order_release);
        });
        while (!ms7.sent.load(std::memory_order_acquire)) {}   // 等 send('Q'), 此时 book_waiting_=true
        BookQuote q7; q7.symbol_id = 7; q7.bid = 10000; q7.bid_vol = 300;
                     q7.ask = 10500; q7.ask_vol = 200;
        ex7.on_book_quote(q7);
        qth.join();
        CHECK(got.load(std::memory_order_acquire));   // 查询成功
        CHECK(result.bid == 10000);
        CHECK(result.ask == 10500);
        CHECK(result.bid_vol == 300);
        CHECK(result.ask_vol == 200);

        // 无 sender → query_book 直接失败
        OrderManager om8; RiskManager rm8; ExecutionEngine ex8(om8, rm8);
        BookQuote r8;
        CHECK(!ex8.query_book(7, r8));
    }

    // ── 市价→限价转换(V5): BUY → 查盘口 → 按 ask 价发 LIMIT 单(TIF='D') ──
    {
        OrderManager om9; RiskManager rm9; ExecutionEngine ex9(om9, rm9);
        OuchOrderCodec codec9;
        MockSender ms9;
        ex9.set_codec(&codec9);
        ex9.set_sender(&ms9);
        ex9.set_base_qty(100);
        Signal sig9 = make_sig(OrderSide::BUY, 9, 0, Signal::kStrengthScale);   // 市价(price=0)
        uint64_t oid9 = 0;
        std::thread th9([&]() { oid9 = ex9.submit_market_as_limit(sig9, 1); });
        while (!ms9.sent.load(std::memory_order_acquire)) {}   // 等 'Q' 发出
        BookQuote q9; q9.symbol_id = 9; q9.bid = 10000; q9.bid_vol = 300;
                     q9.ask = 10500; q9.ask_vol = 200;
        ex9.on_book_quote(q9);   // 唤醒 → submit_signal 发 LIMIT
        th9.join();
        CHECK(oid9 != 0);
        const OrderManager::Entry* e9 = om9.entry(oid9);
        CHECK(e9 != nullptr);
        CHECK(e9->order.type == OrderType::LIMIT);
        CHECK(e9->order.price == 10500);            // BUY 用 ask 价
        CHECK(e9->order.quantity == 100);
        CHECK(om9.status(oid9) == OrderStatus::PENDING);   // 有 sender 走真实发送, 等回报
    }

    // ── 市价→限价: SELL → 按 bid 价 ──
    {
        OrderManager omA; RiskManager rmA; ExecutionEngine exA(omA, rmA);
        OuchOrderCodec codecA;
        MockSender msA;
        exA.set_codec(&codecA);
        exA.set_sender(&msA);
        exA.set_base_qty(100);
        Signal sigA = make_sig(OrderSide::SELL, 10, 0, Signal::kStrengthScale);
        uint64_t oidA = 0;
        std::thread thA([&]() { oidA = exA.submit_market_as_limit(sigA, 1); });
        while (!msA.sent.load(std::memory_order_acquire)) {}
        BookQuote qA; qA.symbol_id = 10; qA.bid = 9900; qA.bid_vol = 400;
                     qA.ask = 10000; qA.ask_vol = 500;
        exA.on_book_quote(qA);
        thA.join();
        CHECK(oidA != 0);
        const OrderManager::Entry* eA = omA.entry(oidA);
        CHECK(eA != nullptr);
        CHECK(eA->order.type == OrderType::LIMIT);
        CHECK(eA->order.price == 9900);             // SELL 用 bid 价
    }

    // ── 查盘口失败(无 sender) → 不下单 ──
    {
        OrderManager omB; RiskManager rmB; ExecutionEngine exB(omB, rmB);
        exB.set_base_qty(100);
        Signal sigB = make_sig(OrderSide::BUY, 11, 0, Signal::kStrengthScale);
        CHECK(exB.submit_market_as_limit(sigB, 1) == 0);
        CHECK(omB.order_count() == 0);
    }

    if (g_failures == 0) {
        printf("\n执行引擎单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
