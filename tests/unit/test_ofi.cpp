// OFI 订单流失衡策略单测
// 验证: 逐笔委托方向累加 + 信号/强度
//   A 买 +shares / A 卖 -shares / D 买撤 -shares / D 卖撤 +shares
//   X 买部分撤 -shares / E 买单被吃 -shares / E 卖单被吃 +shares

#include "strategy/tick/order_flow_imbalance_strategy.h"
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

static MarketEvent make_x(uint64_t locate, uint64_t shares, uint64_t ref) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::CANCEL;
    ev.locate = locate;
    ev.timestamp = 1;
    ev.order.shares = shares;
    ev.order.order_ref = ref;
    return ev;
}

static MarketEvent make_e(uint64_t locate, uint64_t vol, uint64_t ref) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::EXECUTE;
    ev.locate = locate;
    ev.timestamp = 1;
    ev.trade.volume = vol;
    ev.trade.order_ref = ref;
    return ev;
}

int main() {
    OrderFlowImbalanceStrategy ofi(100);  // 阈值 100

    // ── 买入主导: A买100 + D卖撤50 + E卖单被吃30 = +180 → BUY ──
    ofi.on_event(make_add(1, OrderSide::BUY, 1000, 100, 1), OrderSide::BUY);
    CHECK(ofi.ofi() == 100);
    ofi.on_event(make_del(1, 50, 2), OrderSide::SELL);   // 卖侧撤 → +
    CHECK(ofi.ofi() == 150);
    ofi.on_event(make_e(1, 30, 3), OrderSide::SELL);     // 卖单被吃 → +
    CHECK(ofi.ofi() == 180);
    CHECK(ofi.signal().side == OrderSide::BUY);
    CHECK(ofi.signal().locate == 1);
    CHECK(ofi.signal().strength > 0 && ofi.signal().strength <= Signal::kStrengthScale);
    printf("买入主导 OFI=%lld signal=%d strength=%lld\n",
           (long long)ofi.ofi(), (int)ofi.signal().side,
           (long long)ofi.signal().strength);

    // ── 卖出主导: A卖100 + D买撤80 + X买部分撤20 = -200 → SELL ──
    ofi.reset();
    ofi.on_event(make_add(1, OrderSide::SELL, 1010, 100, 1), OrderSide::SELL);
    CHECK(ofi.ofi() == -100);
    ofi.on_event(make_del(1, 80, 2), OrderSide::BUY);    // 买侧撤 → -
    CHECK(ofi.ofi() == -180);
    ofi.on_event(make_x(1, 20, 3), OrderSide::BUY);      // 买侧部分撤 → -
    CHECK(ofi.ofi() == -200);
    CHECK(ofi.signal().side == OrderSide::SELL);
    printf("卖出主导 OFI=%lld signal=%d\n",
           (long long)ofi.ofi(), (int)ofi.signal().side);

    // ── 中性: 买卖抵消 → NONE ──
    ofi.reset();
    ofi.on_event(make_add(1, OrderSide::BUY, 1000, 60, 1), OrderSide::BUY);
    ofi.on_event(make_add(1, OrderSide::SELL, 1010, 60, 2), OrderSide::SELL);
    CHECK(ofi.ofi() == 0);
    CHECK(ofi.signal().side == OrderSide::NONE);
    CHECK(ofi.signal().strength == 0);
    printf("中性 OFI=%lld signal=%d\n",
           (long long)ofi.ofi(), (int)ofi.signal().side);

    // ── 强度封顶: 超阈值 10 倍 → 满强度 ──
    ofi.reset();
    ofi.on_event(make_add(1, OrderSide::BUY, 1000, 10000, 1), OrderSide::BUY);
    CHECK(ofi.signal().strength == Signal::kStrengthScale);
    printf("强度封顶: %lld\n", (long long)ofi.signal().strength);

    if (g_failures == 0) {
        printf("\nOFI 订单流失衡策略单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
