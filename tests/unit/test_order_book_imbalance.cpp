// OBI 策略单测：验证盘口失衡信号的三种方向 + 边界
#include "strategy/book/order_book_imbalance_strategy.h"

#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static Tick make_tick(uint64_t bid_vol, uint64_t ask_vol) {
    Tick t{};
    t.bid_volume = bid_vol;
    t.ask_volume = ask_vol;
    return t;
}

int main() {
    OrderBookImbalanceStrategy s(0.3);  // 阈值 0.3

    // 买压显著：bid 800 / ask 200 → OBI = 0.6 ≥ 0.3 → BUY
    s.on_tick(make_tick(800, 200));
    CHECK(s.signal() == OrderSide::BUY);

    // 卖压显著：bid 200 / ask 800 → OBI = -0.6 ≤ -0.3 → SELL
    s.on_tick(make_tick(200, 800));
    CHECK(s.signal() == OrderSide::SELL);

    // 盘口均衡：bid 500 / ask 500 → OBI = 0.0 → NONE
    s.on_tick(make_tick(500, 500));
    CHECK(s.signal() == OrderSide::NONE);

    // 接近阈值但不越过：OBI = 0.2 < 0.3 → NONE
    s.on_tick(make_tick(600, 400));
    CHECK(s.signal() == OrderSide::NONE);

    // 盘口无挂单：0/0 → 不除零，NONE
    s.on_tick(make_tick(0, 0));
    CHECK(s.signal() == OrderSide::NONE);

    if (g_failures == 0) {
        printf("OBI 策略单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
