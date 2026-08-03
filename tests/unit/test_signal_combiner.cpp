// 主从信号组合器单测
// 验证: 主策略定方向, 从策略定强度
//   - 主 NONE → 不出单
//   - 主有方向 + 从同向 → 强度累加
//   - 从反向 → 削弱
//   - 从全反向/无 → 强度 0

#include "strategy/combo/signal_combiner.h"
#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static Signal mk(OrderSide side, int64_t strength) {
    Signal s{};
    s.side = side;
    s.locate = 1;
    s.price = 1000;
    s.strength = strength;
    return s;
}

int main() {
    // ── 主 NONE → 不出单 ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::NONE, 0));
        c.add_slave(mk(OrderSide::BUY, 8000));
        Signal d = c.combine();
        CHECK(d.side == OrderSide::NONE);
        CHECK(d.strength == 0);
        printf("主NONE: side=%d strength=%lld (不出单)\n",
               (int)d.side, (long long)d.strength);
    }

    // ── 主 BUY(5000) + 同向(6000) + 同向(3000) → 14000 封顶 10000 ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::BUY, 5000));
        c.add_slave(mk(OrderSide::BUY, 6000));
        c.add_slave(mk(OrderSide::BUY, 3000));
        Signal d = c.combine();
        CHECK(d.side == OrderSide::BUY);
        CHECK(d.strength == Signal::kStrengthScale);
        CHECK(d.locate == 1);
        printf("主BUY+从同向×2: strength=%lld (封顶)\n", (long long)d.strength);
    }

    // ── 主 BUY(5000) + 同向(6000) + 反向(SELL 4000) → 5000+6000-4000=7000 ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::BUY, 5000));
        c.add_slave(mk(OrderSide::BUY, 6000));
        c.add_slave(mk(OrderSide::SELL, 4000));
        Signal d = c.combine();
        CHECK(d.side == OrderSide::BUY);
        CHECK(d.strength == 7000);
        printf("主BUY+同向6000-反向4000: strength=%lld\n", (long long)d.strength);
    }

    // ── 从全反向(6000) → 5000-6000=-1000 → 0(强度0 不下单) ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::BUY, 5000));
        c.add_slave(mk(OrderSide::SELL, 6000));
        Signal d = c.combine();
        CHECK(d.side == OrderSide::BUY);
        CHECK(d.strength == 0);
        printf("从全反向: strength=%lld (强度0 不下单)\n", (long long)d.strength);
    }

    // ── 强度封顶: 主 SELL(5000) + 同向(8000+9000) → 22000 封顶 ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::SELL, 5000));
        c.add_slave(mk(OrderSide::SELL, 8000));
        c.add_slave(mk(OrderSide::SELL, 9000));
        Signal d = c.combine();
        CHECK(d.side == OrderSide::SELL);
        CHECK(d.strength == Signal::kStrengthScale);
        printf("强度封顶: strength=%lld\n", (long long)d.strength);
    }

    // ── 无从策略 → 主强度(5000) 保留 ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::BUY, 5000));
        Signal d = c.combine();
        CHECK(d.strength == 5000);
        printf("无从策略: strength=%lld\n", (long long)d.strength);
    }

    // ── 主无强度(0) + 从同向(6000) → 从补足 6000 ──
    {
        SignalCombiner c;
        c.set_primary(mk(OrderSide::BUY, 0));
        c.add_slave(mk(OrderSide::BUY, 6000));
        Signal d = c.combine();
        CHECK(d.strength == 6000);
        printf("主无强度+从同向: strength=%lld\n", (long long)d.strength);
    }

    if (g_failures == 0) {
        printf("\n主从信号组合器单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
