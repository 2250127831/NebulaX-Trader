// 策略信号强度单测
// 验证 3 个 tick 策略 + 趋势/动量 的 Signal.strength 计算符合预期。
// strength 语义: 万分比定点 [0, kStrengthScale]，满强度 = kStrengthScale。

#include "strategy/base/strategy.h"
#include "strategy/tick/volume_breakout_strategy.h"
#include "strategy/tick/price_breakout_strategy.h"
#include "strategy/tick/tick_momentum_strategy.h"
#include "strategy/trend/trend_strategy.h"
#include "strategy/momentum/momentum_strategy.h"

#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_trade(uint64_t locate, uint64_t ts, OrderSide side,
                              int64_t price, uint64_t vol) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::TRADE;
    ev.locate = locate;
    ev.timestamp = ts;
    ev.trade.side = side;
    ev.trade.price = price;
    ev.trade.volume = vol;
    return ev;
}

int main() {
    // ── 成交量突破: window=2, threshold=100 ──
    VolumeBreakoutStrategy vbs(2, 100);
    vbs.on_event(make_trade(7, 1, OrderSide::BUY, 10000, 60));
    vbs.on_event(make_trade(7, 2, OrderSide::BUY, 10000, 60));  // sum=120 > 100 → BUY
    Signal s = vbs.signal();
    CHECK(s.side == OrderSide::BUY);
    CHECK(s.locate == 7);
    CHECK(s.strength == (120 - 100) * Signal::kStrengthScale / 100);  // 2000
    // 极端放量: sum=300 → 强度封顶满强度
    VolumeBreakoutStrategy vbs2(2, 100);
    vbs2.on_event(make_trade(7, 1, OrderSide::BUY, 10000, 200));
    vbs2.on_event(make_trade(7, 2, OrderSide::BUY, 10000, 100));  // sum=300
    CHECK(vbs2.signal().strength == Signal::kStrengthScale);
    printf("成交量突破强度: %lld\n", (long long)s.strength);

    // ── 价格突破: window=3 ──
    PriceBreakoutStrategy pbs(3);
    pbs.on_event(make_trade(9, 1, OrderSide::BUY, 100, 1));
    pbs.on_event(make_trade(9, 2, OrderSide::BUY, 102, 1));
    pbs.on_event(make_trade(9, 3, OrderSide::BUY, 105, 1));
    pbs.on_event(make_trade(9, 4, OrderSide::BUY, 110, 1));  // 破 105 高点 → BUY
    s = pbs.signal();
    CHECK(s.side == OrderSide::BUY);
    CHECK(s.strength == (110 - 105) * Signal::kStrengthScale / 5);  // 满强度(幅度=波动)
    printf("价格突破强度: %lld\n", (long long)s.strength);

    // ── tick 动量: window=4, threshold=10 ──
    TickMomentumStrategy tms(4, 10);
    tms.on_event(make_trade(5, 1, OrderSide::BUY, 100, 1));
    tms.on_event(make_trade(5, 2, OrderSide::BUY, 100, 1));
    tms.on_event(make_trade(5, 3, OrderSide::BUY, 120, 1));
    tms.on_event(make_trade(5, 4, OrderSide::BUY, 120, 1));
    s = tms.signal();
    CHECK(s.side == OrderSide::BUY);                     // 前半VWAP 100 < 后半VWAP 120
    CHECK(s.strength == Signal::kStrengthScale);         // diff=20 > 10 → 满强度
    printf("tick 动量强度: %lld\n", (long long)s.strength);

    // ── 趋势: 上涨 → BUY, 强度 [0, 10000] ──
    TrendStrategy trend(3, 5);
    for (int64_t p = 100; p <= 120; ++p) {
        KLine b{}; b.symbol_id = 2; b.close = p; b.timestamp = (uint64_t)p;
        trend.on_bar(b);
    }
    s = trend.signal();
    CHECK(s.side == OrderSide::BUY);
    CHECK(s.strength > 0 && s.strength <= Signal::kStrengthScale);
    CHECK(s.locate == 2);
    printf("趋势强度: %lld\n", (long long)s.strength);

    // ── 动量(K线): 大涨 5% → BUY, 强度满 ──
    MomentumStrategy mom(5, 0.01);
    for (int64_t p = 100; p <= 105; ++p) {
        KLine b{}; b.close = p;
        mom.on_bar(b);
    }
    s = mom.signal();
    CHECK(s.side == OrderSide::BUY);
    CHECK(s.strength == Signal::kStrengthScale);  // 5% / 1% = 5 倍 → 封顶
    printf("动量强度: %lld\n", (long long)s.strength);

    if (g_failures == 0) {
        printf("\n信号强度单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
