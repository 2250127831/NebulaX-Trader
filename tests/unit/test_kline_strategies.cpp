// K线聚合 + 趋势/动量策略单测
//   - KLineAggregator: 成交按 timestamp 窗口聚合 OHLC
//   - TrendStrategy: 均线交叉出信号
//   - MomentumStrategy: 收益率出信号
#include "strategy/kline/kline_aggregator.h"
#include "strategy/trend/trend_strategy.h"
#include "strategy/momentum/momentum_strategy.h"

#include <cstdio>
#include <cstdint>
#include <vector>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_trade(uint64_t ts, int64_t price, uint64_t vol) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::TRADE;
    ev.timestamp = ts;
    ev.trade.price = price;
    ev.trade.volume = vol;
    ev.locate = 1;
    return ev;
}

int main() {
    // ── K线聚合: 1 秒窗口 ──
    KLineAggregator agg(1000000000ull);  // 1 秒
    std::vector<KLine> bars;
    agg.set_sink([&](const KLine& b) { bars.push_back(b); });

    // 第 1 秒: 100, 102, 98, 101 → OHLC: O=100 H=102 L=98 C=101
    agg.on_trade(make_trade(0, 100, 10));
    agg.on_trade(make_trade(500000000, 102, 20));
    agg.on_trade(make_trade(900000000, 98, 5));
    agg.on_trade(make_trade(999999999, 101, 15));
    // 第 2 秒: 跨边界, 触发第 1 秒 K线完成
    agg.on_trade(make_trade(1000000000, 105, 30));
    // 第 3 秒: 跨边界, 触发第 2 秒 K线
    agg.on_trade(make_trade(2000000000, 110, 40));

    printf("bars 数量: %zu\n", bars.size());
    CHECK(bars.size() == 2);  // 第 2 秒触发第1秒, 第3秒触发第2秒
    if (bars.size() >= 1) {
        CHECK(bars[0].open == 100);
        CHECK(bars[0].high == 102);
        CHECK(bars[0].low  == 98);
        CHECK(bars[0].close == 101);
        CHECK(bars[0].volume == 50);
        printf("bar0: O=%lld H=%lld L=%lld C=%lld V=%llu\n",
               (long long)bars[0].open, (long long)bars[0].high,
               (long long)bars[0].low, (long long)bars[0].close,
               (unsigned long long)bars[0].volume);
    }

    // ── 趋势策略: 上涨趋势 → BUY ──
    TrendStrategy trend(3, 5);  // 短3 长5
    // 构造上升序列: 100,101,102,103,104,105 → 短均线上穿长均线 → BUY
    for (int64_t p = 100; p <= 110; p += 1) {
        KLine b{}; b.close = p;
        trend.on_bar(b);
    }
    printf("趋势信号: %d\n", (int)trend.signal());
    CHECK(trend.signal() == OrderSide::BUY);

    // 下降序列 → SELL
    TrendStrategy trend2(3, 5);
    for (int64_t p = 110; p >= 100; p -= 1) {
        KLine b{}; b.close = p;
        trend2.on_bar(b);
    }
    printf("趋势2信号: %d\n", (int)trend2.signal());
    CHECK(trend2.signal() == OrderSide::SELL);

    // ── 动量策略: 大涨 → BUY, 大跌 → SELL ──
    MomentumStrategy mom(5, 0.01);  // 5根 阈值1%
    // 大涨: 100 → 105 (5% > 1%) → BUY
    for (int64_t p : {100, 101, 102, 103, 104, 105}) {
        KLine b{}; b.close = p;
        mom.on_bar(b);
    }
    printf("动量信号: %d\n", (int)mom.signal());
    CHECK(mom.signal() == OrderSide::BUY);

    if (g_failures == 0) {
        printf("\nK线聚合 + 趋势/动量策略单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
