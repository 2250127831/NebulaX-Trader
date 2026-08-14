// 仲裁决策纯函数单测(V5): 加权净投票定方向 + 权重/阈值/强度加权平均/主策略。
// 验证:
//   - 全同向: 净信号为正 → 方向 + 强度 = 同向加权平均
//   - 部分同意: 一策略 BUY 一策略 NONE → 净信号 → 下单(OR 语义, 比 AND 宽松)
//   - 反方向按权重: BUY(1.0)+SELL(0.6) → 净信号 0.4 → BUY; 权重反转 → SELL
//   - 阈值观望: 净信号 ≤ 阈值 → 不下单
//   - 强度加权平均: 同向策略 strength×weight 加权
//   - primary 定标的/seq
//   - N=1 单策略净信号即定方向; N=0 / primary 越界 → 不决策

#include "strategy/base/arbitrate.h"
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

// 万分比权重: w(1.0)=10000
static constexpr int64_t W1 = 10000;
static constexpr int64_t W06 = 6000;

static constexpr int64_t kFull = Signal::kStrengthScale;   // 10000

static ArbSignal sig(OrderSide side, uint64_t locate, int64_t strength, uint64_t seq) {
    ArbSignal s;
    s.side = side; s.locate = locate; s.strength = strength; s.seq = seq;
    return s;
}

int main() {
    // ── 1. 全同向: OFI BUY(满强度) + OBI BUY(半强度), 权重 1.0/0.6 ──
    {
        ArbSignal sigs[2] = { sig(OrderSide::BUY, 1, kFull, 10),
                              sig(OrderSide::BUY, 2, kFull / 2, 11) };
        int64_t w[2] = { W1, W06 };
        ArbDecision d = arbitrate_decide(sigs, 2, w, 0, 0);
        CHECK(d.act);
        CHECK(d.dir == OrderSide::BUY);
        // 强度 = (10000×1.0 + 5000×0.6)/(1.0+0.6) = 13000/1.6 = 8125
        CHECK(d.strength == (kFull * W1 + (kFull / 2) * W06) / (W1 + W06));
        CHECK(d.locate == 1);   // primary(slot0) 的标的
        CHECK(d.seq == 10);     // primary(slot0) 的 seq
    }

    // ── 2. 部分同意(OR 语义): OFI BUY + OBI NONE → 净信号 1.0 → BUY ──
    {
        ArbSignal sigs[2] = { sig(OrderSide::BUY, 1, kFull, 10),
                              sig(OrderSide::NONE, 2, 0, 11) };
        int64_t w[2] = { W1, W06 };
        ArbDecision d = arbitrate_decide(sigs, 2, w, 0, 0);
        CHECK(d.act);
        CHECK(d.dir == OrderSide::BUY);
        // 强度: 只有 BUY 计入 = 10000
        CHECK(d.strength == kFull);
    }

    // ── 3. 反方向按权重: BUY(1.0) + SELL(0.6) → 净 0.4 → BUY ──
    {
        ArbSignal sigs[2] = { sig(OrderSide::BUY, 1, kFull, 10),
                              sig(OrderSide::SELL, 2, kFull, 11) };
        int64_t w[2] = { W1, W06 };
        ArbDecision d = arbitrate_decide(sigs, 2, w, 0, 0);
        CHECK(d.act);
        CHECK(d.dir == OrderSide::BUY);   // 权重差定方向
        CHECK(d.strength == kFull);       // 只有 BUY 计入
    }

    // ── 4. 权重反转: BUY(0.6) + SELL(1.0) → 净 -0.4 → SELL ──
    {
        ArbSignal sigs[2] = { sig(OrderSide::BUY, 1, kFull, 10),
                              sig(OrderSide::SELL, 2, kFull, 11) };
        int64_t w[2] = { W06, W1 };   // 反序权重(obi 权重更高)
        ArbDecision d = arbitrate_decide(sigs, 2, w, 0, 0);
        CHECK(d.act);
        CHECK(d.dir == OrderSide::SELL);
        CHECK(d.locate == 1);   // primary(slot0) 的标的, 即使方向是 SELL 来自 slot1
    }

    // ── 5. 阈值观望: 净 0.4 < 阈值 1.0 → 不下单 ──
    {
        ArbSignal sigs[2] = { sig(OrderSide::BUY, 1, kFull, 10),
                              sig(OrderSide::SELL, 2, kFull, 11) };
        int64_t w[2] = { W1, W06 };
        ArbDecision d = arbitrate_decide(sigs, 2, w, 0, W1);   // 阈值 1.0
        CHECK(!d.act);   // 净 0.4 ≤ 阈值 → 观望
    }

    // ── 6. primary 定标的: primary 是 slot1(非首个) ──
    {
        ArbSignal sigs[2] = { sig(OrderSide::BUY, 1, kFull, 10),
                              sig(OrderSide::BUY, 2, kFull, 20) };
        int64_t w[2] = { W1, W1 };
        ArbDecision d = arbitrate_decide(sigs, 2, w, 1, 0);   // primary=slot1
        CHECK(d.act);
        CHECK(d.locate == 2);
        CHECK(d.seq == 20);
    }

    // ── 7. N=1 单策略: 净信号即定方向 ──
    {
        ArbSignal sigs[1] = { sig(OrderSide::SELL, 7, kFull / 2, 30) };
        int64_t w[1] = { W1 };
        ArbDecision d = arbitrate_decide(sigs, 1, w, 0, 0);
        CHECK(d.act);
        CHECK(d.dir == OrderSide::SELL);
        CHECK(d.locate == 7);
        CHECK(d.strength == kFull / 2);
    }

    // ── 8. N=0 / primary 越界 → 不决策 ──
    {
        ArbDecision d0 = arbitrate_decide(nullptr, 0, nullptr, 0, 0);
        CHECK(!d0.act);
        ArbSignal sigs[1] = { sig(OrderSide::BUY, 1, kFull, 10) };
        int64_t w[1] = { W1 };
        ArbDecision d1 = arbitrate_decide(sigs, 1, w, 5, 0);   // primary 越界
        CHECK(!d1.act);
    }

    if (g_failures == 0) {
        printf("仲裁决策单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
