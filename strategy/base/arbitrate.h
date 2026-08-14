#pragma once

#include "core/types.h"
#include <cstddef>
#include <cstdint>

// ── 多策略仲裁决策(加权净投票, 纯函数, 可单测) ──
// 输入: 各策略当前信号 + 权重 + 主策略索引 + 净投票阈值 → 输出下单决策。
// 与 BookWorker/ex/rm 解耦, 不依赖具体策略类型(编译期绑定的策略在 BookWorker
// 侧转成 ArbSignal[] 传入)。
//
// 仲裁模型(加权净投票):
//   net_bp = Σ_i (sign(side_i) × weight_bp[i])     // BUY=+1, SELL=-1, NONE=0
//   net_bp >  threshold_bp → BUY; net_bp < -threshold_bp → SELL; 否则观望(不下单)
//   → 策略可部分同意, 按权重定方向(比"全同向才下"更贴合现实策略配合)。
//
// 强度 = 与最终方向同向的策略加权平均(万分比):
//   Σ(side_i==dir 的 strength_i × weight_bp[i]) / Σ(side_i==dir 的 weight_bp[i])
//
// 标的/seq = 主策略(primary_idx)槽的信号(决定买哪个标的/触发 seq)。

struct ArbSignal {
    OrderSide side = OrderSide::NONE;
    uint64_t locate = 0;
    int64_t strength = 0;
    uint64_t seq = 0;
};

struct ArbDecision {
    bool act = false;          // 是否下单
    OrderSide dir = OrderSide::NONE;
    uint64_t locate = 0;
    int64_t strength = 0;      // 万分比 [0, kStrengthScale]
    uint64_t seq = 0;
};

// sigs/weights_bp: 长度 n 的数组; weights_bp 为万分比定点(10000=1.0, 缺省 1.0);
// threshold_bp: 净投票阈值(万分比, 0 = 任一策略有信号即按净信号方向下)。
inline ArbDecision arbitrate_decide(const ArbSignal* sigs, size_t n,
                                    const int64_t* weights_bp, size_t primary_idx,
                                    int64_t threshold_bp) {
    ArbDecision d;
    if (n == 0) return d;
    if (primary_idx >= n) return d;   // primary 越界: 不决策(调用方应启动校验)

    // 净信号: Σ sign(side) × weight
    int64_t net_bp = 0;
    for (size_t i = 0; i < n; ++i) {
        int64_t sign = (sigs[i].side == OrderSide::BUY) ? 1
                     : (sigs[i].side == OrderSide::SELL) ? -1 : 0;
        net_bp += sign * weights_bp[i];
    }
    if (net_bp >  threshold_bp)      d.dir = OrderSide::BUY;
    else if (net_bp < -threshold_bp) d.dir = OrderSide::SELL;
    else                             return d;   // 观望

    // 强度: 与 dir 同向的策略加权平均
    int64_t sum_w = 0, sum_sw = 0;
    for (size_t i = 0; i < n; ++i) {
        if (sigs[i].side == d.dir) {
            sum_w  += weights_bp[i];
            sum_sw += sigs[i].strength * weights_bp[i];
        }
    }
    d.strength = (sum_w > 0) ? sum_sw / sum_w : 0;

    // 标的/seq = 主策略
    d.locate = sigs[primary_idx].locate;
    d.seq    = sigs[primary_idx].seq;
    d.act = true;
    return d;
}
