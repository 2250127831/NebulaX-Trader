#pragma once

#include "strategy/base/signal.h"
#include <cstdint>
#include <vector>

// ── 主从信号组合器 ──
// 多策略 → 单决策。主策略定方向，从策略定强度。
//
// 主从分层(经典)：
//   主策略 = 低频(趋势/动量)，定"该不该动"——方向权威
//   从策略 = 高频(OFI/OBI/成交量突破)，定"多狠"——强度放大/缩小
//
// 组合规则：
//   主 NONE        → 不产出信号(从策略再强也不动手)
//   主有方向        → 产出主方向信号，强度由从策略合成
//   从策略方向同主  → 贡献其强度(放大部分)
//   从策略方向反主  → 贡献负强度(削弱部分)
//   合成强度 = max(0, Σ 从策略同向强度 - Σ 从策略反向强度)
//            ，封顶 kStrengthScale。
//
// 用法：
//   SignalCombiner combiner;
//   combiner.set_primary(master.signal());          // 主策略信号
//   combiner.add_slave(ofi.signal());               // 逐个从策略信号
//   Signal decision = combiner.combine();           // 决策信号
class SignalCombiner {
public:
    // 设主策略信号(方向权威)
    void set_primary(const Signal& master) { primary_ = master; }

    // 加一个从策略信号(强度贡献)
    void add_slave(const Signal& slave) {
        slaves_.push_back(slave);
    }

    void clear_slaves() { slaves_.clear(); }

    // 合成决策信号
    Signal combine() const {
        Signal decision = primary_;

        // 主无方向 → 不出单
        if (primary_.side == OrderSide::NONE) {
            decision.strength = 0;
            return decision;
        }

        // 从策略合成强度
        int64_t same_dir = 0;    // 同向强度(与主一致)
        int64_t opp_dir  = 0;    // 反向强度(与主相反)
        for (const Signal& s : slaves_) {
            if (s.side == OrderSide::NONE || s.strength <= 0) continue;
            if (s.side == primary_.side) same_dir += s.strength;
            else                          opp_dir  += s.strength;
        }

        // 主从分层: 主策略定方向(权威), 从策略调强度。
        // 合成强度 = clamp(主强度 + Σ同向从 − Σ反向从, 0, 满)。
        //   主强度 = 基础仓位; 同向从放大; 反向从削减(可削到 0)。
        //   从策略反向足够强时, 能压过主 → 强度 0(不下单)。
        //   主无强度(0)时以主强度 0 为基数, 从同向补足。
        int64_t base = primary_.strength > 0 ? primary_.strength : 0;
        int64_t strength = base + same_dir - opp_dir;
        if (strength > Signal::kStrengthScale) strength = Signal::kStrengthScale;
        if (strength < 0) strength = 0;
        decision.strength = strength;
        return decision;
    }

    // 从策略个数
    size_t slave_count() const { return slaves_.size(); }

private:
    Signal primary_;
    // 从策略信号临时收集(combine 后由调用方 clear)
    std::vector<Signal> slaves_;
};
