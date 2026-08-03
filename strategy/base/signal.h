#pragma once

#include "core/types.h"

#include <cstdint>

// ── 策略信号 ──
// 策略的产出：方向 + 标的 + 参考价 + 时间戳 + 强度。
// 相比裸 OrderSide，带上标的(locate)让信号可直接转成 Order 送 OMS：
//   Order.symbol_id ← Signal.locate（同源：协议股票 key）。
//
//   - side:      方向(BUY/SELL/NONE 观望)
//   - locate:    股票(Stock Locate，协议级股票 key，与 MarketEvent.locate 同源)
//   - price:     触发信号时的参考价(分，定点整数)；无信号/无价时 0
//   - timestamp: 最后一笔触发事件的行情时间戳(纳秒)
//   - strength:  信号强度，万分比定点整数 [0, kStrengthScale]，0 = 无信号。
//                经典策略不直接给下单数量——数量是资金管理层(ExecutionEngine)的职责，
//                由 strength × 基础数量换算。强度语义由各策略自行定义并写注释。
struct Signal {
    static constexpr int64_t kStrengthScale = 10000;  // 强度单位：万分比

    OrderSide side = OrderSide::NONE;
    uint64_t  locate    = 0;
    int64_t   price     = 0;
    uint64_t  timestamp = 0;
    int64_t   strength  = 0;   // [0, kStrengthScale]
};
