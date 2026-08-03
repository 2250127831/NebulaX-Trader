#pragma once

#include "core/types.h"
#include <cstdint>
#include <unordered_map>

// ── 风险管理器 ──
// 下单前风控校验 + 成交回报更新持仓/盈亏。价格、盈亏一律定点整数(分)，
// 遵守项目"内部计算不碰 double"约定。
//
// 持仓按 symbol(locate) 分账，维护加权平均成本；平仓时结算已实现盈亏。
// V1 提供三类校验：
//   1. 单标的持仓上限：BUY 后总持仓 ≤ max_position_
//   2. 禁止裸卖空：SELL 量不得超过当前持仓
//   3. 日亏损上限：已实现亏损达到 max_daily_loss_ 后拒绝一切新单
class RiskManager {
public:
    // 下单前校验：通过返回 true
    bool check_order(const Order& order) {
        if (order.quantity == 0) return false;
        if (daily_loss_breached()) return false;

        auto it = pos_.find(order.symbol_id);
        uint64_t cur = it == pos_.end() ? 0 : it->second.qty;

        if (order.side == OrderSide::BUY) {
            return cur + order.quantity <= max_position_;
        }
        if (order.side == OrderSide::SELL) {
            return order.quantity <= cur;  // 禁止裸卖空
        }
        return false;  // NONE 不产生订单
    }

    // 成交回报：更新持仓(加权成本)与已实现盈亏。
    //   BUY  → 增持仓，重算加权平均成本
    //   SELL → 减持仓，按(卖出价 - 平均成本)×数量 结算盈亏
    void on_fill(const Order& order) {
        auto& st = pos_[order.symbol_id];
        if (order.side == OrderSide::BUY) {
            int64_t total = st.avg_cost * (int64_t)st.qty
                          + order.price * (int64_t)order.quantity;
            st.qty += order.quantity;
            st.avg_cost = st.qty ? total / (int64_t)st.qty : 0;
        } else if (order.side == OrderSide::SELL) {
            uint64_t qty = order.quantity < st.qty ? order.quantity : st.qty;
            realized_pnl_ += (order.price - st.avg_cost) * (int64_t)qty;
            st.qty -= qty;
            if (st.qty == 0) st.avg_cost = 0;
        }
    }

    // 配置
    void set_max_position(uint64_t max_pos) { max_position_ = max_pos; }
    void set_max_daily_loss(int64_t max_loss_fixed) { max_daily_loss_ = max_loss_fixed; }

    // 查询
    uint64_t position(uint64_t symbol_id) const {
        auto it = pos_.find(symbol_id);
        return it == pos_.end() ? 0 : it->second.qty;
    }
    int64_t avg_cost(uint64_t symbol_id) const {
        auto it = pos_.find(symbol_id);
        return it == pos_.end() ? 0 : it->second.avg_cost;
    }
    int64_t realized_pnl() const { return realized_pnl_; }   // 已实现盈亏(分)
    bool daily_loss_breached() const { return realized_pnl_ <= -max_daily_loss_; }

private:
    struct PositionState {
        uint64_t qty = 0;
        int64_t  avg_cost = 0;   // 加权平均成本(分)
    };

    uint64_t max_position_ = 100000;                 // 单标的持仓上限(股)
    int64_t  max_daily_loss_ = 1000000 * 100;        // 日最大亏损(分)，默认 100 万元
    std::unordered_map<uint64_t, PositionState> pos_;  // symbol → 持仓
    int64_t  realized_pnl_ = 0;                       // 已实现盈亏(分)
};
