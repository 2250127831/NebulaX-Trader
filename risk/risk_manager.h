#pragma once

#include "core/types.h"

#include <atomic>
#include <cstdint>

// ── 风险管理器 ──
// 下单前风控校验 + 成交回报更新持仓/盈亏。价格、盈亏一律定点整数(分)，
// 遵守项目"内部计算不碰 double"约定。
//
// 无锁设计(替代初始的互斥锁 + unordered_map):
//   locate 是 ITCH 16-bit(0-65535)，用固定数组按 locate 索引，上限 kMaxLocate=65536。
//   - position()/check_order() 从 book_th(下单线程)读 qty(原子) → 无锁快路径
//   - on_fill() 从 fill_th(回报线程)写 qty/avg_cost(单写者，ExecutionEngine 锁已串行化)
//   - 无 unordered_map → 无 rehash → 无悬空迭代器竞态(原段错误根因)
//   - realized_pnl_ 原子(已由 ExecutionEngine 锁串行化，原子更干净 + 防御)
//
// 并发正确性: 唯一裸竞态是 position()(book_th) vs on_fill()(fill_th) 读写 qty_。
//   固定数组 + 原子 qty 让 position() 是单次原子读 → 消除跨线程无同步访问的
//   C++ data race(UB，原段错误根源之一)，且防硬件撕裂读(不读到高低位拼凑的怪值)。
//   原子读不保证读到最新值(book_th 可能拿到 fill_th 写前的旧快照)，但风控判断
//   容忍短暂滞后(下单前持仓读数差几十股不影响是否超限)。avg_cost_ 仅 on_fill
//   单写者读写(EE 锁保证)，无需原子。
class RiskManager {
public:
    static constexpr uint32_t kMaxLocate = 65536;   // ITCH 16-bit locate 上限

    RiskManager()
        : qty_(new std::atomic<uint64_t>[kMaxLocate])
        , avg_cost_(new int64_t[kMaxLocate]) {
        for (uint32_t i = 0; i < kMaxLocate; ++i) {
            qty_[i].store(0, std::memory_order_relaxed);
            avg_cost_[i] = 0;
        }
    }
    ~RiskManager() { delete[] qty_; delete[] avg_cost_; }
    RiskManager(const RiskManager&) = delete;
    RiskManager& operator=(const RiskManager&) = delete;

    // 下单前校验：通过返回 true
    bool check_order(const Order& order) {
        if (order.quantity == 0) return false;
        if (realized_pnl_.load(std::memory_order_acquire) <= -max_daily_loss_) return false;

        uint64_t cur = qty_[locate_idx(order.symbol_id)].load(std::memory_order_acquire);
        if (order.side == OrderSide::BUY) {
            return cur + order.quantity <= max_position_;
        }
        if (order.side == OrderSide::SELL) {
            return order.quantity <= cur;  // 禁止裸卖空
        }
        return false;  // NONE 不产生订单
    }

    // 成交回报：更新持仓(加权成本)与已实现盈亏(默认整单成交, 用 order.quantity)。
    void on_fill(const Order& order) { on_fill(order, order.quantity); }

    // 成交回报(带实际成交量): 支持半成交(OUCH 多次 'E' 累积)。
    //   BUY  → 增持仓，重算加权平均成本
    //   SELL → 减持仓，按(卖出价 - 平均成本)×数量 结算盈亏
    // 单写者(ExecutionEngine 锁串行化 on_fill)，qty_ 原子 store 供 position() 读。
    void on_fill(const Order& order, uint64_t filled_qty) {
        uint32_t loc = locate_idx(order.symbol_id);
        if (order.side == OrderSide::BUY) {
            int64_t old_avg = avg_cost_[loc];
            uint64_t old_qty = qty_[loc].load(std::memory_order_relaxed);
            int64_t total = old_avg * (int64_t)old_qty
                          + order.price * (int64_t)filled_qty;
            uint64_t new_qty = old_qty + filled_qty;
            qty_[loc].store(new_qty, std::memory_order_relaxed);
            avg_cost_[loc] = new_qty ? total / (int64_t)new_qty : 0;
        } else if (order.side == OrderSide::SELL) {
            uint64_t old_qty = qty_[loc].load(std::memory_order_relaxed);
            uint64_t qty = filled_qty < old_qty ? filled_qty : old_qty;
            realized_pnl_.fetch_add((order.price - avg_cost_[loc]) * (int64_t)qty,
                                    std::memory_order_relaxed);
            uint64_t new_qty = old_qty - qty;
            qty_[loc].store(new_qty, std::memory_order_relaxed);
            if (new_qty == 0) avg_cost_[loc] = 0;
        }
    }

    // 配置
    void set_max_position(uint64_t max_pos) { max_position_ = max_pos; }
    void set_max_daily_loss(int64_t max_loss_fixed) { max_daily_loss_ = max_loss_fixed; }

    // 查询
    uint64_t position(uint64_t symbol_id) const {
        return qty_[locate_idx(symbol_id)].load(std::memory_order_acquire);
    }
    int64_t avg_cost(uint64_t symbol_id) const {
        return avg_cost_[locate_idx(symbol_id)];
    }
    int64_t realized_pnl() const {
        return realized_pnl_.load(std::memory_order_acquire);   // 已实现盈亏(分)
    }
    bool daily_loss_breached() const {
        return realized_pnl_.load(std::memory_order_acquire) <= -max_daily_loss_;
    }

private:
    // locate 是 ITCH 16-bit(0-65535)，越界防御(正常不会发生，防止异常数据越界写数组)。
    static uint32_t locate_idx(uint64_t symbol_id) {
        return (symbol_id < kMaxLocate) ? static_cast<uint32_t>(symbol_id) : 0;
    }

    uint64_t max_position_ = 100000;                    // 单标的持仓上限(股)
    int64_t  max_daily_loss_ = 1000000 * 100;           // 日最大亏损(分)，默认 100 万元
    std::atomic<uint64_t>* const qty_;                  // 每 locate 持仓量(原子, position 读/on_fill 写)
    int64_t* const avg_cost_;                           // 每 locate 加权成本(仅 on_fill 单写者读写)
    std::atomic<int64_t> realized_pnl_ = 0;             // 已实现盈亏(分)
};
