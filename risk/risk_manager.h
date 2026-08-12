#pragma once

#include "core/types.h"

#include <atomic>
#include <cstdint>

// ── 风险管理器 ──
// 下单前风控校验 + 成交回报更新持仓/盈亏 + 盯市回撤风控。价格、盈亏一律定点整数(分)，
// 遵守项目"内部计算不碰 double"约定。
//
// 回撤风控(V5, 盯市口径): 净值 = 初始资金 + 已实现 + Σ(持仓×(现价−成本))。
//   mark(locate, price) 喂现价(BookWorker 盘口), 盯市捕捉浮亏(不等平仓)。
//   分档触发: max_drawdown_pause(暂停新单) / max_drawdown_flatten(强制平仓)。
//
// 无锁设计(替代初始的互斥锁 + unordered_map):
//   locate 是 ITCH 16-bit(0-65535)，用固定数组按 locate 索引，上限 kMaxLocate=65536。
//   - position()/check_order() 从 book_th(下单线程)读 qty(原子) → 无锁快路径
//   - on_fill() 从 fill_th(回报线程)写 qty/avg_cost(单写者，ExecutionEngine 锁已串行化)
//   - mark() 从 book_th 写 mark_price(原子) / equity() 读(原子)
//   - 无 unordered_map → 无 rehash → 无悬空迭代器竞态(原段错误根因)
//   - realized_pnl_/equity_peak_ 原子(已由 ExecutionEngine 锁串行化，原子更干净 + 防御)
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
        , avg_cost_(new int64_t[kMaxLocate])
        , mark_price_(new std::atomic<int64_t>[kMaxLocate]) {
        for (uint32_t i = 0; i < kMaxLocate; ++i) {
            qty_[i].store(0, std::memory_order_relaxed);
            avg_cost_[i] = 0;
            mark_price_[i].store(0, std::memory_order_relaxed);
        }
    }
    ~RiskManager() { delete[] qty_; delete[] avg_cost_; delete[] mark_price_; }
    RiskManager(const RiskManager&) = delete;
    RiskManager& operator=(const RiskManager&) = delete;

    // 下单前校验：通过返回 true。
    // gate: 零量 / 单日亏损 / 回撤暂停(盯市回撤破档) / 持仓上限 / 禁止裸卖空。
    bool check_order(const Order& order) {
        if (order.quantity == 0) return false;
        if (realized_pnl_.load(std::memory_order_acquire) <= -max_daily_loss_) return false;
        if (drawdown_paused()) return false;   // 回撤破第一档: 暂停新单

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
        update_equity();   // 已实现变化反映到净值/峰值
    }

    // 配置
    void set_max_position(uint64_t max_pos) { max_position_ = max_pos; }
    void set_max_daily_loss(int64_t max_loss_fixed) { max_daily_loss_ = max_loss_fixed; }
    void set_initial_equity(int64_t eq) { initial_equity_ = eq; equity_peak_.store(eq, std::memory_order_relaxed); }
    void set_max_drawdown_pause(int64_t dd) { max_drawdown_pause_ = dd; }
    void set_max_drawdown_flatten(int64_t dd) { max_drawdown_flatten_ = dd; }

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

    // ── 盯市回撤(V5) ──

    // 喂现价(BookWorker 盘口): 更新 mark_price + 重算净值峰值。每笔事件调一次。
    void mark(uint64_t symbol_id, int64_t price) {
        mark_price_[locate_idx(symbol_id)].store(price, std::memory_order_relaxed);
        update_equity();
    }

    // 盯市净值 = 初始资金 + 已实现 + Σ(持仓×(现价−成本))。遍历所有 locate 累加浮盈。
    int64_t equity() const {
        int64_t eq = initial_equity_ + realized_pnl_.load(std::memory_order_acquire);
        for (uint32_t l = 0; l < kMaxLocate; ++l) {
            int64_t q = static_cast<int64_t>(qty_[l].load(std::memory_order_relaxed));
            int64_t mk = mark_price_[l].load(std::memory_order_relaxed);
            if (q > 0 && mk > 0)
                eq += q * (mk - avg_cost_[l]);   // 多头浮盈(禁止裸卖空, position 恒 ≥0)
        }
        return eq;
    }

    // 净值峰值(盯市 high-water mark)。
    int64_t equity_peak() const {
        return equity_peak_.load(std::memory_order_acquire);
    }

    // 当前回撤(绝对额, 分) = 峰值 − 当前净值。
    int64_t drawdown() const {
        return equity_peak_.load(std::memory_order_acquire) - equity();
    }

    // 回撤破第一档(暂停新单)。latch: 一旦破阈值锁定, 不随净值回升解除(实盘保守)。
    bool drawdown_paused() const {
        return drawdown() > max_drawdown_pause_;
    }

    // 回撤破第二档(强制平仓)。latch。
    bool drawdown_flatten() const {
        return drawdown() > max_drawdown_flatten_;
    }

private:
    // 重算净值峰值(equity_peak = max(peak, equity))。on_fill/mark 后调。
    void update_equity() {
        int64_t eq = equity();
        int64_t peak = equity_peak_.load(std::memory_order_relaxed);
        if (eq > peak)
            equity_peak_.store(eq, std::memory_order_relaxed);
    }

    // locate 是 ITCH 16-bit(0-65535)，越界防御(正常不会发生，防止异常数据越界写数组)。
    static uint32_t locate_idx(uint64_t symbol_id) {
        return (symbol_id < kMaxLocate) ? static_cast<uint32_t>(symbol_id) : 0;
    }

    uint64_t max_position_ = 100000;                    // 单标的持仓上限(股)
    int64_t  max_daily_loss_ = 1000000 * 100;           // 日最大亏损(分)，默认 100 万元
    int64_t  initial_equity_ = 0;                       // 初始资金(分), 净值基准
    int64_t  max_drawdown_pause_ = 0;                   // 第一档: 回撤暂停(绝对额, 分)
    int64_t  max_drawdown_flatten_ = 0;                 // 第二档: 回撤平仓(绝对额, 分)
    std::atomic<uint64_t>* const qty_;                  // 每 locate 持仓量(原子, position 读/on_fill 写)
    int64_t* const avg_cost_;                           // 每 locate 加权成本(仅 on_fill 单写者读写)
    std::atomic<int64_t>* const mark_price_;            // 每 locate 现价(原子, mark 写/盯市读)
    std::atomic<int64_t> realized_pnl_ = 0;             // 已实现盈亏(分)
    std::atomic<int64_t> equity_peak_ = 0;              // 净值峰值(盯市 high-water)
};
