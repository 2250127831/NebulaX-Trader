#pragma once

#include "oms/order.h"
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ── 委托簿（订单管理器）──
// 维护自己全部订单的生命周期状态机，持有订单全部信息(不只状态)供执行/风控/查询回查。
// 由 ExecutionEngine 的 on_order_report(Fill) 驱动: 'A'接受 / 'E'成交 / 'C'撤单 / 'J'拒绝。
//
// 状态机:
//   PENDING →(A)→ SUBMITTED →(E 部分)→ PARTIAL_FILL →(E 全)→ FILLED
//                ↓ request_cancel          ↓ request_cancel
//           PENDING_CANCEL          PENDING_CANCEL
//                ↓ 'C'                    ↓ 'C'
//           CANCELLED                CANCELLED
//   PENDING →(J)→ REJECTED
//
// 索引: 按 symbol / strategy 的 open_orders 查询(活态订单)。
// 线程安全: 由 ExecutionEngine 的互斥锁串行化, 本类非线程安全。
class OrderManager {
public:
    struct Entry {
        Order order;
        OrderStatus status;
        uint64_t exchange_ref = 0;   // 交易所分配的 Order Reference Number('A' 带回, 0=未接受)
        uint64_t filled = 0;         // 已成交量
        uint64_t remaining = 0;      // 剩余未成交量(quantity - filled)
        int64_t  avg_fill_price = 0; // 加权成交均价(分)
        uint64_t submit_time = 0;    // 下单时间戳
        uint64_t update_time = 0;    // 最后状态更新时间戳
    };

    // 登记新单：分配 order_id 并写回 order.order_id，状态 PENDING，返回 id。
    uint64_t new_order(Order& order) {
        order.order_id = next_id_++;
        Entry e{};
        e.order = order;
        e.status = OrderStatus::PENDING;
        e.filled = 0;
        e.remaining = order.quantity;
        e.avg_fill_price = 0;
        e.submit_time = order.timestamp;
        e.update_time = order.timestamp;
        orders_[order.order_id] = e;
        sym_orders_[order.symbol_id].insert(order.order_id);      // symbol 索引
        strat_orders_[order.strategy_id].insert(order.order_id);  // strategy 索引
        return order.order_id;
    }

    // 订单被交易所接受(OUCH 'A'): PENDING → SUBMITTED(活态)。
    // ref: 交易所分配的 Order Reference Number, 记录供对账/审计。
    void on_accept(uint64_t order_id, uint64_t ref = 0) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return;
        if (it->second.status == OrderStatus::PENDING) {
            it->second.status = OrderStatus::SUBMITTED;
            it->second.exchange_ref = ref;
            it->second.update_time = now();
        }
    }

    // 成交回报(OUCH 'E'): 累积已成交, 更新剩余/均价, 切换状态。
    // 校验: 仅活态(SUBMITTED/PARTIAL_FILL)可成交; 不过量(filled ≤ quantity)。
    void on_fill(uint64_t order_id, uint64_t filled_qty, int64_t fill_price) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return;
        Entry& e = it->second;
        if (e.status != OrderStatus::SUBMITTED &&
            e.status != OrderStatus::PARTIAL_FILL) return;   // 非法状态不成交
        uint64_t new_filled = e.filled + filled_qty;
        if (new_filled > e.order.quantity) {
            filled_qty = e.order.quantity - e.filled;   // 钳制不过量
            new_filled = e.order.quantity;
        }
        // 加权均价: (旧均价×旧量 + 新价×新量) / 总量
        int64_t total_cost = e.avg_fill_price * (int64_t)e.filled
                           + fill_price * (int64_t)filled_qty;
        e.filled = new_filled;
        e.remaining = e.order.quantity - new_filled;
        e.avg_fill_price = new_filled ? total_cost / (int64_t)new_filled : 0;
        e.update_time = now();
        if (new_filled >= e.order.quantity) {
            e.status = OrderStatus::FILLED;   // 终态: 从活态索引移除
            sym_orders_[e.order.symbol_id].erase(order_id);
            strat_orders_[e.order.strategy_id].erase(order_id);
        } else {
            e.status = OrderStatus::PARTIAL_FILL;
        }
    }

    // 撤单请求(OUCH 'X' 发出前): 仅活态(SUBMITTED/PARTIAL_FILL)可撤。
    // 置 PENDING_CANCEL(等 'C' 回报)。返回 true 成功。
    bool request_cancel(uint64_t order_id) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return false;
        Entry& e = it->second;
        if (e.status != OrderStatus::SUBMITTED &&
            e.status != OrderStatus::PARTIAL_FILL) return false;   // 终态/在途不可撤
        e.status = OrderStatus::PENDING_CANCEL;
        e.update_time = now();
        return true;
    }

    // 撤单回报(OUCH 'C'): 仅 PENDING_CANCEL → CANCELLED(终态)。
    bool on_cancel(uint64_t order_id) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return false;
        Entry& e = it->second;
        if (e.status != OrderStatus::PENDING_CANCEL) return false;   // 非法流转拒收
        e.status = OrderStatus::CANCELLED;
        e.update_time = now();
        sym_orders_[e.order.symbol_id].erase(order_id);
        strat_orders_[e.order.strategy_id].erase(order_id);
        return true;
    }

    // 标记拒绝(OUCH 'J' / 风控拦截 / 发送失败): PENDING → REJECTED。
    void on_reject(uint64_t order_id) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return;
        Entry& e = it->second;
        if (e.status != OrderStatus::PENDING) return;   // 仅待发送可拒
        e.status = OrderStatus::REJECTED;
        e.update_time = now();
        sym_orders_[e.order.symbol_id].erase(order_id);
        strat_orders_[e.order.strategy_id].erase(order_id);
    }

    // ── 查询 ──

    // 状态查询。订单不存在返回 NOT_FOUND(区别于 REJECTED)。
    OrderStatus status(uint64_t order_id) const {
        auto it = orders_.find(order_id);
        return it == orders_.end() ? OrderStatus::NOT_FOUND : it->second.status;
    }

    // 查询订单详情(不存在返回 nullptr)
    const Order* order(uint64_t order_id) const {
        auto it = orders_.find(order_id);
        return it == orders_.end() ? nullptr : &it->second.order;
    }

    // 查询订单条目详情(含状态/成交量/均价, 不存在返回 nullptr)
    const Entry* entry(uint64_t order_id) const {
        auto it = orders_.find(order_id);
        return it == orders_.end() ? nullptr : &it->second;
    }

    // 某 symbol 的活态(未终态)订单 id 列表。
    std::vector<uint64_t> open_orders(uint64_t symbol_id) const {
        std::vector<uint64_t> out;
        auto it = sym_orders_.find(symbol_id);
        if (it == sym_orders_.end()) return out;
        for (uint64_t id : it->second) out.push_back(id);
        return out;
    }

    // 某 strategy 的活态订单 id 列表。
    std::vector<uint64_t> open_orders_by_strategy(uint64_t strategy_id) const {
        std::vector<uint64_t> out;
        auto it = strat_orders_.find(strategy_id);
        if (it == strat_orders_.end()) return out;
        for (uint64_t id : it->second) out.push_back(id);
        return out;
    }

    // 全量迭代(供对账/汇总): on_order(order_id, Entry)。
    void iterate(const std::function<void(uint64_t, const Entry&)>& fn) const {
        for (const auto& [id, e] : orders_) fn(id, e);
    }

    size_t order_count() const { return orders_.size(); }

    // 某状态订单数(验证/统计用)
    size_t count_by_status(OrderStatus st) const {
        size_t n = 0;
        for (const auto& [id, e] : orders_)
            if (e.status == st) ++n;
        return n;
    }

private:
    static uint64_t now() {
        // 下单/回报时序用简单递增计数器(无系统时钟依赖, 测试可复现)
        static uint64_t counter = 0;
        return ++counter;
    }

    std::unordered_map<uint64_t, Entry> orders_;
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> sym_orders_;     // symbol → 活态订单
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> strat_orders_;   // strategy → 活态订单
    uint64_t next_id_ = 1;
};
