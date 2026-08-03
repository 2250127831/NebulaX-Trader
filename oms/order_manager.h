#pragma once

#include "oms/order.h"
#include <cstdint>
#include <unordered_map>

// ── 订单管理器 ──
// 维护订单生命周期状态机，持有订单全部信息(不只状态)供执行/风控/查询回查。
//   新单 → PENDING → (模拟/真实发送) → SUBMITTED
//        → 全部成交 → FILLED
//        → 部分成交 → PARTIAL_FILL
//        → 撤销 → CANCELLED
//        → 风控拒绝/发送失败 → REJECTED
class OrderManager {
public:
    // 登记新单：分配 order_id 并写回 order.order_id，状态 PENDING，返回 id。
    uint64_t new_order(Order& order) {
        order.order_id = next_id_++;
        orders_[order.order_id] = Entry{order, OrderStatus::PENDING, 0};
        return order.order_id;
    }

    // 成交回报：累计已成交数量，按是否全部成交切换状态。
    void on_fill(uint64_t order_id, uint64_t filled_qty) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return;
        it->second.filled += filled_qty;
        if (it->second.filled >= it->second.order.quantity) {
            it->second.status = OrderStatus::FILLED;
        } else if (it->second.status != OrderStatus::CANCELLED &&
                   it->second.status != OrderStatus::REJECTED) {
            it->second.status = OrderStatus::PARTIAL_FILL;
        }
    }

    // 撤销：仅未成交/部分成交可撤，已成交/已拒绝不可撤。
    bool on_cancel(uint64_t order_id) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return false;
        if (it->second.status == OrderStatus::FILLED ||
            it->second.status == OrderStatus::REJECTED) return false;
        it->second.status = OrderStatus::CANCELLED;
        return true;
    }

    // 标记拒绝(风控拦截/发送失败)
    void on_reject(uint64_t order_id) {
        auto it = orders_.find(order_id);
        if (it == orders_.end()) return;
        it->second.status = OrderStatus::REJECTED;
    }

    OrderStatus status(uint64_t order_id) const {
        auto it = orders_.find(order_id);
        return it == orders_.end() ? OrderStatus::REJECTED : it->second.status;
    }

    // 查询订单详情(不存在返回 nullptr)
    const Order* order(uint64_t order_id) const {
        auto it = orders_.find(order_id);
        return it == orders_.end() ? nullptr : &it->second.order;
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
    struct Entry {
        Order order;
        OrderStatus status;
        uint64_t filled;
    };
    std::unordered_map<uint64_t, Entry> orders_;
    uint64_t next_id_ = 1;
};
