#pragma once

#include "oms/order.h"
#include <unordered_map>

// 订单管理器：维护订单生命周期
class OrderManager {
public:
    uint64_t new_order(const Order& order);
    void on_fill(uint64_t order_id, uint64_t filled_qty);
    void on_cancel(uint64_t order_id);
    OrderStatus status(uint64_t order_id) const;
private:
    std::unordered_map<uint64_t, OrderStatus> orders_;
    uint64_t next_id_ = 1;
};
