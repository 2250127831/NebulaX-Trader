#pragma once

#include "core/types.h"
#include "oms/order_manager.h"
#include "risk/risk_manager.h"
#include <vector>

// 执行引擎：收集策略订单，风控检查后发送到交易所
class ExecutionEngine {
public:
    ExecutionEngine(OrderManager& om, RiskManager& rm)
        : order_manager_(om), risk_manager_(rm) {}

    void submit_order(const Order& order);
    void on_fill(uint64_t order_id, uint64_t filled_qty);
    void on_cancel(uint64_t order_id);

private:
    OrderManager& order_manager_;
    RiskManager&  risk_manager_;
};
