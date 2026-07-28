#pragma once

#include "core/types.h"
#include <cstdint>

// 风险管理器：下单前风控校验
class RiskManager {
public:
    bool check_order(const Order& order);
    void on_fill(const Order& order);
    // 配置
    void set_max_position(uint64_t max_pos);
    void set_max_daily_loss(double max_loss);
private:
    uint64_t max_position_ = 10000;
    double   max_daily_loss_ = 100000.0;
    uint64_t current_position_ = 0;
    double   daily_pnl_ = 0.0;
};
