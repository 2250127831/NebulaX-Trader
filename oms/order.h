#pragma once

#include "core/types.h"

// 订单状态枚举
enum class OrderStatus : uint8_t {
    PENDING,      // 待发送
    SUBMITTED,    // 已提交
    PARTIAL_FILL, // 部分成交
    FILLED,       // 全部成交
    CANCELLED,    // 已撤销
    REJECTED      // 已拒绝
};
