#pragma once

#include "core/types.h"

// 订单状态枚举
enum class OrderStatus : uint8_t {
    PENDING,         // 待发送
    SUBMITTED,       // 已提交(交易所接受, 活态)
    PARTIAL_FILL,    // 部分成交(活态)
    PENDING_CANCEL,  // 撤单请求在途(等 'C' 回报)
    FILLED,          // 全部成交(终态)
    CANCELLED,       // 已撤销(终态)
    REJECTED,        // 已拒绝(终态)
    NOT_FOUND        // 订单不存在(查询用, 非真实状态)
};
