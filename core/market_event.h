#pragma once

#include "core/types.h"

#include <cstdint>
#include <string>

// ── 协议无关的市场事件 ──
// 解析器的产出、订单簿消费者的输入。与具体协议（ITCH/L2）无关。
// 解析器负责把各协议的二进制转成统一事件，订单簿只认事件、不认协议。
// 放 core 层：队列（core）也要用它做槽位类型，避免 core→market 反向依赖。
//
// 边界（见 docs/MARKET_DATA_DECISION.md §2.5）：
//   解析器 = 纯转换（字节 → 事件），不碰订单簿、不填价格。
//   订单簿消费者 = 消费事件，重建盘口，E 事件查价由它做。
//
// 为什么 E（部分成交）不带价格：
//   ITCH 的 E 只带 orderref + shares，价格在挂单里（A/U）。
//   解析器不知道价格，订单簿消费者查自己的簿才知道。所以事件里
//   price 是可选（-1 表示未知），由消费者补全。
struct MarketEvent {
    // 事件类型（协议无关）
    enum class Type : uint8_t {
        ADD,        // 新挂单（A/F）
        DELETE,     // 整笔撤单（D）
        CANCEL,     // 部分撤单（X）
        REPLACE,    // 改单（U）
        TRADE,      // 成交（P/C，带价格）
        EXECUTE,    // 部分成交（E，不带价格）
        STOCK_DIR   // 股票目录（R），建立 locate→symbol
    };

    Type   type;
    uint64_t seq_id;        // 消息序号（recv 从 MoldUDP64 包头 seq + 包内偏移推算，全局连续）
    uint64_t locate;        // Stock Locate（协议引用股票的 key）
    uint64_t order_ref;     // 挂单引用号（X/D/E/P 定位用）
    uint64_t new_order_ref; // REPLACE 的新 ref（其他为 0）
    OrderSide side;         // ADD 的方向（其他忽略）
    int64_t  price;         // 价格（分，定点整数）；E 为 -1 未知
    uint64_t shares;        // ADD/REPLACE 的总量 / CANCEL 的撤量 / P/E 的成交量
    uint64_t timestamp;     // 时间戳（纳秒，协议解析）
    std::string symbol;     // STOCK_DIR 用：locate 对应 symbol
};
