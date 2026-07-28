#pragma once

#include <cstddef>
#include <cstdint>

// ── Tick 数据结构 ──
// 定长设计，避免变长消息带来的内存管理开销
struct Tick {
    uint64_t timestamp;       // 时间戳（纳秒）
    uint64_t seq_id;          // 行情序列号
    uint64_t symbol_id;       // 合约/股票 ID
    double    last_price;     // 最新成交价
    uint64_t volume;          // 成交量
    double    bid_price;      // 买一价
    double    ask_price;      // 卖一价
    uint64_t bid_volume;      // 买一量
    uint64_t ask_volume;      // 卖一量
};

// ── Order 数据结构 ──
enum class OrderSide : uint8_t { BUY = 0, SELL = 1 };
enum class OrderType : uint8_t { MARKET = 0, LIMIT = 1, ICEBERG = 2 };

struct Order {
    uint64_t order_id;        // 订单 ID
    uint64_t strategy_id;     // 策略 ID
    uint64_t symbol_id;       // 合约 ID
    OrderSide  side;          // 买卖方向
    OrderType  type;          // 订单类型
    double     price;         // 价格（市价单填 0）
    uint64_t   quantity;      // 数量
    uint64_t   timestamp;     // 下单时间戳
};
