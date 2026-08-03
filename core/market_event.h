#pragma once

#include "core/types.h"

#include <cstdint>

// ── 协议无关的市场事件 ──
// 解析器的产出、策略/订单簿消费者的输入。与具体协议（ITCH/L2）无关。
// 放 core 层：队列（core）也要用它做槽位类型，避免 core→market 反向依赖。
//
// 边界（见 docs/MARKET_DATA_DECISION.md §2.5）：
//   解析器 = 纯转换（字节 → 事件），按消息字母构建 trade 或 order 结构体。
//   订单簿消费者 = 消费事件，重建盘口。
//
// 设计：union 内联两个结构体，type 判别当前是成交还是委托（二选一，不能共存）。
//   - trade（成交）：P/E 消息，策略消费（趋势/动量/K线）
//   - order（委托）：A/D/X/U 消息，订单簿/逐笔委托策略消费（后续通道 B）
//
// 公共字段（union 外）都是整数；无 symbol 字符串（股票代码映射靠外部配置文件，
// 不订阅 R 消息，见 docs）。R 消息由压测客户端过滤，不进主链路。
struct MarketEvent {
    // 事件类型（协议无关）
    enum class Type : uint8_t {
        ADD,        // 新挂单（A/F）
        DELETE,     // 整笔撤单（D）
        CANCEL,     // 部分撤单（X）
        REPLACE,    // 改单（U）
        TRADE,      // 成交（P/C，带价格）
        EXECUTE,    // 部分成交（E，不带价格）
    };

    Type   type;
    uint64_t seq_id;        // 消息序号（recv 从 MoldUDP64 包头 seq + 包内偏移推算，全局连续）
    uint64_t locate;        // Stock Locate（协议引用股票的 key，整数区分股票）
    uint64_t timestamp;     // 时间戳（纳秒，协议解析）

    // ── 类型专属字段（union，二选一，按 type 访问）──
    union {
        struct {            // 成交（TRADE/EXECUTE）
            OrderSide side;     // 方向（仅 P 有；E 无，未知填 NONE）
            int64_t  price;     // 成交价（分，定点整数）；E 为 -1 未知，由消费者查簿补全
            uint64_t volume;    // 成交量
            uint64_t order_ref; // 被成交的挂单引用（订单簿执行用，策略忽略）
        } trade;
        struct {            // 委托（ADD/DELETE/CANCEL/REPLACE）
            OrderSide side;         // 买卖方向（ADD 有；D/X/U 未知填 NONE）
            int64_t  price;         // 挂单价（分，定点整数）
            uint64_t shares;        // ADD/REPLACE 总量 / CANCEL 撤量
            uint64_t order_ref;     // 挂单引用号（X/D/E/P 定位用）
            uint64_t new_order_ref; // REPLACE 的新 ref（其他为 0）
        } order;
    };
};
