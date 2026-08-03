#pragma once

#include <cstddef>
#include <cstdint>

// ── 价格表示约定 ──
// 价格一律用定点整数，不用浮点：
//   - 线上行情（ITCH 等）价格本来就是整数，交易所撮合全程整数
//   - 浮点在展示层才有意义，内部计算/比较/撮合永不碰 double
// 精度：
//   - Tick 用「分」（×100）：12345 = 123.45 元（A 股最小报价单位）
//   - Order 同样用定点整数，精度由接入的市场决定（A 股 ×100 / 美股 ×10000）
// 换算只发生在协议边界和展示层，见 price_to_double()。

// ── Tick 数据结构 ──
// 定长设计，避免变长消息带来的内存管理开销
struct Tick {
    static constexpr int64_t kTickSize = 100;  // 价格精度：1 元 = 100（分）

    uint64_t timestamp;       // 时间戳（纳秒）
    uint64_t seq_id;          // 行情序列号
    uint64_t symbol_id;       // 合约/股票 ID
    int64_t   last_price;     // 最新成交价（分，定点整数）
    uint64_t volume;          // 成交量
    int64_t   bid_price;      // 买一价（分，定点整数）
    int64_t   ask_price;      // 卖一价（分，定点整数）
    uint64_t bid_volume;      // 买一量
    uint64_t ask_volume;      // 卖一量
};

// ── Order 数据结构 ──
enum class OrderSide : uint8_t { BUY = 0, SELL = 1, NONE = 2 };  // NONE = 观望（无方向信号）
enum class OrderType : uint8_t { MARKET = 0, LIMIT = 1, ICEBERG = 2 };

struct Order {
    uint64_t order_id;        // 订单 ID
    uint64_t strategy_id;     // 策略 ID
    uint64_t symbol_id;       // 合约 ID
    OrderSide  side;          // 买卖方向
    OrderType  type;          // 订单类型
    int64_t    price;         // 价格（定点整数，市价单填 0）
    uint64_t   quantity;      // 数量
    uint64_t   timestamp;     // 下单时间戳
};

// ── 订单簿挂单节点 ──
// 高性能订单簿的池化存储单元（迁移自 NebulaX matching Order）。
// 与下单 Order 语义不同：这是行情层盘口的挂单，由 OrderPool 池化管理、
// 按价格档 intrusive 链表组织，零堆分配。
// 64 字节对齐，整节点一个 cache line（同 NebulaX static_assert）。
struct OrderSlot {
    uint64_t order_ref = 0;       // 挂单引用号（ITCH order_ref / 撮合单号）
    OrderSide  side     = OrderSide::NONE;
    int64_t    price    = 0;      // 挂单价（定点整数，分）
    uint64_t   shares   = 0;      // 原始挂单量
    uint64_t   remaining = 0;     // 剩余未成交量（撤/成交后递减）
    uint64_t   sequence = 0;      // 时间优先（FIFO 顺序，同 NebulaX）

    // ── intrusive linked list (pool-managed) ──
    uint32_t prev_idx = UINT32_MAX;   // 同价档前一个挂单
    uint32_t next_idx = UINT32_MAX;   // 同价档后一个挂单
    uint32_t pool_next_free = UINT32_MAX;  // 池空闲链表链接（仅释放时有效）
};
static_assert(sizeof(OrderSlot) == 64, "OrderSlot must be 64 bytes for cache line alignment");

// 定点价格 → double（仅展示层用，业务计算不要用浮点）
inline double price_to_double(int64_t price) {
    return static_cast<double>(price) / static_cast<double>(Tick::kTickSize);
}
