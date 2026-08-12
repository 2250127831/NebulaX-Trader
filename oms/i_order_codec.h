#pragma once

#include "core/types.h"
#include <cstddef>
#include <cstdint>

// ── 订单回报结构 ──
// 协议无关的回报中间表示。业务代码(fill_th/ExecutionEngine)只消费此结构。
// type 表达回报类型: 'F' 自定义协议全额成交; OUCH 用 'A' Accepted / 'E' Executed /
// 'C' Canceled / 'J' Rejected。
struct Fill {
    uint8_t  type = 'F';        // 回报类型(A/E/C/J 或自定义 'F')
    uint64_t order_id = 0;      // 内部订单 id(经 ref/token 映射回)
    uint64_t exchange_ref = 0;  // 交易所分配的 Order Reference Number('A' 带回, 0=未接受)
    uint64_t filled_qty = 0;    // 该笔成交量
    int64_t  fill_price = 0;    // 成交价(定点整数, 分)
};

// ── 订单协议编解码接口 ──
// 抽象"内部 Order/Fill ↔ 协议字节"边界。业务代码(OMS/Risk/ExecutionEngine)只消费内部
// Order/Fill 结构, 不碰协议字节; 换协议(自定义 'O'/'F' → OUCH 4.2 → FIX/国内私有)只换
// IOrderCodec 实现, 业务层零改动。
//
// 实现:
//   CustomOrderCodec  : 现有 'O'/'F' 自定义协议(迁移自 order_protocol.h), benchmark 模拟交易所兼容
//   OuchOrderCodec    : OUCH 4.2(NASDAQ 订单协议, 与 ITCH 行情配套)
//
// 线程安全: codec 需跨线程(encode 在 worker 锁内 / decode 在 fill_th)。
// CustomOrderCodec 无状态可安全共享; OuchOrderCodec 持 token↔order_id 映射表(内部互斥锁保护)。
class IOrderCodec {
public:
    virtual ~IOrderCodec() = default;

    // 订单帧长 / 回报帧长(字节)
    virtual size_t order_msg_len() const = 0;
    virtual size_t fill_msg_len() const = 0;

    // 订单序列化。成功写 out_len 字节, 返回 true; 容量不足/参数非法返回 false。
    virtual bool encode_order(const Order& o, uint8_t* buf, size_t cap,
                              size_t& out_len) const = 0;
    // 订单反序列化。校验 magic 与长度。成功填 out, 返回 true。
    virtual bool decode_order(const uint8_t* buf, size_t len, Order& out) const = 0;

    // 成交回报序列化(交易所 → 交易系统)。成功写 out_len 字节, 返回 true。
    virtual bool encode_fill(uint64_t order_id, uint64_t filled_qty, int64_t fill_price,
                             uint8_t* buf, size_t cap, size_t& out_len) const = 0;
    // 成交回报反序列化。校验 magic 与长度。成功填 out(Fill 含 type/order_id/qty/price)。
    virtual bool decode_fill(const uint8_t* buf, size_t len, Fill& out) const = 0;

    // 撤单请求序列化(交易系统 → 交易所)。成功写 out_len 字节, 返回 true。
    virtual bool encode_cancel_request(uint64_t order_id, uint8_t* buf, size_t cap,
                                       size_t& out_len) const = 0;
};
