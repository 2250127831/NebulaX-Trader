#pragma once

#include "core/types.h"
#include <cstddef>
#include <cstdint>

// ── 订单协议编解码接口 ──
// 抽象"内部 Order ↔ 协议字节"边界。业务代码(OMS/Risk/ExecutionEngine)只消费内部
// Order 结构, 不碰协议字节; 换协议(自定义 'O'/'F' → OUCH 4.2 → FIX/国内私有)只换
// IOrderCodec 实现, 业务层零改动。
//
// 实现:
//   CustomOrderCodec  : 现有 'O'/'F' 自定义协议(迁移自 order_protocol.h), benchmark 模拟交易所兼容
//   OuchOrderCodec    : OUCH 4.2(NASDAQ 订单协议, 与 ITCH 行情配套), 后续增量
//
// 线程安全: codec 只做无状态编解码(不持有可变成员), 可跨线程共用同一实例。
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

    // 成交回报序列化(交易所 → 交易系统)。
    virtual bool encode_fill(uint64_t order_id, uint64_t filled_qty, int64_t fill_price,
                             uint8_t* buf, size_t cap, size_t& out_len) const = 0;
    // 成交回报反序列化。成功填 order_id/filled_qty/fill_price, 返回 true。
    virtual bool decode_fill(const uint8_t* buf, size_t len,
                             uint64_t& order_id, uint64_t& filled_qty,
                             int64_t& fill_price) const = 0;
};
