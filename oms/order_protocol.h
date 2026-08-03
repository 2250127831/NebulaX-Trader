#pragma once

#include "core/types.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <endian.h>

// ── 订单/成交回报网络协议 ──
// 交易系统 ↔ 交易所(模拟)。定长、big-endian 编码，与 ITCH 行情一致(大端)。
//
//   新订单   'O'  交易系统 → 交易所   42 字节
//   成交回报 'F'  交易所 → 交易系统   25 字节
//
// 布局：
//   'O' [1]type [8]order_id [8]symbol_id [1]side [8]price [8]quantity [8]timestamp
//   'F' [1]type [8]order_id [8]filled_qty [8]fill_price
// side: 0=BUY 1=SELL。price/fill_price 为定点整数(分)。全部字段大端。

static constexpr uint8_t kMsgOrder = 'O';
static constexpr uint8_t kMsgFill  = 'F';
static constexpr size_t  kOrderMsgLen = 42;
static constexpr size_t  kFillMsgLen  = 25;

// 订单序列化 → buf(kOrderMsgLen 字节)。返回写入字节数。
static inline size_t encode_order(const Order& o, uint8_t* buf) {
    buf[0] = kMsgOrder;
    uint64_t v = htobe64(o.order_id);   memcpy(buf + 1,  &v, 8);
    v = htobe64(o.symbol_id);            memcpy(buf + 9,  &v, 8);
    buf[17] = (o.side == OrderSide::SELL) ? 1 : 0;
    v = htobe64((uint64_t)o.price);      memcpy(buf + 18, &v, 8);
    v = htobe64(o.quantity);             memcpy(buf + 26, &v, 8);
    v = htobe64(o.timestamp);            memcpy(buf + 34, &v, 8);
    return kOrderMsgLen;
}

// 订单反序列化。校验 type 与长度。type 字段协议中没有，统一填 MARKET。
static inline bool decode_order(const uint8_t* buf, size_t len, Order& out) {
    if (len < kOrderMsgLen || buf[0] != kMsgOrder) return false;
    out = Order{};
    uint64_t v;
    memcpy(&v, buf + 1, 8);  out.order_id  = be64toh(v);
    memcpy(&v, buf + 9, 8);  out.symbol_id = be64toh(v);
    out.side = (buf[17] == 1) ? OrderSide::SELL : OrderSide::BUY;
    memcpy(&v, buf + 18, 8); out.price     = (int64_t)be64toh(v);
    memcpy(&v, buf + 26, 8); out.quantity  = be64toh(v);
    memcpy(&v, buf + 34, 8); out.timestamp = be64toh(v);
    out.type = OrderType::MARKET;
    return true;
}

// 成交回报序列化 → buf(kFillMsgLen 字节)。返回写入字节数。
static inline size_t encode_fill(uint64_t order_id, uint64_t filled_qty,
                                 int64_t fill_price, uint8_t* buf) {
    buf[0] = kMsgFill;
    uint64_t v = htobe64(order_id);     memcpy(buf + 1,  &v, 8);
    v = htobe64(filled_qty);            memcpy(buf + 9,  &v, 8);
    v = htobe64((uint64_t)fill_price);  memcpy(buf + 17, &v, 8);
    return kFillMsgLen;
}

// 成交回报反序列化。校验 type 与长度。
static inline bool decode_fill(const uint8_t* buf, size_t len,
                               uint64_t& order_id, uint64_t& filled_qty,
                               int64_t& fill_price) {
    if (len < kFillMsgLen || buf[0] != kMsgFill) return false;
    uint64_t v;
    memcpy(&v, buf + 1, 8);  order_id   = be64toh(v);
    memcpy(&v, buf + 9, 8);  filled_qty = be64toh(v);
    memcpy(&v, buf + 17, 8); fill_price = (int64_t)be64toh(v);
    return true;
}
