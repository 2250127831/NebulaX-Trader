#pragma once

#include "oms/i_order_codec.h"
#include <cstring>
#include <endian.h>

// ── 自定义 'O'/'F' 协议 codec（V5 解耦后的默认实现）──
// 迁移自 order_protocol.h 的编解码体，行为逐字节不变（benchmark 模拟交易所兼容）。
// 定长、big-endian，与 ITCH 行情一致(大端)。
//
//   新订单   'O'  交易系统 → 交易所   42 字节
//   成交回报 'F'  交易所 → 交易系统   25 字节
//
// 布局：
//   'O' [1]type [8]order_id [8]symbol_id [1]side [8]price [8]quantity [8]timestamp
//   'F' [1]type [8]order_id [8]filled_qty [8]fill_price
// side: 0=BUY 1=SELL。price/fill_price 为定点整数(分)。全部字段大端。
class CustomOrderCodec : public IOrderCodec {
public:
    static constexpr uint8_t kMsgOrder = 'O';
    static constexpr uint8_t kMsgFill  = 'F';
    static constexpr size_t  kOrderMsgLen = 42;
    static constexpr size_t  kFillMsgLen  = 25;

    size_t order_msg_len() const override { return kOrderMsgLen; }
    size_t fill_msg_len() const override { return kFillMsgLen; }

    bool encode_order(const Order& o, uint8_t* buf, size_t cap, size_t& out_len) const override {
        if (!buf || cap < kOrderMsgLen) return false;
        buf[0] = kMsgOrder;
        uint64_t v = htobe64(o.order_id);   memcpy(buf + 1,  &v, 8);
        v = htobe64(o.symbol_id);            memcpy(buf + 9,  &v, 8);
        buf[17] = (o.side == OrderSide::SELL) ? 1 : 0;
        v = htobe64((uint64_t)o.price);      memcpy(buf + 18, &v, 8);
        v = htobe64(o.quantity);             memcpy(buf + 26, &v, 8);
        v = htobe64(o.timestamp);            memcpy(buf + 34, &v, 8);
        out_len = kOrderMsgLen;
        return true;
    }

    bool decode_order(const uint8_t* buf, size_t len, Order& out) const override {
        if (len < kOrderMsgLen || buf[0] != kMsgOrder) return false;
        out = Order{};
        uint64_t v;
        memcpy(&v, buf + 1, 8);  out.order_id  = be64toh(v);
        memcpy(&v, buf + 9, 8);  out.symbol_id = be64toh(v);
        out.side = (buf[17] == 1) ? OrderSide::SELL : OrderSide::BUY;
        memcpy(&v, buf + 18, 8); out.price     = (int64_t)be64toh(v);
        memcpy(&v, buf + 26, 8); out.quantity  = be64toh(v);
        memcpy(&v, buf + 34, 8); out.timestamp = be64toh(v);
        out.type = OrderType::MARKET;   // type 字段协议中没有, 统一填 MARKET
        return true;
    }

    bool encode_fill(uint64_t order_id, uint64_t filled_qty, int64_t fill_price,
                     uint8_t* buf, size_t cap, size_t& out_len) const override {
        if (!buf || cap < kFillMsgLen) return false;
        buf[0] = kMsgFill;
        uint64_t v = htobe64(order_id);     memcpy(buf + 1,  &v, 8);
        v = htobe64(filled_qty);            memcpy(buf + 9,  &v, 8);
        v = htobe64((uint64_t)fill_price);  memcpy(buf + 17, &v, 8);
        out_len = kFillMsgLen;
        return true;
    }

    bool decode_fill(const uint8_t* buf, size_t len,
                     uint64_t& order_id, uint64_t& filled_qty,
                     int64_t& fill_price) const override {
        if (len < kFillMsgLen || buf[0] != kMsgFill) return false;
        uint64_t v;
        memcpy(&v, buf + 1, 8);  order_id   = be64toh(v);
        memcpy(&v, buf + 9, 8);  filled_qty = be64toh(v);
        memcpy(&v, buf + 17, 8); fill_price = (int64_t)be64toh(v);
        return true;
    }
};
