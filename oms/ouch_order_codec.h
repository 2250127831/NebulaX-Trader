#pragma once

#include "oms/i_order_codec.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

// ── OUCH 4.2 订单协议 codec（V5 实盘协议化）──
// NASDAQ OUCH 4.2 订单协议, 与 ITCH 行情配套。定长大端 ASCII/整数混合。
//
// 消息布局(与官方规范对齐, 实现时以 NASDAQ OUCH4.2.pdf 为准):
//   Enter Order 'O'(49B): [0]type [1..14]ClientOrderId(14 ASCII) [15]side('B'/'S')
//     [16..19]qty(u32 BE) [20..29]orderBook(10 ASCII) [30..33]price(i32 BE, ×10000)
//     [34]timeInForce('Y') [35..40]firm(6 ASCII, 空) [41]display('N' 非显示)
//     [42]capacity('A') [43]isc('N') [44..47]minQty(u32 BE, 0) [48]checksum
//   Accepted 'A'(31B): [0]'A' [1..14]token [15]side [16..19]qty [20..29]book [30]checksum
//   Executed 'E'(34B): [0]'E' [1..14]token [15]side [16..19]qty [20..23]price
//     [24..27]executedQty [28..31]executedPrice [32]liquidity('T') [33]checksum
//   checksum = 前所有字节和 mod 256。
//
// 状态(用户定): token↔order_id 映射表放 codec 内(为委托簿关联订单保留完整语义)。
//   encode_order 分配 token + 登记; decode_fill 从回报 token 查回 order_id。
//   跨线程(encode 在 worker 锁内 / decode 在 fill_th) → 内部互斥锁保护。
class OuchOrderCodec : public IOrderCodec {
public:
    static constexpr size_t kOrderMsgLen  = 49;   // 'O' Enter Order
    static constexpr size_t kAckMsgLen    = 31;   // 'A' Accepted
    static constexpr size_t kExecMsgLen   = 34;   // 'E' Executed
    static constexpr size_t kCancelMsgLen = 30;   // 'C' Canceled
    static constexpr size_t kRejectMsgLen = 28;   // 'J' Rejected
    static constexpr uint8_t kMsgOrder  = 'O';
    static constexpr uint8_t kMsgAck    = 'A';
    static constexpr uint8_t kMsgExec   = 'E';
    static constexpr uint8_t kMsgCancel = 'C';
    static constexpr uint8_t kMsgReject = 'J';

    size_t order_msg_len() const override { return kOrderMsgLen; }
    size_t fill_msg_len() const override { return kExecMsgLen; }   // 最小回报帧(用于缓冲)

    // ── 订单 'O': 内部 Order → 49B ──
    bool encode_order(const Order& o, uint8_t* buf, size_t cap, size_t& out_len) const override {
        if (!buf || cap < kOrderMsgLen) return false;
        std::memset(buf, ' ', kOrderMsgLen);
        buf[0] = kMsgOrder;
        std::string token = token_for(o.order_id);   // 14 字符定宽
        std::memcpy(buf + 1, token.data(), 14);
        buf[15] = (o.side == OrderSide::SELL) ? 'S' : 'B';
        be32(buf + 16, static_cast<uint32_t>(o.quantity));
        // orderBook: symbol_id 数字字符串, 左对齐 10 字符(空补空格)
        char book[11];
        snprintf(book, sizeof(book), "%llu", (unsigned long long)o.symbol_id);
        std::memcpy(buf + 20, book, strlen(book));
        be32(buf + 30, static_cast<int32_t>(o.price * 100));   // 分 → OUCH ×10000
        buf[34] = 'Y';          // timeInForce: 当日
        buf[41] = 'N';          // display: 非显示(匿名单)
        buf[42] = 'A';          // capacity: 代理
        buf[43] = 'N';          // isc: 无
        be32(buf + 44, 0);      // minQty
        buf[48] = checksum(buf, kOrderMsgLen - 1);
        out_len = kOrderMsgLen;
        // 登记 token → order_id(报回收回时查表)
        std::lock_guard<std::mutex> lk(mu_);
        token_to_id_[token] = o.order_id;
        return true;
    }

    // ── 订单 'O' 反序列化(模拟交易所侧) ──
    // token 是 order_id 的 14 位定宽十进制(可逆), decode 直接解析; map 作为已登记校验。
    bool decode_order(const uint8_t* buf, size_t len, Order& out) const override {
        if (len < kOrderMsgLen || buf[0] != kMsgOrder) return false;
        if (buf[48] != checksum(buf, kOrderMsgLen - 1)) return false;   // checksum 校验
        out = Order{};
        char token[15];
        std::memcpy(token, buf + 1, 14); token[14] = '\0';
        out.order_id = strtoull(token, nullptr, 10);   // 定宽十进制可逆, 直接解析
        out.side = (buf[15] == 'S') ? OrderSide::SELL : OrderSide::BUY;
        out.quantity = be32(buf + 16);
        // orderBook: 10 字符 → symbol_id(截去尾部空格)
        char book[11];
        std::memcpy(book, buf + 20, 10); book[10] = '\0';
        out.symbol_id = strtoull(book, nullptr, 10);
        out.price = static_cast<int64_t>(be32(buf + 30)) / 100;   // OUCH ×10000 → 分
        out.type = OrderType::MARKET;
        out.timestamp = 0;
        return true;
    }

    // ── 回报编码(模拟交易所侧): 输出 'A' Accepted + 'E' Executed 两段 ──
    // 调用方(buf 足够大, cap >= kAckMsgLen + kExecMsgLen)两次调用拿两段。
    // 用 encode_ack / encode_exec 分开(encode_fill 接口单段, OUCH 用 exec 代表"成交")。
    bool encode_fill(uint64_t order_id, uint64_t filled_qty, int64_t fill_price,
                     uint8_t* buf, size_t cap, size_t& out_len) const override {
        // 沿用接口语义: 单段 'E' Executed 作为成交回报(模拟交易所先 A 后 E, A 由 encode_ack)
        return encode_exec(order_id, filled_qty, fill_price, buf, cap, out_len);
    }

    // 'A' Accepted(31B): 回显订单(token/side/qty/book), 告诉交易系统订单已被接受
    bool encode_ack(const Order& o, uint8_t* buf, size_t cap, size_t& out_len) const {
        if (!buf || cap < kAckMsgLen) return false;
        std::memset(buf, ' ', kAckMsgLen);
        buf[0] = kMsgAck;
        std::string token = token_for(o.order_id);
        std::memcpy(buf + 1, token.data(), 14);
        buf[15] = (o.side == OrderSide::SELL) ? 'S' : 'B';
        be32(buf + 16, static_cast<uint32_t>(o.quantity));
        char book[11];
        snprintf(book, sizeof(book), "%llu", (unsigned long long)o.symbol_id);
        std::memcpy(buf + 20, book, strlen(book));
        buf[30] = checksum(buf, kAckMsgLen - 1);
        out_len = kAckMsgLen;
        return true;
    }

    // 'E' Executed(34B): 成交回报
    bool encode_exec(uint64_t order_id, uint64_t filled_qty, int64_t fill_price,
                     uint8_t* buf, size_t cap, size_t& out_len) const {
        if (!buf || cap < kExecMsgLen) return false;
        std::memset(buf, ' ', kExecMsgLen);
        buf[0] = kMsgExec;
        std::string token = token_for(order_id);
        std::memcpy(buf + 1, token.data(), 14);
        buf[15] = 'B';   // 模拟交易所全额成交, side 回显未知(用 B; 交易系统侧重在 order_id)
        be32(buf + 16, static_cast<uint32_t>(filled_qty));
        be32(buf + 20, static_cast<int32_t>(fill_price * 100));
        be32(buf + 24, static_cast<uint32_t>(filled_qty));   // executedQty
        be32(buf + 28, static_cast<int32_t>(fill_price * 100));   // executedPrice
        buf[32] = 'T';   // liquidity: taker
        buf[33] = checksum(buf, kExecMsgLen - 1);
        out_len = kExecMsgLen;
        return true;
    }

    // ── 回报反序列化(trader 侧): 识别 'A'/'E'/'C'/'J', token → order_id ──
    bool decode_fill(const uint8_t* buf, size_t len, Fill& out) const override {
        if (len == 0) return false;
        uint8_t type = buf[0];
        size_t msg_len = 0;
        if (type == kMsgAck) msg_len = kAckMsgLen;
        else if (type == kMsgExec) msg_len = kExecMsgLen;
        else if (type == kMsgCancel) msg_len = kCancelMsgLen;
        else if (type == kMsgReject) msg_len = kRejectMsgLen;
        else return false;
        if (len < msg_len) return false;
        if (buf[msg_len - 1] != checksum(buf, msg_len - 1)) return false;   // checksum

        out = Fill{};
        out.type = type;
        char token[15];
        std::memcpy(token, buf + 1, 14); token[14] = '\0';
        out.order_id = strtoull(token, nullptr, 10);   // 定宽十进制可逆, 直接解析
        // map 作为已登记 token 校验(encode_order 登记); 未登记也按 token 解析(可靠)
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = token_to_id_.find(token);
            if (it != token_to_id_.end()) out.order_id = it->second;
        }
        if (type == kMsgExec) {
            out.filled_qty = be32(buf + 24);                       // executedQty
            out.fill_price = static_cast<int64_t>(be32(buf + 28)) / 100;   // → 分
        }
        return true;
    }

    // 'C' Canceled(30B): 撤单回报。token/side + canceled qty。
    bool encode_cancel(uint64_t order_id, uint64_t canceled_qty, uint8_t* buf,
                       size_t cap, size_t& out_len) const {
        if (!buf || cap < kCancelMsgLen) return false;
        std::memset(buf, ' ', kCancelMsgLen);
        buf[0] = kMsgCancel;
        std::string token = token_for(order_id);
        std::memcpy(buf + 1, token.data(), 14);
        buf[15] = 'B';
        be32(buf + 16, static_cast<uint32_t>(canceled_qty));
        buf[29] = checksum(buf, kCancelMsgLen - 1);
        out_len = kCancelMsgLen;
        return true;
    }

    // 'J' Rejected(28B): 拒单回报。token/side + reason。
    bool encode_reject(uint64_t order_id, uint8_t* buf, size_t cap, size_t& out_len) const {
        if (!buf || cap < kRejectMsgLen) return false;
        std::memset(buf, ' ', kRejectMsgLen);
        buf[0] = kMsgReject;
        std::string token = token_for(order_id);
        std::memcpy(buf + 1, token.data(), 14);
        buf[15] = 'B';
        buf[27] = checksum(buf, kRejectMsgLen - 1);
        out_len = kRejectMsgLen;
        return true;
    }

    // 查询 token(供委托簿关联/日志)
    static std::string token_for(uint64_t order_id) {
        char t[15];
        snprintf(t, sizeof(t), "%014llu", (unsigned long long)order_id);   // 14 位定宽
        return std::string(t, 14);
    }

private:
    static uint8_t checksum(const uint8_t* b, size_t n) {
        unsigned s = 0;
        for (size_t i = 0; i < n; ++i) s += b[i];
        return static_cast<uint8_t>(s & 0xFF);
    }
    static void be32(uint8_t* p, uint32_t v) {
        p[0] = static_cast<uint8_t>(v >> 24);
        p[1] = static_cast<uint8_t>(v >> 16);
        p[2] = static_cast<uint8_t>(v >> 8);
        p[3] = static_cast<uint8_t>(v & 0xFF);
    }
    static uint32_t be32(const uint8_t* p) {
        return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16)
             | (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
    }

    mutable std::mutex mu_;
    mutable std::unordered_map<std::string, uint64_t> token_to_id_;
};
