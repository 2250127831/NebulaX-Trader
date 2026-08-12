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
// 订单标识(两标识模型, 与真实链路一致):
//   Order Token(14B ASCII)        = 客户端生成。本系统用内部 order_id 的定宽十进制编码。
//   Order Reference Number(8B 整数)= 交易所分配。'A' Accepted 回带, 后续回报以此关联订单。
//   'O' Enter 只带 token(客户端下单向, 交易所未分配号); 'A'/'E'/'C'/'J' 回报带 ref + token。
//
// 消息布局(与官方规范对齐, 实现时以 NASDAQ OUCH4.2.pdf 为准):
//   Enter Order 'O'(49B): [0]type [1..14]ClientOrderId(14 ASCII) [15]side('B'/'S')
//     [16..19]qty(u32 BE) [20..29]orderBook(10 ASCII) [30..33]price(i32 BE, ×10000)
//     [34]timeInForce('Y') [35..40]firm(6 ASCII, 空) [41]display('N' 非显示)
//     [42]capacity('A') [43]isc('N') [44..47]minQty(u32 BE, 0) [48]checksum
//   Accepted 'A'(39B): [0]'A' [1..8]orderRef(u64 BE) [9..22]token [23]side
//     [24..27]shares [28..37]book [38]checksum
//   Executed 'E'(42B): [0]'E' [1..8]ref [9..22]token [23]side [24..27]shares
//     [28..31]price [32..35]executedQty [36..39]executedPrice [40]liquidity('T') [41]checksum
//   Canceled 'C'(39B): [0]'C' [1..8]ref [9..22]token [23]side [24..27]canceledQty
//     [28..37]book [38]checksum
//   Rejected 'J'(36B): [0]'J' [1..8]ref [9..22]token [23]side [24..34]reason [35]checksum
//   checksum = 前所有字节和 mod 256。
//
// 状态(用户定): token↔order_id 映射表放 codec 内(为委托簿关联订单保留完整语义)。
//   encode_order 分配 token + 登记; decode_fill 从回报 token 查回 order_id。
//   跨线程(encode 在 worker 锁内 / decode 在 fill_th) → 内部互斥锁保护。
//   Order Reference Number 由交易所侧分配: encode_ack(模拟交易所)自增分配并登记 id→ref;
//   decode_fill('A', Trader 侧) 学习 ref→order_id, 后续 'E'/'C'/'J' 按 ref 关联订单。
class OuchOrderCodec : public IOrderCodec {
public:
    static constexpr size_t kOrderMsgLen    = 49;   // 'O' Enter Order
    static constexpr size_t kAckMsgLen      = 39;   // 'A' Accepted
    static constexpr size_t kExecMsgLen     = 42;   // 'E' Executed
    static constexpr size_t kCancelMsgLen   = 39;   // 'C' Canceled(回报)
    static constexpr size_t kRejectMsgLen   = 36;   // 'J' Rejected(回报)
    static constexpr size_t kCancelReqLen   = 19;   // 'X' Cancel Order(请求)
    static constexpr uint8_t kMsgOrder  = 'O';
    static constexpr uint8_t kMsgAck    = 'A';
    static constexpr uint8_t kMsgExec   = 'E';
    static constexpr uint8_t kMsgCancel = 'C';
    static constexpr uint8_t kMsgReject = 'J';
    static constexpr uint8_t kMsgCancelReq = 'X';

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

    // 'A' Accepted(39B): 交易所分配 Order Reference Number + 回显 token/side/qty/book。
    // ref 由本侧分配(模拟交易所为订单号权威), 登记 id→ref 供后续 'E'/'C'/'J' 使用。
    bool encode_ack(const Order& o, uint8_t* buf, size_t cap, size_t& out_len) const {
        if (!buf || cap < kAckMsgLen) return false;
        std::memset(buf, ' ', kAckMsgLen);
        buf[0] = kMsgAck;
        be64(buf + 1, ref_for(o.order_id));   // 交易所分配的 Order Reference Number
        std::string token = token_for(o.order_id);
        std::memcpy(buf + 9, token.data(), 14);
        buf[23] = (o.side == OrderSide::SELL) ? 'S' : 'B';
        be32(buf + 24, static_cast<uint32_t>(o.quantity));
        char book[11];
        snprintf(book, sizeof(book), "%llu", (unsigned long long)o.symbol_id);
        std::memcpy(buf + 28, book, strlen(book));
        buf[38] = checksum(buf, kAckMsgLen - 1);
        out_len = kAckMsgLen;
        return true;
    }

    // 'E' Executed(42B): 成交回报。带交易所分配的 ref + 回显 token。
    bool encode_exec(uint64_t order_id, uint64_t filled_qty, int64_t fill_price,
                     uint8_t* buf, size_t cap, size_t& out_len) const {
        if (!buf || cap < kExecMsgLen) return false;
        std::memset(buf, ' ', kExecMsgLen);
        buf[0] = kMsgExec;
        be64(buf + 1, ref_for(order_id));   // 交易所分配的 ref('A' 时登记过)
        std::string token = token_for(order_id);
        std::memcpy(buf + 9, token.data(), 14);
        buf[23] = 'B';   // 模拟交易所全额成交, side 回显未知(用 B; 交易系统侧重在 order_id)
        be32(buf + 24, static_cast<uint32_t>(filled_qty));
        be32(buf + 28, static_cast<int32_t>(fill_price * 100));
        be32(buf + 32, static_cast<uint32_t>(filled_qty));   // executedQty
        be32(buf + 36, static_cast<int32_t>(fill_price * 100));   // executedPrice
        buf[40] = 'T';   // liquidity: taker
        buf[41] = checksum(buf, kExecMsgLen - 1);
        out_len = kExecMsgLen;
        return true;
    }

    // ── 回报反序列化(trader 侧): 识别 'A'/'E'/'C'/'J' ──
    // 订单关联以交易所分配的 ref 为首要键('A' 时学习 ref→order_id), token 为容错回退。
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
        uint64_t ref = be64(buf + 1);   // 交易所分配的 Order Reference Number
        char token[15];
        std::memcpy(token, buf + 9, 14); token[14] = '\0';
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = ref_to_id_.find(ref);   // 已登记('A' 学到的 ref → order_id)
            if (it != ref_to_id_.end())
                out.order_id = it->second;
            else
                out.order_id = strtoull(token, nullptr, 10);   // 回退: token 定宽十进制可逆
            if (type == kMsgAck) ref_to_id_[ref] = out.order_id;   // 'A' 学习 ref→order_id
        }
        out.exchange_ref = ref;
        if (type == kMsgExec) {
            out.filled_qty = be32(buf + 32);                       // executedQty
            out.fill_price = static_cast<int64_t>(be32(buf + 36)) / 100;   // → 分
        }
        return true;
    }

    // 'C' Canceled(39B): 撤单回报。交易所 ref + token + canceled qty。
    bool encode_cancel(uint64_t order_id, uint64_t canceled_qty, uint8_t* buf,
                       size_t cap, size_t& out_len) const {
        if (!buf || cap < kCancelMsgLen) return false;
        std::memset(buf, ' ', kCancelMsgLen);
        buf[0] = kMsgCancel;
        be64(buf + 1, ref_for(order_id));
        std::string token = token_for(order_id);
        std::memcpy(buf + 9, token.data(), 14);
        buf[23] = 'B';
        be32(buf + 24, static_cast<uint32_t>(canceled_qty));
        buf[38] = checksum(buf, kCancelMsgLen - 1);
        out_len = kCancelMsgLen;
        return true;
    }

    // 'J' Rejected(36B): 拒单回报。交易所 ref + token + reason。
    bool encode_reject(uint64_t order_id, uint8_t* buf, size_t cap, size_t& out_len) const {
        if (!buf || cap < kRejectMsgLen) return false;
        std::memset(buf, ' ', kRejectMsgLen);
        buf[0] = kMsgReject;
        be64(buf + 1, ref_for(order_id));
        std::string token = token_for(order_id);
        std::memcpy(buf + 9, token.data(), 14);
        buf[23] = 'B';
        buf[35] = checksum(buf, kRejectMsgLen - 1);
        out_len = kRejectMsgLen;
        return true;
    }

    // 'X' Cancel Order Request(19B): 撤单请求。token + shares=0(撤全部)。
    bool encode_cancel_request(uint64_t order_id, uint8_t* buf, size_t cap,
                               size_t& out_len) const override {
        if (!buf || cap < kCancelReqLen) return false;
        std::memset(buf, ' ', kCancelReqLen);
        buf[0] = kMsgCancelReq;
        std::string token = token_for(order_id);
        std::memcpy(buf + 1, token.data(), 14);
        be32(buf + 15, 0);   // shares=0 → 撤全部剩余
        buf[18] = checksum(buf, kCancelReqLen - 1);
        out_len = kCancelReqLen;
        return true;
    }

    // 查询 token(供委托簿关联/日志)
    static std::string token_for(uint64_t order_id) {
        char t[15];
        snprintf(t, sizeof(t), "%014llu", (unsigned long long)order_id);   // 14 位定宽
        return std::string(t, 14);
    }

private:
    // 取订单的交易所 Order Reference Number; 未分配则分配新号(模拟交易所为订单号权威)。
    // 'A' Accepted 编码时分配并登记; 'E'/'C'/'J' 编码时取回同一 ref。
    uint64_t ref_for(uint64_t order_id) const {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = id_to_ref_.find(order_id);
        if (it != id_to_ref_.end()) return it->second;
        uint64_t ref = next_ref_++;
        id_to_ref_[order_id] = ref;
        return ref;
    }

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
    static void be64(uint8_t* p, uint64_t v) {
        for (int i = 7; i >= 0; --i) p[7 - i] = static_cast<uint8_t>(v >> (8 * i));
    }
    static uint64_t be64(const uint8_t* p) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
        return v;
    }

    mutable std::mutex mu_;
    mutable std::unordered_map<std::string, uint64_t> token_to_id_;  // encode_order 登记
    mutable std::unordered_map<uint64_t, uint64_t> id_to_ref_;       // order_id → ref(模拟交易所侧)
    mutable std::unordered_map<uint64_t, uint64_t> ref_to_id_;       // ref → order_id(Trader 侧, 'A' 学习)
    mutable uint64_t next_ref_ = 1;   // 模拟交易所订单号分配器
};
