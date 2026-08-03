#pragma once

#include "core/market_event.h"
#include "core/types.h"

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

// ── ITCH 5.0 解析器（纯转换）──
// 消费原始 ITCH 字节流（NASDAQ TotalView，大端序），产出协议无关的 MarketEvent。
// 只做"字节 → 事件"，不碰订单簿、不填价格、不维护市场状态。
//
// 边界（见 docs/MARKET_DATA_DECISION.md §2.5）：
//   解析器 = 纯转换（字节 → 事件），不碰订单簿。
//   订单簿消费者 = 消费事件，重建盘口，E 事件查价由它做。
//
// 投递方式：set_sink(callback)。回调每收到一个 MarketEvent 被调用。
//   V1 单线程：回调可以是订单簿消费者的处理函数（直通）。
//   V2 并行：回调内部把事件 push 进 SPSC 队列，订单簿消费者从队列取。
//   解析器不关心下游怎么处理，只负责转换 + 投递。
//
// 字段布局见 docs/design/itch5_protocol.md。
// 价格：ITCH 是 int32 × 1/10000 美元，内部统一转 int64_t 定点（分）。
//
// 用法：
//   ItchParser parser;
//   parser.set_sink([](const MarketEvent& ev){ /* 处理 */ });
//   for each message: parser.feed(msg_bytes, msg_len);
class ItchParser {
public:
    using Sink = std::function<void(const MarketEvent&)>;

    static constexpr int64_t kTickSize = 100;          // 内部定点精度：1 元 = 100 分
    static constexpr uint64_t kUsdToCents = 100;       // 1 美元 = 100 分

    void set_sink(Sink sink) { sink_ = std::move(sink); }

    // 喂一条消息（不含长度前缀，len = 消息体长度）。返回是否成功处理。
    bool feed(const uint8_t* msg, size_t len);
    // 带 seq 版本：seq 是 recv 从 MoldUDP64 包头 + 包内偏移推算的消息序号。
    bool feed(const uint8_t* msg, size_t len, uint16_t seq);

    // locate → symbol（未建立映射返回空串）
    std::string symbol(uint64_t locate) const {
        auto it = symbols_.find(locate);
        return it == symbols_.end() ? "" : it->second;
    }

    uint64_t message_count() const { return msg_count_; }

private:
    // ITCH int32 价格 → 内部定点（分）
    static int64_t itch_to_cents(uint32_t itch_price) {
        return static_cast<int64_t>(itch_price) / kUsdToCents;
    }

    void parse_R(const uint8_t* m, size_t len);   // Stock Directory
    void parse_A(const uint8_t* m, size_t len);   // Add Order（含 F）
    void parse_D(const uint8_t* m, size_t len);   // Delete
    void parse_X(const uint8_t* m, size_t len);   // Cancel
    void parse_U(const uint8_t* m, size_t len);   // Replace
    void parse_P(const uint8_t* m, size_t len);   // Trade Non-Cross
    void parse_E(const uint8_t* m, size_t len);   // Order Executed
    void parse_C(const uint8_t* m, size_t len);   // Order Executed w/ Price

    void emit(const MarketEvent& ev) {
        MarketEvent e = ev;
        e.seq_id = cur_seq_;  // 填入当前消息的序号
        if (sink_) sink_(e);
    }

    Sink sink_;
    std::unordered_map<uint64_t, std::string> symbols_;  // locate → symbol
    uint64_t msg_count_ = 0;
    uint16_t cur_seq_ = 0;   // 当前消息序号（feed 带 seq 时设置）
};
