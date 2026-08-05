#include "market/parser/itch_parser.h"

#include <cstring>

namespace {
// 大端序读取辅助
inline uint16_t rd_u16(const uint8_t* p) {
    return static_cast<uint16_t>((p[0] << 8) | p[1]);
}
inline uint32_t rd_u32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16)
         | (static_cast<uint32_t>(p[2]) << 8)  |  static_cast<uint32_t>(p[3]);
}
inline uint64_t rd_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
    return v;
}
inline uint64_t rd_ts(const uint8_t* p) {  // 6 字节纳秒时间戳
    uint64_t v = 0;
    for (int i = 0; i < 6; ++i) v = (v << 8) | p[i];
    return v;
}
}  // namespace

bool ItchParser::feed(const uint8_t* msg, size_t len) {
    if (len < 3) return false;  // 至少 type(1) + 少量字段
    msg_count_.fetch_add(1, std::memory_order_relaxed);
    // 注：不在此重置 cur_seq_。带 seq 版本(feed(msg,len,seq))先设 cur_seq_ 再调本函数,
    // 若这里重置为 0 会覆盖 seq, 导致 emit 的 seq_id 恒为 0。
    const uint8_t mt = msg[0];
    switch (mt) {
        case 'A': case 'F': parse_A(msg, len); return true;
        case 'D': parse_D(msg, len); return true;
        case 'X': parse_X(msg, len); return true;
        case 'U': parse_U(msg, len); return true;
        case 'P': parse_P(msg, len); return true;
        case 'E': parse_E(msg, len); return true;
        case 'C': parse_C(msg, len); return true;
        default:  return false;  // 其他类型（R/S/H/Y/L/V 等）不处理
    }
}

bool ItchParser::feed(const uint8_t* msg, size_t len, uint64_t seq) {
    cur_seq_ = seq;
    return feed(msg, len);
}

void ItchParser::parse_A(const uint8_t* m, size_t len) {
    if (len < 36) return;
    uint64_t locate  = rd_u16(m + 1);
    uint64_t oref    = rd_u64(m + 11);
    OrderSide side   = (m[19] == 'S') ? OrderSide::SELL : OrderSide::BUY;
    uint32_t shares  = rd_u32(m + 20);
    int64_t  price   = itch_to_cents(rd_u32(m + 32));

    MarketEvent ev{};
    ev.type            = MarketEvent::Type::ADD;
    ev.locate          = locate;
    ev.timestamp       = rd_ts(m + 5);
    ev.order.side      = side;
    ev.order.price     = price;
    ev.order.shares    = shares;
    ev.order.order_ref = oref;
    emit(ev);
}

void ItchParser::parse_D(const uint8_t* m, size_t len) {
    if (len < 19) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);

    MarketEvent ev{};
    ev.type            = MarketEvent::Type::DELETE;
    ev.locate          = locate;
    ev.timestamp       = rd_ts(m + 5);
    ev.order.order_ref = oref;
    emit(ev);
}

void ItchParser::parse_X(const uint8_t* m, size_t len) {
    if (len < 23) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);
    uint32_t cshares = rd_u32(m + 19);

    MarketEvent ev{};
    ev.type            = MarketEvent::Type::CANCEL;
    ev.locate          = locate;
    ev.timestamp       = rd_ts(m + 5);
    ev.order.order_ref = oref;
    ev.order.shares    = cshares;
    emit(ev);
}

void ItchParser::parse_U(const uint8_t* m, size_t len) {
    if (len < 35) return;
    uint64_t locate  = rd_u16(m + 1);
    uint64_t oldref  = rd_u64(m + 11);
    uint64_t newref  = rd_u64(m + 19);
    uint32_t shares  = rd_u32(m + 27);
    int64_t  price   = itch_to_cents(rd_u32(m + 31));

    MarketEvent ev{};
    ev.type                = MarketEvent::Type::REPLACE;
    ev.locate              = locate;
    ev.timestamp           = rd_ts(m + 5);
    ev.order.order_ref     = oldref;
    ev.order.new_order_ref = newref;
    ev.order.price         = price;
    ev.order.shares        = shares;
    emit(ev);
}

void ItchParser::parse_P(const uint8_t* m, size_t len) {
    if (len < 44) return;
    uint64_t locate  = rd_u16(m + 1);
    uint64_t oref    = rd_u64(m + 11);
    uint32_t shares  = rd_u32(m + 20);
    int64_t  price   = itch_to_cents(rd_u32(m + 32));

    MarketEvent ev{};
    ev.type           = MarketEvent::Type::TRADE;
    ev.locate         = locate;
    ev.timestamp      = rd_ts(m + 5);
    ev.trade.price    = price;
    ev.trade.volume   = shares;
    ev.trade.side     = (m[19] == 'S') ? OrderSide::SELL : OrderSide::BUY;
    ev.trade.order_ref = oref;
    emit(ev);
}

void ItchParser::parse_E(const uint8_t* m, size_t len) {
    if (len < 31) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);
    uint32_t shares = rd_u32(m + 19);

    MarketEvent ev{};
    ev.type           = MarketEvent::Type::EXECUTE;
    ev.locate         = locate;
    ev.timestamp      = rd_ts(m + 5);
    ev.trade.price    = -1;  // E 不带价格，由订单簿消费者查簿补全
    ev.trade.volume   = shares;
    ev.trade.side     = OrderSide::NONE;  // E 无方向
    ev.trade.order_ref = oref;
    emit(ev);
}

void ItchParser::parse_C(const uint8_t* m, size_t len) {
    if (len < 35) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);
    uint32_t shares = rd_u32(m + 19);
    int64_t  price  = itch_to_cents(rd_u32(m + 27));

    MarketEvent ev{};
    ev.type           = MarketEvent::Type::TRADE;
    ev.locate         = locate;
    ev.timestamp      = rd_ts(m + 5);
    ev.trade.price    = price;
    ev.trade.volume   = shares;
    ev.trade.side     = OrderSide::NONE;
    ev.trade.order_ref = oref;
    emit(ev);
}
