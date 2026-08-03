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
    ++msg_count_;
    cur_seq_ = 0;  // 无 seq 版本：seq 默认 0
    const uint8_t mt = msg[0];
    switch (mt) {
        case 'R': parse_R(msg, len); return true;
        case 'A': case 'F': parse_A(msg, len); return true;
        case 'D': parse_D(msg, len); return true;
        case 'X': parse_X(msg, len); return true;
        case 'U': parse_U(msg, len); return true;
        case 'P': parse_P(msg, len); return true;
        case 'E': parse_E(msg, len); return true;
        case 'C': parse_C(msg, len); return true;
        default:  return false;  // 其他类型（S/H/Y/L/V 等）不处理
    }
}

bool ItchParser::feed(const uint8_t* msg, size_t len, uint16_t seq) {
    cur_seq_ = seq;
    return feed(msg, len);
}

void ItchParser::parse_R(const uint8_t* m, size_t len) {
    if (len < 19) return;
    uint64_t locate = rd_u16(m + 1);
    std::string stock(reinterpret_cast<const char*>(m + 11), 8);
    stock.resize(stock.find_last_not_of(' ') + 1);
    if (stock.empty()) return;
    symbols_[locate] = stock;

    MarketEvent ev{};
    ev.type   = MarketEvent::Type::STOCK_DIR;
    ev.locate = locate;
    ev.symbol = stock;
    emit(ev);
}

void ItchParser::parse_A(const uint8_t* m, size_t len) {
    if (len < 36) return;
    uint64_t locate  = rd_u16(m + 1);
    uint64_t oref    = rd_u64(m + 11);
    OrderSide side   = (m[19] == 'S') ? OrderSide::SELL : OrderSide::BUY;
    uint32_t shares  = rd_u32(m + 20);
    int64_t  price   = itch_to_cents(rd_u32(m + 32));

    MarketEvent ev{};
    ev.type       = MarketEvent::Type::ADD;
    ev.locate     = locate;
    ev.order_ref  = oref;
    ev.side       = side;
    ev.price      = price;
    ev.shares     = shares;
    ev.timestamp  = rd_ts(m + 5);
    emit(ev);
}

void ItchParser::parse_D(const uint8_t* m, size_t len) {
    if (len < 19) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);

    MarketEvent ev{};
    ev.type      = MarketEvent::Type::DELETE;
    ev.locate    = locate;
    ev.order_ref = oref;
    ev.timestamp = rd_ts(m + 5);
    emit(ev);
}

void ItchParser::parse_X(const uint8_t* m, size_t len) {
    if (len < 23) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);
    uint32_t cshares = rd_u32(m + 19);

    MarketEvent ev{};
    ev.type      = MarketEvent::Type::CANCEL;
    ev.locate    = locate;
    ev.order_ref = oref;
    ev.shares    = cshares;
    ev.timestamp = rd_ts(m + 5);
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
    ev.type         = MarketEvent::Type::REPLACE;
    ev.locate       = locate;
    ev.order_ref    = oldref;
    ev.new_order_ref = newref;
    ev.price        = price;
    ev.shares       = shares;
    ev.timestamp    = rd_ts(m + 5);
    emit(ev);
}

void ItchParser::parse_P(const uint8_t* m, size_t len) {
    if (len < 44) return;
    uint64_t locate  = rd_u16(m + 1);
    uint64_t oref    = rd_u64(m + 11);
    uint32_t shares  = rd_u32(m + 20);
    int64_t  price   = itch_to_cents(rd_u32(m + 32));

    MarketEvent ev{};
    ev.type      = MarketEvent::Type::TRADE;
    ev.locate    = locate;
    ev.order_ref = oref;
    ev.price     = price;
    ev.shares    = shares;
    ev.timestamp = rd_ts(m + 5);
    emit(ev);
}

void ItchParser::parse_E(const uint8_t* m, size_t len) {
    if (len < 31) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);
    uint32_t shares = rd_u32(m + 19);

    MarketEvent ev{};
    ev.type      = MarketEvent::Type::EXECUTE;
    ev.locate    = locate;
    ev.order_ref = oref;
    ev.shares    = shares;
    ev.price     = -1;  // E 不带价格，由订单簿消费者查簿补全
    ev.timestamp = rd_ts(m + 5);
    emit(ev);
}

void ItchParser::parse_C(const uint8_t* m, size_t len) {
    if (len < 35) return;
    uint64_t locate = rd_u16(m + 1);
    uint64_t oref   = rd_u64(m + 11);
    uint32_t shares = rd_u32(m + 19);
    int64_t  price  = itch_to_cents(rd_u32(m + 27));

    MarketEvent ev{};
    ev.type      = MarketEvent::Type::TRADE;
    ev.locate    = locate;
    ev.order_ref = oref;
    ev.price     = price;
    ev.shares    = shares;
    ev.timestamp = rd_ts(m + 5);
    emit(ev);
}
