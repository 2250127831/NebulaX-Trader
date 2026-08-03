// ITCH 解析器 + 订单簿消费者单测：用真实样本验证
//   - 长度前缀切分、消息计数
//   - 解析器 → 消费者：R → locate→symbol 映射
//   - A/D/X/U/F → 订单簿重建（区分买卖方向、best bid/ask）
//   - P/E → 成交 Tick 生成
//
// 链路：长度前缀切分 → ItchParser(feed) → MarketEvent → OrderBookConsumer(on_event)
#include "market/parser/itch_parser.h"
#include "market/book/order_book_consumer.h"

#include <cstdio>
#include <cstdint>
#include <vector>
#include <cstdlib>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("usage: %s <itch_sample.bin>\n", argv[0]);
        return 1;
    }
    // ── 读样本 ──
    FILE* f = fopen(argv[1], "rb");
    if (!f) { printf("FAIL: 无法打开 %s\n", argv[1]); return 1; }
    fseek(f, 0, SEEK_END); long fsize = ftell(f); fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> data(fsize);
    if (fread(data.data(), 1, fsize, f) != (size_t)fsize) {
        printf("FAIL: 读取 %s 失败\n", argv[1]); fclose(f); return 1;
    }
    fclose(f);

    // ── 链路：解析器 → 消费者 ──
    ItchParser parser;
    OrderBookConsumer consumer;
    parser.set_sink([&](const MarketEvent& ev) { consumer.on_event(ev); });

    size_t pos = 0;
    while (pos + 2 <= data.size()) {
        uint16_t body_len = (static_cast<uint16_t>(data[pos]) << 8) | data[pos+1];
        if (body_len < 1 || body_len > 200) { ++pos; continue; }
        size_t msg_len = 2 + body_len;
        if (pos + msg_len > data.size()) break;
        parser.feed(&data[pos+2], body_len);
        pos += msg_len;
    }

    // ── 断言：消息计数 ──
    // feed 统计所有喂进去的消息。样本完整消息 249789 条（末尾 27 字节截断尾巴被跳过）。
    CHECK(parser.message_count() == 249789);
    printf("message_count = %llu\n", (unsigned long long)parser.message_count());

    // ── 断言：locate → symbol 映射（解析器 + 消费者都记录）──
    CHECK(parser.symbol(1) == "A");
    CHECK(parser.symbol(2) == "AA");
    CHECK(consumer.symbol(1) == "A");
    printf("symbol(1)='%s' symbol(2)='%s'\n",
           parser.symbol(1).c_str(), parser.symbol(2).c_str());

    // ── 断言：订单簿重建（VOD, locate=8432）──
    const OrderBook* vod = consumer.book(8432);
    CHECK(vod != nullptr);
    if (vod) {
        // 买卖方向分离：best_bid 应低于 best_ask
        int64_t bb = vod->best_bid();
        int64_t ba = vod->best_ask();
        printf("VOD best_bid=%lld vol=%llu, best_ask=%lld vol=%llu\n",
               (long long)bb, (unsigned long long)vod->best_bid_volume(),
               (long long)ba, (unsigned long long)vod->best_ask_volume());
        CHECK(bb == 1984);
        CHECK(vod->best_bid_volume() == 6700);
        CHECK(ba == 1987);
        CHECK(vod->best_ask_volume() == 5900);
        CHECK(bb < ba);  // 买卖分离正确：买价 < 卖价
        CHECK(vod->bid_levels() == 4);
        CHECK(vod->ask_levels() == 4);
    }

    // ── 断言：成交 Tick 生成 ──
    Tick t;
    CHECK(consumer.last_tick(t));
    if (consumer.last_tick(t)) {
        printf("last_tick: ts=%llu locate=%llu price=%lld vol=%llu\n",
               (unsigned long long)t.timestamp,
               (unsigned long long)t.symbol_id,
               (long long)t.last_price,
               (unsigned long long)t.volume);
        // 样本最后一条 E 成交：locate=8132 price=4533 vol=30
        CHECK(t.symbol_id == 8132);
        CHECK(t.last_price == 4533);
        CHECK(t.volume == 30);
        CHECK(t.timestamp > 0);
    }

    if (g_failures == 0) {
        printf("\nITCH 解析器 + 消费者单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
