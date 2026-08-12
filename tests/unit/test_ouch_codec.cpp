// OUCH 4.2 订单协议 codec 单测(V5 实盘协议化)
// 验证:
//   - 'O' Enter Order 49B 编解码 + checksum
//   - token↔order_id 映射(encode 登记 / decode_fill 查表)
//   - 'A' Accepted / 'E' Executed 回报解码
//   - 价格换算(内部分 ×100 → OUCH ×10000)
//   - 越界/错误 checksum → 返回 false

#include "oms/i_order_codec.h"
#include "oms/ouch_order_codec.h"
#include "core/types.h"

#include <cstdio>
#include <cstring>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

int main() {
    OuchOrderCodec codec;
    IOrderCodec* iface = &codec;

    // ── 1. 帧长 ──
    CHECK(iface->order_msg_len() == 49);
    CHECK(iface->fill_msg_len() == 34);   // 最小回报帧(Executed)

    // ── 2. 'O' Enter Order 编解码 + token 映射 ──
    Order o{};
    o.order_id = 42;
    o.symbol_id = 65535;
    o.side = OrderSide::SELL;
    o.type = OrderType::LIMIT;
    o.price = 10000;          // 分 → OUCH ×10000 = 100000000
    o.quantity = 500;
    o.timestamp = 0;

    uint8_t buf[64];
    size_t len = 0;
    CHECK(iface->encode_order(o, buf, sizeof(buf), len));
    CHECK(len == 49);
    CHECK(buf[0] == 'O');
    // token = 14 位定宽 order_id 字符串
    CHECK(std::memcmp(buf + 1, "00000000000042", 14) == 0);
    CHECK(buf[15] == 'S');                    // SELL
    // qty=500 u32 BE @16-19 = 0x000001F4
    CHECK(buf[16] == 0 && buf[17] == 0 && buf[18] == 0x01 && buf[19] == 0xF4);
    // price=10000分=100.00元 ×10000 @30-33 = 1000000 (0x000F4240)
    CHECK(buf[30] == 0x00 && buf[31] == 0x0F && buf[32] == 0x42 && buf[33] == 0x40);
    CHECK(buf[48] != 0);                       // checksum 非零(至少有一字节非空格)

    // decode_order(模拟交易所侧): token → order_id 查表
    Order r{};
    CHECK(iface->decode_order(buf, len, r));
    CHECK(r.order_id == 42);                   // token 映射回
    CHECK(r.side == OrderSide::SELL);
    CHECK(r.quantity == 500);
    CHECK(r.price == 10000);                   // OUCH ×10000 → 分
    CHECK(r.symbol_id == 65535);

    // ── 3. 'E' Executed 回报解码(token → order_id) ──
    // encode_exec 用同一 codec(登记了 token 42)
    uint8_t exec[64];
    size_t elen = 0;
    CHECK(codec.encode_exec(42, 500, 10000, exec, sizeof(exec), elen));
    CHECK(elen == 34);
    CHECK(exec[0] == 'E');
    Fill f;
    CHECK(iface->decode_fill(exec, elen, f));
    CHECK(f.type == 'E');
    CHECK(f.order_id == 42);                   // token 查表 → order_id
    CHECK(f.filled_qty == 500);
    CHECK(f.fill_price == 10000);              // OUCH ×10000 → 分

    // ── 4. 'A' Accepted 回报解码 ──
    uint8_t ack[64];
    size_t alen = 0;
    CHECK(codec.encode_ack(o, ack, sizeof(ack), alen));
    CHECK(alen == 31);
    CHECK(ack[0] == 'A');
    Fill fa;
    CHECK(iface->decode_fill(ack, alen, fa));
    CHECK(fa.type == 'A');
    CHECK(fa.order_id == 42);

    // ── 5. 校验: 错误 checksum / 未知类型 / 长度不足 ──
    uint8_t bad[49];
    memcpy(bad, buf, 49);
    bad[48] = static_cast<uint8_t>(bad[48] + 1);   // 破坏 checksum
    Order rb;
    CHECK(!iface->decode_order(bad, 49, rb));
    uint8_t unknown[1] = {'X'};
    Fill fu;
    CHECK(!iface->decode_fill(unknown, 1, fu));
    CHECK(!iface->decode_fill(buf, 48, fu));        // 长度不足
    CHECK(!iface->encode_order(o, buf, 48, len));   // 容量不足

    // ── 6. 映射幂等: 同 order_id 重新 encode, token 一致 ──
    uint8_t buf2[64];
    size_t len2 = 0;
    CHECK(iface->encode_order(o, buf2, sizeof(buf2), len2));
    CHECK(std::memcmp(buf + 1, buf2 + 1, 14) == 0);   // 同一 token

    if (g_failures == 0) {
        printf("OUCH 4.2 codec 单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
