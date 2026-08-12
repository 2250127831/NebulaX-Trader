// 订单协议 codec 单测(V5 解耦): IOrderCodec 接口 + CustomOrderCodec 实现
// 验证:
//   - 帧长: order_msg_len==42 / fill_msg_len==25(自定义 'O'/'F' 协议)
//   - 订单编解码往返: Order → encode → decode → 字段一致(大端字节序)
//   - 成交回报编解码往返: encode_fill → decode_fill → 字段一致
//   - 解码校验: 越界长度 / 错误 magic → 返回 false
//   - 多态: 通过 IOrderCodec* 调用(证明业务代码只依赖接口, 不依赖具体协议)

#include "oms/i_order_codec.h"
#include "oms/custom_order_codec.h"
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
    CustomOrderCodec codec;
    IOrderCodec* iface = &codec;   // 通过接口调(证明业务只依赖抽象)

    // ── 1. 帧长 ──
    CHECK(iface->order_msg_len() == 42);
    CHECK(iface->fill_msg_len() == 25);

    // ── 2. 订单编解码往返 ──
    Order o{};
    o.order_id = 12345;
    o.strategy_id = 7;
    o.symbol_id = 65535;          // ITCH locate 上限
    o.side = OrderSide::SELL;
    o.type = OrderType::LIMIT;
    o.price = 10000;              // 定点分
    o.quantity = 500;
    o.timestamp = 9876543210ull;

    uint8_t buf[64];
    size_t len = 0;
    CHECK(iface->encode_order(o, buf, sizeof(buf), len));
    CHECK(len == 42);
    CHECK(buf[0] == 'O');                       // magic
    // 大端布局抽查: order_id=12345(0x3039) → 高字节@1=0, 低字节@8=0x39; side@17=1(SELL)
    CHECK(buf[1] == 0 && buf[2] == 0 && buf[7] == 0x30 && buf[8] == 0x39);
    CHECK(buf[17] == 1);                        // SELL=1
    // price=10000(0x2710) → 低字节@25=0x10; qty=500(0x1F4) → 低字节@33=0xF4
    CHECK(buf[18] == 0 && buf[25] == 0x10);
    CHECK(buf[33] == 0xF4);

    Order r{};
    CHECK(iface->decode_order(buf, len, r));
    CHECK(r.order_id == o.order_id);
    CHECK(r.symbol_id == o.symbol_id);
    CHECK(r.side == OrderSide::SELL);
    CHECK(r.price == o.price);
    CHECK(r.quantity == o.quantity);
    CHECK(r.timestamp == o.timestamp);
    CHECK(r.type == OrderType::MARKET);         // type 不在协议, decode 统一填 MARKET

    // ── 3. 成交回报编解码往返 ──
    uint8_t fill[32];
    size_t flen = 0;
    CHECK(iface->encode_fill(999, 200, 10000, fill, sizeof(fill), flen));
    CHECK(flen == 25);
    CHECK(fill[0] == 'F');
    uint64_t oid = 0, qty = 0;
    int64_t price = 0;
    CHECK(iface->decode_fill(fill, flen, oid, qty, price));
    CHECK(oid == 999 && qty == 200 && price == 10000);

    // ── 4. 解码校验: 越界 / 错误 magic ──
    CHECK(!iface->decode_order(buf, 41, r));                 // 长度不足
    CHECK(!iface->decode_fill(fill, 24, oid, qty, price));   // 长度不足
    uint8_t bad[42];
    memcpy(bad, buf, 42);
    bad[0] = 'X';                                            // 错误 magic
    CHECK(!iface->decode_order(bad, 42, r));
    uint8_t badfill[25];
    memcpy(badfill, fill, 25);
    badfill[0] = 'Y';
    CHECK(!iface->decode_fill(badfill, 25, oid, qty, price));

    // ── 5. 编码校验: 容量不足 / 空指针 ──
    size_t small_len = 0;
    CHECK(!iface->encode_order(o, buf, 41, small_len));      // 容量不足
    CHECK(!iface->encode_fill(1, 1, 1, fill, 24, small_len));
    CHECK(!iface->encode_order(o, nullptr, 42, small_len));  // 空指针
    CHECK(!iface->encode_fill(1, 1, 1, nullptr, 25, small_len));

    if (g_failures == 0) {
        printf("订单协议 codec 单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
