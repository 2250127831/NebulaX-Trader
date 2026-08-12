// OUCH 回报语义枚举测试(V5): 不用真实撮合, 构造各类回报场景验证 OMS 状态机。
// 验证:
//   - 'A' Accepted → 订单 PENDING→SUBMITTED(进入活态)
//   - 'E' Executed 多次 → 累积成交, PARTIAL_FILL → FILLED(半成交语义)
//   - 'C' Canceled → CANCELLED
//   - 'J' Rejected → REJECTED
//   - 非法流转拒收(已成交不可撤/已拒不可接受)
//   - 解码各类型: A/E/C/J 字节 → Fill, token 解析 order_id
//   - 端到端: encode_order(登记 token) → 各回报 encode → decode_fill → on_order_report

#include "oms/i_order_codec.h"
#include "oms/ouch_order_codec.h"
#include "oms/order_manager.h"
#include "risk/risk_manager.h"
#include "execution/execution_engine.h"

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
    OrderManager om;
    RiskManager rm;
    ExecutionEngine ex(om, rm);
    ex.set_base_qty(100);

    // 下单(无 sender → 走进程内模拟成交分支)。为测 OUCH 回报, 直接操作 OrderManager 状态。
    // 构造订单: order_id 由 new_order 分配, quantity=100。
    Order order{};
    order.symbol_id = 1;
    order.side = OrderSide::BUY;
    order.type = OrderType::MARKET;
    order.price = 10000;
    order.quantity = 100;
    uint64_t oid = om.new_order(order);   // PENDING
    CHECK(oid != 0);
    CHECK(om.status(oid) == OrderStatus::PENDING);

    // ── 1. 'A' Accepted → SUBMITTED ──
    {
        uint8_t ack[OuchOrderCodec::kAckMsgLen];
        size_t alen = 0;
        CHECK(codec.encode_ack(order, ack, sizeof(ack), alen));
        Fill f;
        CHECK(codec.decode_fill(ack, alen, f));
        CHECK(f.type == OuchOrderCodec::kMsgAck);
        CHECK(f.order_id == oid);
        ex.on_order_report(f);
        CHECK(om.status(oid) == OrderStatus::SUBMITTED);
    }

    // ── 2. 'E' Executed 半成交 60 → PARTIAL_FILL ──
    {
        uint8_t exec[OuchOrderCodec::kExecMsgLen];
        size_t elen = 0;
        CHECK(codec.encode_exec(oid, 60, 10000, exec, sizeof(exec), elen));
        Fill f;
        CHECK(codec.decode_fill(exec, elen, f));
        CHECK(f.type == OuchOrderCodec::kMsgExec);
        CHECK(f.order_id == oid);
        CHECK(f.filled_qty == 60);
        ex.on_order_report(f);
        CHECK(om.status(oid) == OrderStatus::PARTIAL_FILL);
        CHECK(rm.position(1) == 60);   // 半成交更新持仓
    }

    // ── 3. 'E' 再成交 40 → FILLED(累积到满) ──
    {
        uint8_t exec[OuchOrderCodec::kExecMsgLen];
        size_t elen = 0;
        CHECK(codec.encode_exec(oid, 40, 10000, exec, sizeof(exec), elen));
        Fill f;
        CHECK(codec.decode_fill(exec, elen, f));
        ex.on_order_report(f);
        CHECK(om.status(oid) == OrderStatus::FILLED);
        CHECK(rm.position(1) == 100);   // 全成交
    }

    // ── 4. 已成交订单不可撤(非法流转拒收) ──
    {
        uint8_t cmsg[OuchOrderCodec::kCancelMsgLen];
        size_t clen = 0;
        CHECK(codec.encode_cancel(oid, 100, cmsg, sizeof(cmsg), clen));
        Fill f;
        CHECK(codec.decode_fill(cmsg, clen, f));
        CHECK(f.type == OuchOrderCodec::kMsgCancel);
        CHECK(f.order_id == oid);
        ex.on_order_report(f);
        CHECK(om.status(oid) == OrderStatus::FILLED);   // 已成交, 撤单拒收
    }

    // ── 5. 'J' Rejected → REJECTED ──
    {
        Order o2{};
        o2.symbol_id = 2;
        o2.side = OrderSide::SELL;
        o2.type = OrderType::MARKET;
        o2.price = 10000;
        o2.quantity = 50;
        uint64_t oid2 = om.new_order(o2);
        CHECK(om.status(oid2) == OrderStatus::PENDING);
        uint8_t jmsg[OuchOrderCodec::kRejectMsgLen];
        size_t jlen = 0;
        CHECK(codec.encode_reject(oid2, jmsg, sizeof(jmsg), jlen));
        Fill f;
        CHECK(codec.decode_fill(jmsg, jlen, f));
        CHECK(f.type == OuchOrderCodec::kMsgReject);
        CHECK(f.order_id == oid2);
        ex.on_order_report(f);
        CHECK(om.status(oid2) == OrderStatus::REJECTED);
    }

    // ── 6. 'A' 对已拒绝订单 → 拒收(非法流转) ──
    {
        // oid2 已 REJECTED, 再来 'A' 应保持 REJECTED
        Order o3{};
        o3.symbol_id = 3;
        o3.side = OrderSide::BUY;
        o3.quantity = 10;
        uint64_t oid3 = om.new_order(o3);
        om.on_reject(oid3);
        CHECK(om.status(oid3) == OrderStatus::REJECTED);
        uint8_t ack[OuchOrderCodec::kAckMsgLen];
        size_t alen = 0;
        CHECK(codec.encode_ack(o3, ack, sizeof(ack), alen));
        Fill f;
        CHECK(codec.decode_fill(ack, alen, f));
        ex.on_order_report(f);
        CHECK(om.status(oid3) == OrderStatus::REJECTED);   // 已拒不可接受
    }

    // ── 7. 多种回报流解码: A 后 C(挂单未成交即撤) ──
    {
        Order o4{};
        o4.symbol_id = 4;
        o4.side = OrderSide::SELL;
        o4.quantity = 30;
        uint64_t oid4 = om.new_order(o4);
        uint8_t ack[OuchOrderCodec::kAckMsgLen];
        size_t alen = 0;
        codec.encode_ack(o4, ack, sizeof(ack), alen);
        uint8_t cmsg[OuchOrderCodec::kCancelMsgLen];
        size_t clen = 0;
        codec.encode_cancel(oid4, 30, cmsg, sizeof(cmsg), clen);
        Fill fa, fc;
        CHECK(codec.decode_fill(ack, alen, fa));
        CHECK(codec.decode_fill(cmsg, clen, fc));
        ex.on_order_report(fa);   // A → SUBMITTED
        CHECK(om.status(oid4) == OrderStatus::SUBMITTED);
        CHECK(om.request_cancel(oid4));   // 已发 'X' 撤单请求 → PENDING_CANCEL
        CHECK(om.status(oid4) == OrderStatus::PENDING_CANCEL);
        ex.on_order_report(fc);   // C → CANCELLED
        CHECK(om.status(oid4) == OrderStatus::CANCELLED);
    }

    if (g_failures == 0) {
        printf("OUCH 回报语义枚举测试 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
