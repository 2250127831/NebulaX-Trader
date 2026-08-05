#pragma once

#include "strategy/base/signal.h"
#include "oms/order_manager.h"
#include "oms/order_protocol.h"
#include "risk/risk_manager.h"
#include "core/net/i_market_data_sender.h"

#include <cstdint>
#include <mutex>

// ── 执行引擎 ──
// 把策略信号转成订单，走 风控校验 → OMS 登记 → 发送 → 成交回报 的完整链路。
//
// 信号 → 数量：经典策略不直接给下单量，数量由资金管理层换算。
//   quantity = base_qty × strength / kStrengthScale
//   即满强度下单 base_qty，半强度下 base_qty 的一半；NONE 不下单。
//
// 发送模式：
//   - 接了 sender(IMarketDataSender)：订单序列化后真实发送，成交回报经 on_order_fill
//     从交易所返回驱动 OMS/Risk(真实闭环)。
//   - 未接 sender：退回进程内模拟成交(单测验证业务逻辑用，不涉及网络)。
//
// 线程安全：submit_signal(下单线程)与 on_order_fill(回报线程)并发调用，
// OMS/Risk 内部容器非线程安全，访问统一串行化(单互斥锁)。
class ExecutionEngine {
public:
    ExecutionEngine(OrderManager& om, RiskManager& rm)
        : order_manager_(om), risk_manager_(rm) {}

    // 配置
    void set_base_qty(uint64_t qty) { base_qty_ = qty; }
    void set_sender(IMarketDataSender* sender) { sender_ = sender; }

    // 提交一个策略信号，返回订单 id。无信号(side NONE)/无价格 → 0(不下单)。
    // 风控拒绝 → 订单登记为 REJECTED，返回其 id。
    // 发送失败 → REJECTED。发送成功 → PENDING，等成交回报驱动状态。
    uint64_t submit_signal(const Signal& sig, uint64_t strategy_id) {
        if (sig.side == OrderSide::NONE || sig.price < 0) return 0;

        // 数量：满强度 = base_qty，无强度 = 0(不下单)
        uint64_t qty = base_qty_ * (uint64_t)sig.strength
                     / (uint64_t)Signal::kStrengthScale;
        if (qty == 0) return 0;

        Order order{};
        order.strategy_id = strategy_id;
        order.symbol_id   = sig.locate;
        order.side        = sig.side;
        order.type        = OrderType::MARKET;
        order.price       = sig.price;
        order.quantity    = qty;
        order.timestamp   = sig.timestamp;

        uint8_t buf[kOrderMsgLen];
        uint64_t id;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            id = order_manager_.new_order(order);         // PENDING
            if (!risk_manager_.check_order(order)) {
                order_manager_.on_reject(id);             // 风控拦截
                return id;
            }
            if (sender_) {
                // 发送在锁内: IoUringSender 内部是 SPSCByteRing 非线程安全,
                // 分簿后多 worker 并发 submit_signal → 锁内串行化 send。
                // 下单频率万级, io_uring SQE 提交非阻塞, 锁内可接受。
                encode_order(order, buf);
                ssize_t r = sender_->send(buf, kOrderMsgLen);
                if (r != (ssize_t)kOrderMsgLen)
                    order_manager_.on_reject(id);         // 发送失败
                // 发送成功：保持 PENDING，等交易所成交回报
            } else {
                // 无 sender：进程内模拟成交(单测/无网络模式)
                order_manager_.on_fill(id, qty);
                risk_manager_.on_fill(order);
            }
        }
        return id;
    }

    // 成交回报：交易所确认成交，驱动 OMS 状态 + 风控持仓/盈亏。
    // V1 模拟交易所按订单价全额成交，成交价恒等于订单价，故忽略 fill_price。
    void on_order_fill(uint64_t order_id, uint64_t filled_qty, int64_t /*fill_price*/) {
        std::lock_guard<std::mutex> lk(mtx_);
        const Order* o = order_manager_.order(order_id);
        if (!o) return;
        order_manager_.on_fill(order_id, filled_qty);
        risk_manager_.on_fill(*o);
    }

private:
    OrderManager& order_manager_;
    RiskManager&  risk_manager_;
    IMarketDataSender* sender_ = nullptr;
    uint64_t base_qty_ = 100;   // 满强度基准下单量(股)
    std::mutex mtx_;
};
