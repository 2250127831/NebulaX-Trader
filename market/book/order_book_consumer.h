#pragma once

#include "core/market_event.h"
#include "core/types.h"
#include "market/book/order_book.h"

#include <cstdint>
#include <string>
#include <unordered_map>

// ── 订单簿消费者 ──
// 消费 MarketEvent（解析器的产物），重建多标的订单簿，并产出成交 Tick。
// 与解析器解耦：解析器只产出事件，簿的重建全在这里。
//
// 边界（见 docs/MARKET_DATA_DECISION.md §2.5）：
//   解析器 = 纯转换（字节 → 事件），不碰簿。
//   本消费者 = 消费事件，重建盘口，E 事件查价由这里做。
//
// 职责：
//   - ADD/DELETE/CANCEL/REPLACE → 更新对应 locate 的 OrderBook
//   - TRADE/EXECUTE → 更新簿 + 产出 Tick（喂策略）
//   - STOCK_DIR → 记录 locate→symbol
//
// V1 单线程：解析器回调直接调 on_event()（直通）。
// V2 并行：解析器 → SPSC 队列 → 消费者从队列取事件调 on_event()。
// 本类不关心事件怎么来，只处理事件。
class OrderBookConsumer {
public:
    // 共享池：所有标的簿共用，避免每标的独立大池爆内存
    OrderBookConsumer(size_t pool_capacity = 1 << 20)
        : shared_pool_(pool_capacity) {}

    // 处理一个事件（生产者在解析器回调里调用）
    void on_event(const MarketEvent& ev);

    // locate → 订单簿（不存在返回 nullptr）
    const OrderBook* book(uint64_t locate) const {
        auto it = books_.find(locate);
        return it == books_.end() ? nullptr : &it->second;
    }

    // 最近一条成交 Tick（没有成交返回 false）
    bool last_tick(Tick& out) const {
        if (!has_tick_) return false;
        out = last_tick_;
        return true;
    }

private:
    OrderBook& book_for(uint64_t locate);   // 取指定标的簿（共享池新建）
    void handle_add(const MarketEvent& ev);
    void handle_delete(const MarketEvent& ev);
    void handle_cancel(const MarketEvent& ev);
    void handle_replace(const MarketEvent& ev);
    void handle_trade(const MarketEvent& ev, bool has_price);
    void handle_execute(const MarketEvent& ev);

    OrderPool shared_pool_;                               // 共享挂单池（所有标的簿共用）
    std::unordered_map<uint64_t, OrderBook> books_;       // locate → 订单簿（共享池构造）
    Tick last_tick_{};
    bool has_tick_ = false;
    uint64_t tick_seq_ = 0;   // Tick 序列号（来自事件计数）
    uint64_t order_seq_ = 0;  // 挂单序列号（时间优先 FIFO，跨事件递增）
};
