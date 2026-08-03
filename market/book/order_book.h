#pragma once

#include "core/memory/order_map.h"
#include "core/memory/order_pool.h"
#include "core/types.h"

#include <cstdint>
#include <map>
#include <memory>

// ── 高性能订单簿（单标的盘口）──
// 迁移自 NebulaX matching 引擎的 OrderBook 核心设计，按行情簿语义适配。
//
// 保留的撮合簿高性能核心：
//   - bids_/asks_: map<price, PriceLevel>（买降序 / 卖升序），TopOfBook O(1)
//   - 每档 PriceLevel: 挂单 intrusive 链表 + 档总量 + 档内单数
//   - OrderPool: 固定容量池化挂单，零堆分配（下单路径无 new/delete）
//   - OrderMap: order_ref → OrderSlot* 零堆分配哈希（查单 O(1)）
//
// 适配的行情语义（与撮合簿的差异）：
//   - 不撮合：只 ADD/CANCEL/DELETE/EXECUTE（重建盘口快照），不匹配买卖
//   - ITCH order_ref 定位单，X(部分撤)/E(成交) 只带 order_ref，必须查簿
//
// 用途：通道 B 消费委托事件重建盘口，供逐笔委托/订单簿策略（高频）消费。
class OrderBook {
public:
    // 挂单节点（池化存储的 64 字节 OrderSlot），对外暴露只读视图
    struct OrderSlotView {
        uint64_t order_ref;
        OrderSide side;
        int64_t   price;
        uint64_t  shares;
        uint64_t  remaining;
        uint64_t  sequence;
    };

    // 默认：自建内部池（单标的，独立簿用）
    explicit OrderBook(size_t pool_capacity = 1 << 16)
        : owned_pool_(std::make_unique<OrderPool>(pool_capacity))
        , pool_(*owned_pool_)
        , order_index_(pool_capacity)
    {}

    // 共享外部池（多标的簿共用，避免每标的独立大池爆内存）
    explicit OrderBook(OrderPool& shared_pool)
        : pool_(shared_pool)
        , order_index_(shared_pool.capacity())
    {}

    // ── 挂单生命周期 ──

    // 新增挂单（A/F）。返回节点句柄（UINT32_MAX = 池满）。
    uint32_t add(uint64_t order_ref, OrderSide side, int64_t price,
                 uint64_t shares, uint64_t sequence) {
        OrderSlot* s = pool_.allocate();
        if (!s) return UINT32_MAX;
        s->order_ref = order_ref;
        s->side      = side;
        s->price     = price;
        s->shares    = shares;
        s->remaining = shares;
        s->sequence  = sequence;
        s->prev_idx  = UINT32_MAX;
        s->next_idx  = UINT32_MAX;

        uint32_t idx = pool_.indexOf(s);
        PriceLevel& lvl = (side == OrderSide::BUY) ? bids_[price] : asks_[price];
        // 头插（时间优先由 sequence 排序，V1 简单头插）
        if (lvl.head_idx != UINT32_MAX) {
            OrderSlot* head = pool_.at(lvl.head_idx);
            head->prev_idx = idx;
            s->next_idx = lvl.head_idx;
        }
        lvl.head_idx = idx;
        if (lvl.tail_idx == UINT32_MAX) lvl.tail_idx = idx;
        ++lvl.count;
        lvl.total_qty += shares;

        order_index_.insert(order_ref, s);
        return idx;
    }

    // 整笔撤单（D）：全部移除
    bool remove(uint64_t order_ref) {
        OrderSlot* s = order_index_.find(order_ref);
        if (!s) return false;
        unlink_and_free(s);
        order_index_.erase(order_ref);
        return true;
    }

    // 部分撤单（X）：该挂单减少 cancelled_shares
    bool cancel(uint64_t order_ref, uint64_t cancelled_shares) {
        OrderSlot* s = order_index_.find(order_ref);
        if (!s) return false;
        if (cancelled_shares > s->remaining) cancelled_shares = s->remaining;
        s->remaining -= cancelled_shares;
        dec_level_qty(s->side, s->price, cancelled_shares);
        if (s->remaining == 0) {
            unlink_and_free(s);
            order_index_.erase(order_ref);
        }
        return true;
    }

    // 成交（E/P）：该挂单剩余量减少 executed_shares（价格用挂单价）
    bool execute(uint64_t order_ref, uint64_t executed_shares) {
        OrderSlot* s = order_index_.find(order_ref);
        if (!s) return false;
        if (executed_shares > s->remaining) executed_shares = s->remaining;
        s->remaining -= executed_shares;
        dec_level_qty(s->side, s->price, executed_shares);
        if (s->remaining == 0) {
            unlink_and_free(s);
            order_index_.erase(order_ref);
        }
        return true;
    }

    // 改单（U）：old_ref 作废，new_ref 以新价/新量挂出。U 无方向，side 由调用方传入。
    bool replace(uint64_t old_ref, uint64_t new_ref, OrderSide side,
                 int64_t new_price, uint64_t new_shares, uint64_t new_sequence) {
        OrderSlot* s = order_index_.find(old_ref);
        if (!s) return false;
        uint64_t seq = (new_sequence != 0) ? new_sequence : s->sequence;
        remove(old_ref);
        uint32_t idx = add(new_ref, side, new_price, new_shares, seq);
        return idx != UINT32_MAX;
    }

    // ── 查询 ──

    // 某挂单视图（不存在返回 false）
    bool get(uint64_t order_ref, OrderSlotView& out) const {
        OrderSlot* s = order_index_.find(order_ref);
        if (!s) return false;
        out = OrderSlotView{s->order_ref, s->side, s->price, s->shares,
                            s->remaining, s->sequence};
        return true;
    }

    // 某挂单的方向(不存在返回 NONE)——OFI 查 D/X/E 方向用
    OrderSide side_of(uint64_t order_ref) const {
        OrderSlot* s = order_index_.find(order_ref);
        return s ? s->side : OrderSide::NONE;
    }

    // TopOfBook：best bid / best ask
    int64_t best_bid() const { return bids_.empty() ? -1 : bids_.begin()->first; }
    int64_t best_ask() const { return asks_.empty() ? -1 : asks_.begin()->first; }
    uint64_t bid_volume_at(int64_t price) const { return level_qty(bids_, price); }
    uint64_t ask_volume_at(int64_t price) const { return level_qty(asks_, price); }
    uint64_t best_bid_volume() const {
        auto it = bids_.begin();
        return it == bids_.end() ? 0 : it->second.total_qty;
    }
    uint64_t best_ask_volume() const {
        auto it = asks_.begin();
        return it == asks_.end() ? 0 : it->second.total_qty;
    }
    size_t bid_levels() const { return bids_.size(); }
    size_t ask_levels() const { return asks_.size(); }
    bool empty() const { return bids_.empty() && asks_.empty(); }

    // 池用量（验证/监控）
    size_t pool_usage() const { return pool_.size(); }
    size_t pool_capacity() const { return pool_.capacity(); }

private:
    struct PriceLevel {
        uint32_t head_idx = UINT32_MAX;   // 档内链表头（最新挂单）
        uint32_t tail_idx = UINT32_MAX;   // 档内链表尾（最早挂单）
        uint32_t count = 0;               // 档内单数
        uint32_t total_qty = 0;           // 档内总挂单量
    };

    // 从档内链表摘除并归还池
    void unlink_and_free(OrderSlot* s) {
        PriceLevel& lvl = (s->side == OrderSide::BUY) ? bids_[s->price] : asks_[s->price];
        uint32_t idx = pool_.indexOf(s);
        if (s->prev_idx != UINT32_MAX) pool_.at(s->prev_idx)->next_idx = s->next_idx;
        else lvl.head_idx = s->next_idx;
        if (s->next_idx != UINT32_MAX) pool_.at(s->next_idx)->prev_idx = s->prev_idx;
        else lvl.tail_idx = s->prev_idx;
        --lvl.count;
        lvl.total_qty -= (uint32_t)s->shares;
        if (lvl.count == 0) {   // 档空移除
            if (s->side == OrderSide::BUY) bids_.erase(s->price);
            else asks_.erase(s->price);
        }
        pool_.deallocate(idx);
    }

    void dec_level_qty(OrderSide side, int64_t price, uint64_t shares) {
        PriceLevel& lvl = (side == OrderSide::BUY) ? bids_[price] : asks_[price];
        lvl.total_qty -= (uint32_t)shares;
        if (lvl.total_qty == 0) {   // 档空移除（该档所有单已撤/成交）
            if (side == OrderSide::BUY) bids_.erase(price);
            else asks_.erase(price);
        }
    }

    static uint64_t level_qty(const std::map<int64_t, PriceLevel, std::greater<>>& m,
                              int64_t p) {
        auto it = m.find(p); return it == m.end() ? 0 : it->second.total_qty;
    }
    static uint64_t level_qty(const std::map<int64_t, PriceLevel>& m, int64_t p) {
        auto it = m.find(p); return it == m.end() ? 0 : it->second.total_qty;
    }

    std::unique_ptr<OrderPool> owned_pool_;               // 自建池（外部池时 nullptr）
    OrderPool& pool_;                                     // 始终指向可用池（外部或自建）
    std::map<int64_t, PriceLevel, std::greater<>> bids_;  // 买档：价降序
    std::map<int64_t, PriceLevel> asks_;                  // 卖档：价升序
    OrderMap order_index_;                                // order_ref → OrderSlot*
};
