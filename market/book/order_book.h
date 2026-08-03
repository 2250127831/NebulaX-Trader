#pragma once

#include "core/types.h"

#include <cstdint>
#include <map>
#include <unordered_map>

// ── 行情层订单簿（单标的盘口）──
// 重建一个股票（locate）的盘口快照。设计借鉴 NebulaX 撮合引擎的 OrderBook
// 核心：买卖方向分离（bids 降序 / asks 升序）+ 每笔挂单索引。
//
// 职责边界（见 docs/MARKET_DATA_DECISION.md §2.5）：
//   这是行情层的市场快照（对手挂单），不是撮合簿。
//   撮合簿在 NebulaX（撮合引擎），本簿只重建、不撮合。
//
// 与 NebulaX OrderBook 的差异（为什么不是直接迁移）：
//   撮合簿需要 每笔单 FIFO 队列 / 内存池 / 持久化（撮合热路径用）；
//   行情簿只需 每档总量 + 每单剩余量（重建快照用）。
//   这里保留它的核心设计（买卖分离、TopOfBook），去掉撮合专用能力。
//
// 结构（买卖各一侧，与 NebulaX 相同）：
//   bids_:  map<price, uint64_t, greater>   买档：每档总量（降序，最高价在顶）
//   asks_:  map<price, uint64_t>            卖档：每档总量（升序，最低价在顶）
//   orders_: unordered_map<orderref, Info>  每笔挂单：方向 + 价格 + 剩余量
//
// 为什么按 orderref 记每笔单：ITCH 的 X（部分撤）/ E（成交）只带 orderref，
// 必须查 orders_ 才知道撤/吃掉多少、剩多少，否则无法判断全部还是部分。
// 这是"只有成交流无法重建簿"的根源，见 docs/design/itch5_protocol.md。
class OrderBook {
public:
    struct OrderInfo {
        OrderSide side;
        int64_t   price;
        uint64_t  remaining;
    };

    // 新增挂单
    void add(uint64_t orderref, OrderSide side, int64_t price, uint64_t shares) {
        orders_[orderref] = {side, price, shares};
        if (side == OrderSide::BUY) bids_[price] += shares;
        else asks_[price] += shares;
    }

    // 部分撤单：该挂单减少 cancelled_shares
    bool cancel(uint64_t orderref, uint64_t cancelled_shares) {
        auto it = orders_.find(orderref);
        if (it == orders_.end()) return false;
        if (cancelled_shares > it->second.remaining) cancelled_shares = it->second.remaining;
        it->second.remaining -= cancelled_shares;
        dec_level(it->second.side, it->second.price, cancelled_shares);
        if (it->second.remaining == 0) orders_.erase(orderref);
        return true;
    }

    // 整笔撤单：该挂单全部移除
    bool remove(uint64_t orderref) {
        auto it = orders_.find(orderref);
        if (it == orders_.end()) return false;
        dec_level(it->second.side, it->second.price, it->second.remaining);
        orders_.erase(orderref);
        return true;
    }

    // 改单：oldref 作废，newref 以新方向/新价/新量挂出
    void replace(uint64_t oldref, uint64_t newref,
                 OrderSide side, int64_t new_price, uint64_t new_shares) {
        remove(oldref);
        add(newref, side, new_price, new_shares);
    }

    // 成交：该挂单剩余量减少 executed_shares（价格用挂单本身的价格）
    void execute(uint64_t orderref, uint64_t executed_shares) {
        auto it = orders_.find(orderref);
        if (it == orders_.end()) return;
        if (executed_shares > it->second.remaining) executed_shares = it->second.remaining;
        it->second.remaining -= executed_shares;
        dec_level(it->second.side, it->second.price, executed_shares);
        if (it->second.remaining == 0) orders_.erase(orderref);
    }

    // 某档总挂单量（无该档返回 0）
    uint64_t bid_volume_at(int64_t price) const { return volume_at(bids_, price); }
    uint64_t ask_volume_at(int64_t price) const { return volume_at(asks_, price); }

    // ── 盘口快照（TopOfBook，对齐 NebulaX）──
    // bids_ 降序（最高买价在 begin），asks_ 升序（最低卖价在 begin）
    int64_t best_bid() const { return bids_.empty() ? -1 : bids_.begin()->first; }
    int64_t best_ask() const { return asks_.empty() ? -1 : asks_.begin()->first; }
    uint64_t best_bid_volume() const {
        auto it = bids_.find(best_bid());
        return it == bids_.end() ? 0 : it->second;
    }
    uint64_t best_ask_volume() const {
        auto it = asks_.find(best_ask());
        return it == asks_.end() ? 0 : it->second;
    }

    // 某挂单当前剩余量 / 价格（不存在返回 0 / -1）
    uint64_t remaining(uint64_t orderref) const {
        auto it = orders_.find(orderref);
        return it == orders_.end() ? 0 : it->second.remaining;
    }
    int64_t price_of(uint64_t orderref) const {
        auto it = orders_.find(orderref);
        return it == orders_.end() ? -1 : it->second.price;
    }
    OrderSide side_of(uint64_t orderref) const {
        auto it = orders_.find(orderref);
        return it == orders_.end() ? OrderSide::NONE : it->second.side;
    }

    bool empty() const { return bids_.empty() && asks_.empty(); }
    size_t bid_levels() const { return bids_.size(); }
    size_t ask_levels() const { return asks_.size(); }

private:
    static uint64_t volume_at(const std::map<int64_t, uint64_t, std::greater<>>& m, int64_t p) {
        auto it = m.find(p); return it == m.end() ? 0 : it->second;
    }
    static uint64_t volume_at(const std::map<int64_t, uint64_t>& m, int64_t p) {
        auto it = m.find(p); return it == m.end() ? 0 : it->second;
    }

    void dec_level(OrderSide side, int64_t price, uint64_t shares) {
        if (side == OrderSide::BUY) {
            auto it = bids_.find(price);
            if (it == bids_.end()) return;
            if (shares >= it->second) bids_.erase(price);
            else it->second -= shares;
        } else {
            auto it = asks_.find(price);
            if (it == asks_.end()) return;
            if (shares >= it->second) asks_.erase(price);
            else it->second -= shares;
        }
    }

    std::map<int64_t, uint64_t, std::greater<>> bids_;  // 买档：价降序
    std::map<int64_t, uint64_t> asks_;                  // 卖档：价升序
    std::unordered_map<uint64_t, OrderInfo> orders_;    // orderref → {side, price, remaining}
};
