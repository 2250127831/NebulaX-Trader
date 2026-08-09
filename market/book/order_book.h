#pragma once

#include "core/memory/order_map.h"
#include "core/memory/order_pool.h"
#include "core/types.h"

#include <cstddef>
#include <cstdint>

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

    // 共享池 + 共享索引（唯一构造）:
    // OrderPool/OrderMap 由外部(主线程)创建为全局，所有订单簿引用同一份。
    // 挂单数据与索引全局唯一，每簿只保留盘口(bids_/asks_)。
    // 不再支持自建——多股票场景下自建池/索引是设计缺陷(见 OOM 根因)。
    explicit OrderBook(OrderPool& shared_pool, OrderMap& shared_index)
        : pool_(shared_pool)
        , order_index_(shared_index)
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

    // TopOfBook：best bid / best ask（V2.2: 哈希表 + 极值缓存, O(1)）
    int64_t best_bid() const { return bids_.empty() ? -1 : bids_.best(); }
    int64_t best_ask() const { return asks_.empty() ? -1 : asks_.best(); }
    uint64_t bid_volume_at(int64_t price) const {
        PriceLevel lvl; return bids_.find(price, lvl) ? lvl.total_qty : 0;
    }
    uint64_t ask_volume_at(int64_t price) const {
        PriceLevel lvl; return asks_.find(price, lvl) ? lvl.total_qty : 0;
    }
    uint64_t best_bid_volume() const { return bids_.best_qty(); }
    uint64_t best_ask_volume() const { return asks_.best_qty(); }
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

    // ── 价格档表（V2.2: std::map → 开地址哈希 + 极值缓存）──
    // 生产路径只读 best_bid/best_ask（TopOfBook），中间档从不读 → 无需全序。
    // 开地址哈希 O(1) 插/删/查 + 缓存极值 O(1) 读 TopOfBook；删 best 时降级重扫
    // （档数实测仅 4，几乎零成本）。替代红黑树 std::map（O(log n) erase + 指针追逐，
    // 是 V2.1 perf 的 worker CPU 热点）。
    class PriceLevelTable {
    public:
        static constexpr size_t kInitCapacity = 1024;    // 初始单边档数(实测 VOD 4 档; 大标的有超 64 档, 1024 起步)
        static constexpr int64_t kEmpty = INT64_MIN;     // 从未占用的空槽(查找终止哨兵)
        static constexpr int64_t kTombstone = INT64_MIN + 1;   // 已删除(查找穿过, 插入复用)

        explicit PriceLevelTable(bool is_bid)
            : is_bid_(is_bid), best_(is_bid ? INT64_MIN : INT64_MAX),
              slots_(new Slot[kInitCapacity]), cap_(kInitCapacity) {
            for (size_t i = 0; i < cap_; ++i) slots_[i].price = kEmpty;
        }

        // 值存 map 的 OrderBook 需要正确析构/拷贝。删除拷贝(books_ 靠 try_emplace
        // 原地构造, 不拷贝), 只允许移动构造(初始 1024 槽小, 移动开销可忽略)。
        ~PriceLevelTable() { delete[] slots_; }
        PriceLevelTable(const PriceLevelTable&) = delete;
        PriceLevelTable& operator=(const PriceLevelTable&) = delete;
        PriceLevelTable(PriceLevelTable&& o) noexcept
            : is_bid_(o.is_bid_), best_(o.best_), slots_(o.slots_), cap_(o.cap_), count_(o.count_) {
            o.slots_ = nullptr; o.cap_ = 0; o.count_ = 0;
        }
        PriceLevelTable& operator=(PriceLevelTable&& o) noexcept {
            if (this != &o) {
                delete[] slots_;
                is_bid_ = o.is_bid_; best_ = o.best_;
                slots_ = o.slots_; cap_ = o.cap_; count_ = o.count_;
                o.slots_ = nullptr; o.cap_ = 0; o.count_ = 0;
            }
            return *this;
        }

        // 语义同 std::map::operator[]：取或插入（返回可修改引用）。
        // 查找穿过 tombstone、只停在空槽；未命中时优先复用探测链上第一个 tombstone。
        // 新插入清空 lvl（tombstone 残留旧值，须重置为默认——等同 std::map 新节点）。
        // 表满(有效+tombstone 占满, 无空槽)且 key 不在时扩容(翻倍, 学 vector)——避免
        // 死循环或覆盖。扩容后重新定位插入槽(旧 slots_ 已释放, 引用须重取)。
        PriceLevel& operator[](int64_t price) {
            size_t i = probe(price);
            size_t first_tomb = cap_;                    // 探测链上第一个 tombstone
            bool full = true;                            // 探测绕满一圈 = 无空槽
            for (size_t n = 0; n < cap_; ++n) {
                if (slots_[i].price == price) {          // 已存在
                    if (is_bid_ ? price > best_ : price < best_) best_ = price;
                    return slots_[i].lvl;
                }
                if (slots_[i].price == kTombstone && first_tomb == cap_)
                    first_tomb = i;
                if (slots_[i].price == kEmpty) { full = false; break; }   // 空槽终止(查找结束)
                i = (i + 1) & (cap_ - 1);
            }
            // 未命中: 绕满一圈(无空槽) → 必须扩容(即使有 tombstone 可复用, 否则
            // count_ 会超 cap_ 逻辑错乱)。扩容丢弃 tombstone 重新哈希。
            if (full) {
                grow();
                i = probe(price);
                while (slots_[i].price != kEmpty) i = (i + 1) & (cap_ - 1);   // 新表必有空槽
                first_tomb = cap_;                        // 新表无 tombstone
            }
            size_t ins = (first_tomb != cap_) ? first_tomb : i;
            slots_[ins].price = price;
            slots_[ins].lvl = PriceLevel{};               // 清残留
            ++count_;
            if (is_bid_ ? price > best_ : price < best_) best_ = price;   // 更新极值
            return slots_[ins].lvl;
        }

        // 删除（标 tombstone，不立即腾位——查找穿过 tombstone 链不破）。
        // 删的是 best_ 则降级重扫找新极值。
        void erase(int64_t price) {
            size_t i = probe(price);
            for (size_t n = 0; n < cap_; ++n) {
                if (slots_[i].price == price) {
                    slots_[i].price = kTombstone;
                    --count_;
                    if (price == best_) rescan_best();
                    return;
                }
                if (slots_[i].price == kEmpty) return;   // 空槽终止, 不存在
                i = (i + 1) & (cap_ - 1);
            }
        }

        bool find(int64_t price, PriceLevel& out) const {
            size_t i = probe(price);
            for (size_t n = 0; n < cap_; ++n) {
                if (slots_[i].price == price) { out = slots_[i].lvl; return true; }
                if (slots_[i].price == kEmpty) return false;   // 空槽终止, 不存在
                i = (i + 1) & (cap_ - 1);
            }
            return false;
        }

        int64_t best() const { return best_; }      // 买=最高价 / 卖=最低价
        size_t size() const { return count_; }      // 有效档数(不含 tombstone)
        size_t capacity() const { return cap_; }
        bool empty() const { return count_ == 0; }

        // TopOfBook 档的总挂单量（best_ 对应的 total_qty）
        uint64_t best_qty() const {
            if (empty()) return 0;
            size_t i = probe(best_);
            for (size_t n = 0; n < cap_; ++n) {
                if (slots_[i].price == best_) return slots_[i].lvl.total_qty;
                if (slots_[i].price == kEmpty) break;
                i = (i + 1) & (cap_ - 1);
            }
            return 0;                                    // best_ 不在(逻辑上不应发生)
        }

    private:
        // 黄金常数乘高位取模：避免直接 price&N 把 N 的倍数价格全映射到同槽。
        // cap_ 是 2 的幂, 取高 log2(cap_) 位与 (cap_-1)。
        size_t probe(int64_t price) const {
            uint64_t h = (uint64_t)price * 0x9E3779B97F4A7C15ULL;
            return (h >> (64 - __builtin_ctz(cap_))) & (cap_ - 1);
        }

        // 扩容(翻倍, 学 vector)。丢弃 tombstone(重新哈希有效键), 探测链天然干净。
        // 只在 operator[] 表满时触发, 频率极低(档数 >1024 才首次扩容)。
        void grow() {
            size_t new_cap = cap_ * 2;
            Slot* ns = new Slot[new_cap];
            for (size_t i = 0; i < new_cap; ++i) ns[i].price = kEmpty;
            const size_t shift = 64 - __builtin_ctz(new_cap);
            // 重新哈希所有有效档(跳过 tombstone)
            for (size_t i = 0; i < cap_; ++i) {
                if (slots_[i].price == kEmpty || slots_[i].price == kTombstone) continue;
                int64_t p = slots_[i].price;
                size_t j = ((uint64_t)p * 0x9E3779B97F4A7C15ULL >> shift) & (new_cap - 1);
                while (ns[j].price != kEmpty) j = (j + 1) & (new_cap - 1);
                ns[j] = slots_[i];
            }
            delete[] slots_;
            slots_ = ns;
            cap_ = new_cap;
        }

        // 删 best 后降级重扫全表找新极值（档数少，几乎零成本）
        void rescan_best() {
            best_ = is_bid_ ? INT64_MIN : INT64_MAX;
            for (size_t i = 0; i < cap_; ++i) {
                if (slots_[i].price == kEmpty || slots_[i].price == kTombstone) continue;
                if (is_bid_ ? slots_[i].price > best_ : slots_[i].price < best_)
                    best_ = slots_[i].price;
            }
        }

        struct Slot { int64_t price; PriceLevel lvl; };
        bool is_bid_;                 // true=买边(best 取最大价), false=卖边(取最小价)
        int64_t best_;                // 当前极值价缓存(买=最高, 卖=最低)
        Slot*   slots_;               // 开地址数组(初始 1024, 满则翻倍扩容)
        size_t  cap_;                 // 当前槽数(2 的幂)
        size_t  count_ = 0;           // 有效档数
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

    OrderPool& pool_;                                     // 共享挂单池（外部持有）
    OrderMap&  order_index_;                              // 共享挂单索引（外部持有）
    PriceLevelTable bids_{true};                          // 买档（best=最高价）
    PriceLevelTable asks_{false};                         // 卖档（best=最低价）
};
