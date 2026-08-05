#pragma once

#include "core/types.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>

// ── order_ref → OrderSlot* 哈希表（迁移自 NebulaX order_map.h）──
// 分离链接法，底层池化管理，零堆分配（构造时预分配），
// 替代 std::unordered_map（下单路径高频查找 order_ref → 挂单）。
// 桶过长(≥8)时整体迁移到 std::map overflow（TREEIFY，防退化）。
class OrderMap {
    // Node.bucket 是并发安全的根基(见 docs/ORDER_MAP_CONCURRENCY.md):
    //   节点被归还池后可能被复用(内存池是栈, 立即复用), next 被改写指向别的桶链 →
    //   无锁 find 可能被带偏。find 每步校验 bucket==目标桶, 不匹配 = 链被并发跳走 → 重试,
    //   用数据结构规避节点复用导致的 ABA(跳链), 不需要惰性删除/hazard pointer/延迟回收。
    struct Node {
        uint64_t          order_ref;
        OrderSlot*        order;
        std::atomic<uint32_t> next_idx;   // 链指针 / 空闲链表(共用, 摘除 CAS 用)
        uint32_t          bucket;     // 本节点所属桶(防 find 跳链, 只在 insert 时写, find 只读)
    };

public:
    OrderMap(size_t capacity)
        : nodes_(new Node[capacity])
        , buckets_(new std::atomic<uint32_t>[roundPow2(capacity)])
        , bucket_len_(new std::atomic<uint16_t>[roundPow2(capacity)])
        , state_(new std::atomic<uint8_t>[roundPow2(capacity)])
        , bucket_mask_(roundPow2(capacity) - 1)
        , hash_shift_(64 - __builtin_ctz(bucket_mask_ + 1))
        , capacity_(capacity)
    {
        for (uint32_t i = 0; i < capacity_ - 1; ++i)
            nodes_[i].next_idx.store(i + 1, std::memory_order_relaxed);
        nodes_[capacity_ - 1].next_idx.store(UINT32_MAX, std::memory_order_relaxed);
        free_head_.store(0, std::memory_order_release);

        for (uint32_t i = 0; i <= bucket_mask_; ++i) {
            buckets_[i].store(UINT32_MAX, std::memory_order_relaxed);
            bucket_len_[i].store(0, std::memory_order_relaxed);
            state_[i].store(0, std::memory_order_relaxed);
        }
    }

    ~OrderMap() {
        delete[] nodes_;
        delete[] buckets_;
        delete[] bucket_len_;
        delete[] state_;
    }

    OrderMap(const OrderMap&) = delete;
    OrderMap& operator=(const OrderMap&) = delete;

    static constexpr uint32_t TREEIFY_THRESHOLD = 8;

    void insert(uint64_t order_ref, OrderSlot* order) {
        uint32_t b = hash(order_ref);
        // 状态门禁: 链路径每一步前检查 state_。读到 ≠0(转化中/已树化) → 锁 overflow_ 写。
        // 转化中(1): 等 treeify 完成(锁会串行化), 然后走 overflow_。
        if (state_[b].load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lk(overflow_lock_);
            overflow_[order_ref] = order;
            size_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        // 桶长≥阈值 或 空闲满 → 触发转化(0→1 CAS)。
        if (bucket_len_[b].load(std::memory_order_relaxed) >= TREEIFY_THRESHOLD ||
            free_head_.load(std::memory_order_relaxed) == UINT32_MAX) {
            treeify(b);
            std::lock_guard<std::mutex> lk(overflow_lock_);
            overflow_[order_ref] = order;
            size_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        uint32_t idx = allocNode();
        if (idx == UINT32_MAX) return;   // 节点池耗尽, 丢弃(不越界写)
        nodes_[idx].order_ref = order_ref;
        nodes_[idx].order     = order;
        nodes_[idx].bucket    = b;
        // 链头 CAS 前重查状态: treeify 可能在 allocNode 期间启动(0→1), 读到则重试进 map。
        if (state_[b].load(std::memory_order_acquire)) {
            freeNode(idx);   // 归还节点(不落链)
            std::lock_guard<std::mutex> lk(overflow_lock_);
            overflow_[order_ref] = order;
            size_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        uint32_t head = buckets_[b].load(std::memory_order_relaxed);
        nodes_[idx].next_idx.store(head, std::memory_order_relaxed);   // next 指向当前桶头
        while (!buckets_[b].compare_exchange_weak(head, idx,
                    std::memory_order_release, std::memory_order_relaxed)) {
            // CAS 失败 → 其他 worker 改了链头, head 已更新, 更新 next 重试。
            // 若转化已启动, 放弃链路径。
            if (state_[b].load(std::memory_order_acquire)) {
                freeNode(idx);
                std::lock_guard<std::mutex> lk(overflow_lock_);
                overflow_[order_ref] = order;
                size_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            nodes_[idx].next_idx.store(head, std::memory_order_relaxed);
        }
        bucket_len_[b].fetch_add(1, std::memory_order_relaxed);
        size_.fetch_add(1, std::memory_order_relaxed);

        if (bucket_len_[b].load(std::memory_order_relaxed) >= TREEIFY_THRESHOLD)
            treeify(b);
    }

    OrderSlot* find(uint64_t order_ref) const {
        uint32_t b = hash(order_ref);
        // 状态门禁: 已树化(2) → 锁 overflow_ 查。转化中(1) → 等锁(串行化)后查。
        if (state_[b].load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lk(overflow_lock_);
            auto it = overflow_.find(order_ref);
            return (it != overflow_.end()) ? it->second : nullptr;
        }
        // 无锁读链 + bucket 校验 + 重试: 读到别的桶的节点 = 链被并发跳走 → 重来,
        // 而不是返回 not found (k 可能还在, 只是链刚被并发改过)。
    retry:
        uint32_t idx = buckets_[b].load(std::memory_order_acquire);
        while (idx != UINT32_MAX) {
            if (nodes_[idx].bucket != b) goto retry;   // 跳到别的桶 → 重试
            if (nodes_[idx].order_ref == order_ref)
                return nodes_[idx].order;
            idx = nodes_[idx].next_idx.load(std::memory_order_relaxed);
        }
        return nullptr;   // 正常走到链尾 → 真不存在
    }

    void erase(uint64_t order_ref) {
        uint32_t b = hash(order_ref);
        // 状态门禁: 已树化(2) → 锁 overflow_ 删。转化中(1) → 等锁后查。
        if (state_[b].load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lk(overflow_lock_);
            auto it = overflow_.find(order_ref);
            if (it != overflow_.end()) {
                overflow_.erase(it);
                size_.fetch_sub(1, std::memory_order_relaxed);
            }
            return;
        }

    retry:
        uint32_t idx = buckets_[b].load(std::memory_order_acquire);
        if (idx == UINT32_MAX) return;

        if (nodes_[idx].order_ref == order_ref) {   // 链头
            // CAS 桶头跳过本节点。失败 → 其他 worker 改了链头, idx 更新为新链头。
            uint32_t nxt = nodes_[idx].next_idx.load(std::memory_order_relaxed);
            while (!buckets_[b].compare_exchange_weak(idx, nxt,
                        std::memory_order_release, std::memory_order_acquire)) {
                if (nodes_[idx].order_ref != order_ref) goto retry;  // 新链头非 K → K 在链中, 重找
                nxt = nodes_[idx].next_idx.load(std::memory_order_relaxed);
            }
            freeNode(idx);
            bucket_len_[b].fetch_sub(1, std::memory_order_relaxed);
            size_.fetch_sub(1, std::memory_order_relaxed);
            return;
        }

        // 链中删除: 找前驱, CAS 前驱的 next 跳过本节点。
        // 前驱可能是别人的节点, 会被其 owner 并发 erase → freeNode 复用。bucket 校验:
        //   读到 bucket≠b 的前驱 = 已被复用跳桶 → 重试。CAS 返回值是唯一权威(不靠
        //   re-read 判相等, 那正是 ABA 来源)。
        while (idx != UINT32_MAX) {
            if (nodes_[idx].bucket != b) goto retry;   // 前驱被复用跳桶 → 重找
            uint32_t next = nodes_[idx].next_idx.load(std::memory_order_relaxed);
            if (next == UINT32_MAX) return;            // K 不在此链(极端, 重试兜底)
            if (nodes_[next].order_ref == order_ref) {
                uint32_t nxt2 = nodes_[next].next_idx.load(std::memory_order_relaxed);
                // CAS 前驱 next: idx→next 改为 idx→nxt2 (跳过本节点)。
                if (nodes_[idx].next_idx.compare_exchange_weak(
                        next, nxt2, std::memory_order_release, std::memory_order_relaxed)) {
                    freeNode(next);
                    bucket_len_[b].fetch_sub(1, std::memory_order_relaxed);
                    size_.fetch_sub(1, std::memory_order_relaxed);
                    return;
                }
                goto retry;   // CAS 失败: 前驱 next 被并发改, 重走链头
            }
            idx = next;
        }
    }

    bool contains(uint64_t order_ref) const {
        return find(order_ref) != nullptr;
    }

    size_t size() const { return size_.load(std::memory_order_relaxed); }

private:
    Node* const    nodes_;
    std::atomic<uint32_t>* const buckets_;   // 链头(无锁 CAS)
    std::atomic<uint16_t>* const bucket_len_; // 每桶链长(原子, 树化阈值判断, 允许近似)
    // 每桶转化状态(原子, 树化门禁): 0=链(可无锁操作), 1=转化中(treeify 复制链, 链操作须重试),
    //   2=已树化(所有操作锁 overflow_lock_ 走 overflow_)。0→1→2 单调, 链操作读到 ≠0 即重试进 map。
    std::atomic<uint8_t>* const state_;
    const uint32_t bucket_mask_;
    const uint32_t hash_shift_;
    const size_t   capacity_;
    // Treiber 无锁空闲栈, 带 tag(高32位)消除 ABA: 栈头存 (tag<<32)|idx, 每次成功
    // alloc/free 都 tag+1。节点归还后立即复用, 并发 alloc/free 时若无 tag, 一个线程
    // 读 (head=X,next=Y) 后另一线程 alloc+free X(X回栈顶), 前者的 CAS(X→Y) 仍成功
    // 但 Y 已非 X 的 next → 两个线程同时"拥有"X → 链上节点被覆盖。tag 保证 stale CAS 失败。
    std::atomic<uint64_t> free_head_ = UINT32_MAX;   // (tag<<32)|idx, idx 低32位
    std::atomic<size_t>   size_ = 0;
    mutable std::mutex overflow_lock_;        // 树化桶操作锁(STL map 内部不可控, 只能外部锁)
    mutable std::map<uint64_t, OrderSlot*> overflow_; // 树化桶共用(仅极少桶树化时用)

    static uint32_t roundPow2(size_t n) {
        size_t p = 1;
        while (p < n) p <<= 1;
        return static_cast<uint32_t>(p);
    }

    uint32_t hash(uint64_t id) const {
        // 乘黄金常数取高位，对任何 ID 分布都均匀
        return (id * 0x9E3779B97F4A7C15ULL) >> hash_shift_;
    }

    uint32_t allocNode() {
        // Treiber 无锁栈取节点(带 tag 消 ABA)。success = head 高32位(tag) 也匹配,
        // 节点被并发归还复用(栈头变过)时 tag 变化 → stale CAS 失败 → 重读重试。
        uint64_t fh = free_head_.load(std::memory_order_relaxed);
        while ((uint32_t)fh != UINT32_MAX) {
            uint32_t head = (uint32_t)fh;
            uint32_t next = nodes_[head].next_idx.load(std::memory_order_relaxed);
            uint64_t expected = fh;
            uint64_t desired = (((fh >> 32) + 1) << 32) | next;   // tag+1, idx=next
            if (free_head_.compare_exchange_weak(expected, desired,
                    std::memory_order_release, std::memory_order_relaxed))
                return head;
            fh = expected;   // CAS 失败, expected 已更新为实际栈头, 重试
        }
        return UINT32_MAX;
    }

    void freeNode(uint32_t idx) {
        uint64_t fh = free_head_.load(std::memory_order_relaxed);
        for (;;) {
            uint32_t head = (uint32_t)fh;
            nodes_[idx].next_idx.store(head, std::memory_order_relaxed);
            uint64_t expected = fh;
            uint64_t desired = (((fh >> 32) + 1) << 32) | idx;   // tag+1, idx=idx
            if (free_head_.compare_exchange_weak(expected, desired,
                    std::memory_order_release, std::memory_order_relaxed))
                return;
            fh = expected;   // CAS 失败, 重读栈头重试
        }
    }

    // 桶树化(桶长≥8, 6.6e-24 几乎不触发): CAS 0→1 抢占转化权(防并发触发),
    // 锁 overflow_lock_ 复制式迁移链节点进 overflow_ → 置 2(已树化) → 解锁。
    // 转化期间链操作读到 state_=1 会重试进 map(锁串行化), 故转化中无新链插入;
    // 桶链遗留孤儿节点不回收(树化几乎不触发, 可接受)。
    void treeify(uint32_t b) {
        uint8_t expect = 0;
        if (!state_[b].compare_exchange_strong(expect, 1,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
            return;   // 已被并发转化或已树化
        }
        std::lock_guard<std::mutex> lk(overflow_lock_);
        uint32_t cur = buckets_[b].load(std::memory_order_relaxed);
        while (cur != UINT32_MAX) {
            overflow_[nodes_[cur].order_ref] = nodes_[cur].order;
            cur = nodes_[cur].next_idx.load(std::memory_order_relaxed);
        }
        state_[b].store(2, std::memory_order_release);
    }
};
