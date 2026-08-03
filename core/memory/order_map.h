#pragma once

#include "core/types.h"

#include <cstddef>
#include <cstdint>
#include <map>

// ── order_ref → OrderSlot* 哈希表（迁移自 NebulaX order_map.h）──
// 分离链接法，底层池化管理，零堆分配（构造时预分配），
// 替代 std::unordered_map（下单路径高频查找 order_ref → 挂单）。
// 桶过长(≥8)时整体迁移到 std::map overflow（TREEIFY，防退化）。
class OrderMap {
    struct Node {
        uint64_t   order_ref;
        OrderSlot* order;
        uint32_t   next_idx;   // 链指针 / 空闲链表
    };

public:
    OrderMap(size_t capacity)
        : nodes_(new Node[capacity])
        , buckets_(new uint32_t[roundPow2(capacity)])
        , bucket_len_(new uint16_t[roundPow2(capacity)]())
        , bucket_mask_(roundPow2(capacity) - 1)
        , hash_shift_(64 - __builtin_ctz(bucket_mask_ + 1))
        , capacity_(capacity)
    {
        for (uint32_t i = 0; i < capacity_ - 1; ++i)
            nodes_[i].next_idx = i + 1;
        nodes_[capacity_ - 1].next_idx = UINT32_MAX;
        free_head_ = 0;

        for (uint32_t i = 0; i <= bucket_mask_; ++i)
            buckets_[i] = UINT32_MAX;
    }

    ~OrderMap() {
        delete[] nodes_;
        delete[] buckets_;
        delete[] bucket_len_;
    }

    OrderMap(const OrderMap&) = delete;
    OrderMap& operator=(const OrderMap&) = delete;

    static constexpr uint32_t TREEIFY_THRESHOLD = 8;

    void insert(uint64_t order_ref, OrderSlot* order) {
        uint32_t b = hash(order_ref);
        if (bucket_len_[b] >= TREEIFY_THRESHOLD || free_head_ == UINT32_MAX) {
            overflow_[order_ref] = order;
            ++size_;
            return;
        }

        uint32_t idx = allocNode();
        nodes_[idx].order_ref = order_ref;
        nodes_[idx].order     = order;
        nodes_[idx].next_idx  = buckets_[b];
        buckets_[b] = idx;
        ++bucket_len_[b];
        ++size_;

        if (bucket_len_[b] >= TREEIFY_THRESHOLD) {
            // 将整个 bucket 移入 overflow map（防退化）
            uint32_t cur = buckets_[b];
            while (cur != UINT32_MAX) {
                overflow_[nodes_[cur].order_ref] = nodes_[cur].order;
                uint32_t next = nodes_[cur].next_idx;
                freeNode(cur);
                cur = next;
            }
            buckets_[b] = UINT32_MAX;
        }
    }

    OrderSlot* find(uint64_t order_ref) const {
        uint32_t idx = buckets_[hash(order_ref)];
        while (idx != UINT32_MAX) {
            if (nodes_[idx].order_ref == order_ref)
                return nodes_[idx].order;
            idx = nodes_[idx].next_idx;
        }
        auto it = overflow_.find(order_ref);
        return (it != overflow_.end()) ? it->second : nullptr;
    }

    void erase(uint64_t order_ref) {
        auto it = overflow_.find(order_ref);
        if (it != overflow_.end()) {
            overflow_.erase(it);
            --size_;
            return;
        }

        uint32_t b = hash(order_ref);
        uint32_t idx = buckets_[b];
        if (idx == UINT32_MAX) return;

        if (nodes_[idx].order_ref == order_ref) {
            buckets_[b] = nodes_[idx].next_idx;
            freeNode(idx);
            --bucket_len_[b];
            --size_;
            return;
        }

        while (nodes_[idx].next_idx != UINT32_MAX) {
            uint32_t next = nodes_[idx].next_idx;
            if (nodes_[next].order_ref == order_ref) {
                nodes_[idx].next_idx = nodes_[next].next_idx;
                freeNode(next);
                --bucket_len_[b];
                --size_;
                return;
            }
            idx = next;
        }
    }

    bool contains(uint64_t order_ref) const {
        return find(order_ref) != nullptr;
    }

    size_t size() const { return size_; }

private:
    Node* const    nodes_;
    uint32_t* const buckets_;
    uint16_t* const bucket_len_;    // 每个 bucket 的链长
    const uint32_t bucket_mask_;
    const uint32_t hash_shift_;
    const size_t   capacity_;
    uint32_t       free_head_ = UINT32_MAX;
    size_t         size_ = 0;
    std::map<uint64_t, OrderSlot*> overflow_;

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
        uint32_t idx = free_head_;
        free_head_ = nodes_[idx].next_idx;
        return idx;
    }

    void freeNode(uint32_t idx) {
        nodes_[idx].next_idx = free_head_;
        free_head_ = idx;
    }
};
