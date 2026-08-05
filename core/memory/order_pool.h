#pragma once

#include "core/types.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

// ── 固定容量 OrderSlot 池（迁移自 NebulaX order_pool.h）──
// 单块连续数组，OrderSlot* 永远稳定，at() 零开销，空闲链表管理。
// 零堆分配（构造时一次分配，运行期 allocate/deallocate 只改链表头）。
// 用于高性能订单簿的挂单存储。
class OrderPool {
public:
    explicit OrderPool(size_t capacity)
        : storage_(new OrderSlot[capacity])
        , capacity_(capacity)
    {
        initFreeList();
    }

    // 使用外部 mmap 存储（共享内存，V2）
    OrderPool(OrderSlot* external_storage, size_t capacity, bool init_free)
        : storage_(external_storage)
        , capacity_(capacity)
        , owns_storage_(false)
    {
        if (init_free) initFreeList();
    }

    ~OrderPool() { if (owns_storage_) delete[] storage_; }

    OrderPool(const OrderPool&) = delete;
    OrderPool& operator=(const OrderPool&) = delete;

    OrderSlot* allocate()
    {
        // Treiber 无锁空闲栈(带 tag 消 ABA): 槽归还后立即复用, 并发 alloc/free 时
        // 无 tag 会因 ABA 让两个线程同时"拥有"同一槽。tag 随每次成功 CAS 递增, stale CAS 失败。
        uint64_t fh = free_head_.load(std::memory_order_relaxed);
        while ((uint32_t)fh != UINT32_MAX) {
            uint32_t head = (uint32_t)fh;
            uint32_t next = storage_[head].pool_next_free;
            uint64_t expected = fh;
            uint64_t desired = (((fh >> 32) + 1) << 32) | next;
            if (free_head_.compare_exchange_weak(expected, desired,
                    std::memory_order_release, std::memory_order_relaxed)) {
                size_.fetch_add(1, std::memory_order_relaxed);
                return &storage_[head];     // CAS 成功 → 取走头部
            }
            fh = expected;   // CAS 失败, 重读栈头重试
        }
        return nullptr;                     // 空池
    }

    void deallocate(uint32_t idx)
    {
        // Treiber 无锁空闲栈(带 tag 消 ABA): CAS 循环压栈顶。
        uint64_t fh = free_head_.load(std::memory_order_relaxed);
        for (;;) {
            uint32_t head = (uint32_t)fh;
            storage_[idx].pool_next_free = head;   // 新节点指向当前头
            uint64_t expected = fh;
            uint64_t desired = (((fh >> 32) + 1) << 32) | idx;
            if (free_head_.compare_exchange_weak(expected, desired,
                    std::memory_order_release, std::memory_order_relaxed)) {
                size_.fetch_sub(1, std::memory_order_relaxed);
                return;
            }
            fh = expected;   // CAS 失败, 重读栈头重试
        }
    }

    void deallocate(OrderSlot* ptr)
    {
        if (!ptr) return;
        deallocate(static_cast<uint32_t>(ptr - storage_));
    }

    uint32_t indexOf(const OrderSlot* ptr) const
    {
        return static_cast<uint32_t>(ptr - storage_);
    }

    OrderSlot* at(uint32_t idx) { return &storage_[idx]; }
    const OrderSlot* at(uint32_t idx) const { return &storage_[idx]; }
    size_t capacity() const { return capacity_; }
    size_t size() const { return size_.load(std::memory_order_relaxed); }

    // 从头扫描 storage 重建空闲链表（崩溃恢复用，V2）
    void rebuildFreelist() {
        for (uint32_t i = 0; i < capacity_ - 1; i++)
            storage_[i].pool_next_free = i + 1;
        storage_[capacity_ - 1].pool_next_free = UINT32_MAX;
        free_head_.store(0, std::memory_order_release);
        size_.store(0, std::memory_order_relaxed);
    }

private:
    void initFreeList() {
        for (uint32_t i = 0; i < capacity_ - 1; ++i)
            storage_[i].pool_next_free = i + 1;
        storage_[capacity_ - 1].pool_next_free = UINT32_MAX;
        free_head_.store(0, std::memory_order_release);
        size_.store(0, std::memory_order_relaxed);
    }

    OrderSlot* const storage_;
    const size_t capacity_;
    bool owns_storage_ = true;
    std::atomic<uint64_t> free_head_ = 0;   // Treiber 无锁栈头 (tag<<32)|idx, tag 消 ABA
    std::atomic<size_t>   size_ = 0;        // 统计用(原子, 并发安全)
};
