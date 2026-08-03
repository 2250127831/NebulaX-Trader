#pragma once

#include "core/types.h"

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
        uint32_t idx = free_head_;
        if (idx == UINT32_MAX) return nullptr;
        free_head_ = storage_[idx].pool_next_free;
        ++size_;
        return &storage_[idx];
    }

    void deallocate(uint32_t idx)
    {
        storage_[idx].pool_next_free = free_head_;
        free_head_ = idx;
        --size_;
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
    size_t size() const { return size_; }

    // 从头扫描 storage 重建空闲链表（崩溃恢复用，V2）
    void rebuildFreelist() {
        for (uint32_t i = 0; i < capacity_ - 1; i++)
            storage_[i].pool_next_free = i + 1;
        storage_[capacity_ - 1].pool_next_free = UINT32_MAX;
        free_head_ = 0;
        size_ = 0;
    }

private:
    void initFreeList() {
        for (uint32_t i = 0; i < capacity_ - 1; ++i)
            storage_[i].pool_next_free = i + 1;
        storage_[capacity_ - 1].pool_next_free = UINT32_MAX;
        free_head_ = 0;
        size_ = 0;
    }

    OrderSlot* const storage_;
    const size_t capacity_;
    bool owns_storage_ = true;
    uint32_t free_head_ = 0;
    size_t size_ = 0;
};
