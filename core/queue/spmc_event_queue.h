#pragma once

#include "core/market_event.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

// ── SPMC 定长槽位事件队列（多消费者广播）──
// 单生产者 → 多消费者，无锁。每个槽位放一个 MarketEvent（定长结构体）。
//
// 用途：第 2 级通道。解析器把变长 ITCH 二进制转换成定长 MarketEvent 后，
//   一个事件广播给多个消费者（策略 / 订单簿 / OFI）。
//
// 多消费者机制（参考 SPMCByteRing）：
//   - 每个消费者维护自己的进度 heads_[consumer_id]
//   - pop(id, ev)：读到即推进该消费者进度（"读到"= 可跳过不处理）
//   - push(ev)：按最慢消费者进度判断空间，满了返回 false（等最慢消费者）
//   - 一个事件被所有消费者读过（最慢的也读了），生产者才能覆盖 → 释放
//
// 为什么不关心就跳过：消费者读到事件但不处理（推进进度即弃），
//   生产者仍能覆盖该槽位，因为该消费者的进度已推进。
//
// capacity 必须是 2 的幂（构造时校验）；MAX_CONSUMERS 是最大消费者数（编译期）。
// 存储空间由用户传入（堆分配 / 共享内存），队列不拥有。
//
// 用法：
//   MarketEvent* slots = new MarketEvent[1 << 16];
//   SPMCEventQueue<16> q(slots, 1 << 16);
//   q.set_num_consumers(3);
template <size_t MAX_CONSUMERS = 16>
class SPMCEventQueue {
    static_assert(MAX_CONSUMERS > 0, "need at least 1 consumer");

public:
    // 用户传入槽位数组 + 容量。capacity 必须 2 的幂（不满足返回 false，需检查 valid()）。
    SPMCEventQueue(MarketEvent* slots, size_t capacity)
        : slots_(slots), capacity_(capacity), valid_((capacity & (capacity - 1)) == 0) {}

    // 默认构造：不绑定空间，需先 valid() 检查
    SPMCEventQueue() = default;
    SPMCEventQueue(const SPMCEventQueue&) = delete;
    SPMCEventQueue& operator=(const SPMCEventQueue&) = delete;

    bool valid() const { return valid_ && slots_ != nullptr; }
    size_t capacity() const { return capacity_; }

    // 生产者：写入一个事件。队列满（最慢消费者未消费）返回 false。
    bool push(const MarketEvent& ev) {
        // 找最慢消费者的进度：它决定能写多少
        size_t min_head = ~0ULL;
        for (size_t i = 0; i < num_consumers_; ++i) {
            size_t h = heads_[i].load(std::memory_order_acquire);
            if (h < min_head) min_head = h;
        }
        const size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail - min_head >= capacity_) return false;  // 最慢消费者还没消费够

        slots_[tail & (capacity_ - 1)] = ev;
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // 消费者：读取一个事件。读到即推进该消费者进度（可跳过不处理）。
    // 队列空（该消费者已追上生产者）返回 false。
    bool pop(size_t consumer_id, MarketEvent& ev) {
        const size_t tail = tail_.load(std::memory_order_acquire);
        const size_t head = heads_[consumer_id].load(std::memory_order_relaxed);
        if (tail == head) return false;  // 该消费者已消费完

        ev = slots_[head & (capacity_ - 1)];
        heads_[consumer_id].store(head + 1, std::memory_order_release);
        return true;
    }

    // 消费者：读到但不处理（跳过）。只推进进度，不返回事件。
    // 等价于"读到即跳过"，用于消费者不关心的类型。
    void skip(size_t consumer_id) {
        MarketEvent tmp;
        (void)pop(consumer_id, tmp);
    }

    void set_num_consumers(size_t n) { num_consumers_ = n; }
    size_t num_consumers() const { return num_consumers_; }

    // 该消费者尚未消费的事件数
    size_t pending(size_t consumer_id) const {
        return tail_.load(std::memory_order_relaxed) -
               heads_[consumer_id].load(std::memory_order_relaxed);
    }

private:
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) std::atomic<size_t> heads_[MAX_CONSUMERS]{};
    size_t num_consumers_ = 1;
    MarketEvent* slots_ = nullptr;   // 用户传入的槽位数组（队列不拥有）
    size_t capacity_ = 0;            // 容量（运行时）
    bool   valid_ = false;           // 空间是否合法（2 的幂 + 已绑定）
};
