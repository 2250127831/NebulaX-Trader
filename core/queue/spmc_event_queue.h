#pragma once

#include "core/market_event.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <immintrin.h>   // _mm_pause (背压忙等)
#include <sys/eventfd.h>
#include <poll.h>
#include <unistd.h>

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
        : slots_(slots), capacity_(capacity), valid_((capacity & (capacity - 1)) == 0) {
        wake_fd_ = eventfd(0, EFD_NONBLOCK);   // 学撮合引擎: 生产写1唤醒, 消费poll阻塞
        if (wake_fd_ < 0) wake_fd_ = -1;
    }

    ~SPMCEventQueue() {
        if (wake_fd_ >= 0) close(wake_fd_);
    }

    // 默认构造：不绑定空间，需先 valid() 检查
    SPMCEventQueue() = default;
    SPMCEventQueue(const SPMCEventQueue&) = delete;
    SPMCEventQueue& operator=(const SPMCEventQueue&) = delete;

    bool valid() const { return valid_ && slots_ != nullptr; }
    size_t capacity() const { return capacity_; }

    // 生产者：写入一个事件。满时**尝试清理一次**, 仍满才返回 false。
    // 元素槽队列: "清理已消费" = tail 覆盖 seq < min_head 的槽位(顺序写, 隐式回收)。
    //   满 = 无已消费区可覆盖 → 尝试清理(一次检查) → 仍满返回 false(调用方决定)。
    // 平时快速检查 free(不遍历); 仅 free==0(满)才尝试清理(找已消费区可覆盖)。
    // 满 → 尝试清理一次(检查是否有已消费区) → 有则写入 / 仍满返回 false(调用方重试)。
    bool push(const MarketEvent& ev) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        // 快路径: 检查 free(最慢消费者进度)。空→非空才 notify, 避免每 push syscall。
        size_t min_head = min_consumed();   // 最慢消费者已消费到哪
        if (tail - min_head < capacity_) {   // 有空间(fast path)
            bool was_empty = (tail == min_head);
            slots_[tail & (capacity_ - 1)] = ev;
            tail_.store(tail + 1, std::memory_order_release);
            if (was_empty) notify_all();   // 学撮合引擎: 生产写1唤醒阻塞消费者
            return true;
        }
        // 满(free==0): 尝试清理一次——重读 min_head(消费者可能刚推进)。
        // 清理 = 已消费区(seq < min_head)可覆盖复用。仍满返回 false。
        min_head = min_consumed();
        if (tail - min_head < capacity_) {
            slots_[tail & (capacity_ - 1)] = ev;
            tail_.store(tail + 1, std::memory_order_release);
            return true;
        }
        return false;   // 尝试清理后仍满, 调用方决定重试/失败策略
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

    // 最慢消费者的已消费进度(决定可覆盖区)。生产 push 用。
    size_t min_consumed() const {
        size_t min_head = ~0ULL;
        for (size_t i = 0; i < num_consumers_; ++i) {
            size_t h = heads_[i].load(std::memory_order_acquire);
            if (h < min_head) min_head = h;
        }
        return min_head;
    }

    // ── eventfd 唤醒（学撮合引擎 poll + eventfd 方案）──
    // 消费者 pop 空时 poll(wake_fd) 阻塞，生产者 push 后 notify_all() 唤醒。
    // eventfd 累积计数：多次 notify 只产生一次唤醒，不丢。写一次全醒（广播）。

    // 生产者：写完数据后唤醒可能阻塞在 poll 的消费者。
    void notify_all() {
        if (wake_fd_ >= 0) {
            uint64_t one = 1;
            ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r;
        }
    }

    // 消费者：阻塞等数据。poll(wake_fd) 直到被唤醒（或 timeout_ms 超时）。
    // 返回 true 表示有唤醒（可能有数据），false 表示超时。
    bool wait_for_data(int timeout_ms = 1000) {
        if (wake_fd_ < 0) return false;
        struct pollfd pfd = {wake_fd_, POLLIN, 0};
        int ret = poll(&pfd, 1, timeout_ms);
        if (ret > 0) {
            uint64_t ev;
            ssize_t r = read(wake_fd_, &ev, sizeof(ev)); (void)r;  // 消费唤醒计数(EFD_NONBLOCK)
            return true;
        }
        return false;  // 超时或错误
    }

private:
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) std::atomic<size_t> heads_[MAX_CONSUMERS]{};
    size_t num_consumers_ = 1;
    MarketEvent* slots_ = nullptr;   // 用户传入的槽位数组（队列不拥有）
    size_t capacity_ = 0;            // 容量（运行时）
    bool   valid_ = false;           // 空间是否合法（2 的幂 + 已绑定）
    int    wake_fd_ = -1;            // eventfd 唤醒（学撮合引擎 poll+eventfd）
};
