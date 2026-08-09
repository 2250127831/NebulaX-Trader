#pragma once

#include "core/market_event.h"


#include <atomic>
#include <cstddef>
#include <cstdint>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

// ── SPSCEventRing: 单生产者单消费者定长槽事件环形队列 ──
//
// V3 分发器下游: 每 worker 一条, 解析器(单写) → worker(单读)。
// 与 SPMCEventQueue 的区别: 去掉 multi-consumer 的 heads_/min_head/blocked-mask/
//   cached_min_head 扫描——单消费者无"最慢消费者", 满 = head+capacity, 直接判。
//   SPMC 的锁判定(cached_min_head 遍历)是枷锁, 单消费者下拖累性能。
//
// 语义:
//   push(ev): 满返回 false(调用方决定重试/卸载到 retry)。成功写槽 + release tail。
//   pop(ev):  空返回 false(worker 循环混合退避)。成功读槽 + release head。
//   唤醒: 每队列 1 个 eventfd + blocked 标志。消费者阻塞前登记 blocked=true,
//         push 只写阻塞者的 fd(单消费者, 有阻塞才写, 无阻塞零 syscall)。
//
// 存储空间由用户传入(堆/共享内存), 队列不拥有。capacity 必须 2 的幂。
class SPSCEventRing {
public:
    SPSCEventRing(MarketEvent* slots, size_t capacity)
        : slots_(slots), capacity_(capacity), mask_(capacity - 1) {
        wake_fd_ = eventfd(0, EFD_NONBLOCK);
    }
    ~SPSCEventRing() {
        if (wake_fd_ >= 0) close(wake_fd_);
    }
    SPSCEventRing(const SPSCEventRing&) = delete;
    SPSCEventRing& operator=(const SPSCEventRing&) = delete;

    bool valid() const { return slots_ != nullptr && ((capacity_ & (capacity_ - 1)) == 0); }
    size_t capacity() const { return capacity_; }
    size_t pending() const {
        return tail_.load(std::memory_order_acquire) - head_.load(std::memory_order_acquire);
    }

    // ── producer(解析器) ──
    // 满返回 false。单写者无竞争, tail 单调。
    bool push(const MarketEvent& ev) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail - head_.load(std::memory_order_acquire) >= capacity_) return false;  // 满
        slots_[tail & mask_] = ev;
        tail_.store(tail + 1, std::memory_order_release);
        if (blocked_.load(std::memory_order_acquire)) {   // 有消费者阻塞才写 fd
            uint64_t one = 1;
            ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r;
        }
        return true;
    }

    // ── consumer(worker) ──
    // 空返回 false。单读者无竞争, head 单调。
    bool pop(MarketEvent& ev) {
        size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) return false;   // 空
        ev = slots_[head & mask_];
        head_.store(head + 1, std::memory_order_release);
        return true;
    }
    // peek 头(不推进 head)。retry 线程用: 先看头能否推 spsc, 能推才 pop(防取出发推不回的乱序)。
    bool peek(MarketEvent& ev) {
        size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) return false;   // 空
        ev = slots_[head & mask_];
        return true;
    }

    // 消费者阻塞前登记(之后 poll wake_fd 无限阻塞)。返回后可 poll 阻塞。
    void set_blocked() {
        blocked_.store(true, std::memory_order_release);
        // 登记后可能 push 已发生(竞态), 写一次 fd 兜底避免漏唤醒。
        uint64_t one = 1;
        ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r;
    }
    void set_active() { blocked_.store(false, std::memory_order_release); }

    int wake_fd() const { return wake_fd_; }

    // 唤醒阻塞的消费者(停止时用: 让 wait_for_data 返回, 检查退出条件)。
    void wake() {
        if (wake_fd_ >= 0) {
            uint64_t one = 1;
            ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r;
        }
    }

    // 阻塞等数据。返回 true=有唤醒(可能有数据), false=超时。
    bool wait_for_data(int timeout_ms = 1000) {
        struct pollfd pfd = {wake_fd_, POLLIN, 0};
        int ret = poll(&pfd, 1, timeout_ms);
        if (ret > 0) {
            uint64_t ev;
            ssize_t r = read(wake_fd_, &ev, sizeof(ev)); (void)r;   // 消费唤醒计数
            return true;
        }
        return false;
    }

private:
    MarketEvent* const slots_;
    const size_t capacity_;
    const size_t mask_;
    alignas(64) std::atomic<size_t> head_{0};       // 消费者推进
    alignas(64) std::atomic<size_t> tail_{0};       // 生产者推进
    alignas(64) std::atomic<bool> blocked_{false};  // 消费者是否阻塞(push 只写阻塞者)
    int wake_fd_ = -1;
};

// ── RetryBucket: 每下游 SPSC 一个待重试桶 ──
//   bucket: SPSCEventRing(解析器单写 / retry 线程单读)
//   active: 该 SPSC 是否有积压(桶非空)。active=true 时解析器必须进桶保序,
//           禁止直接 push spsc(可能乱序)。retry 清空桶后才置 false。
struct RetryBucket {
    explicit RetryBucket(MarketEvent* slots, size_t capacity) : bucket(slots, capacity) {}
    SPSCEventRing bucket;
    std::atomic<bool> active{false};
};
