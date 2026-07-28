#pragma once

#include <atomic>
#include <cstddef>
#include <cstring>
#include <cstdint>

// ── SPMCByteRing: 单生产者多消费者的无锁环形缓冲区 ──
//
// 用途：行情网关写入 tick → 多个策略 Worker 并行消费
// 特点：写入一次，多个消费者各自读取不同的数据块
//       消费者之间无竞争（各读各的）
//       一个消费者慢不会阻塞其他消费者
//
// N 必须是 2 的幂。

template<size_t N>
class SPMCByteRing {
    static_assert((N & (N - 1)) == 0, "N must be a power of 2");

public:
    SPMCByteRing() = default;
    SPMCByteRing(const SPMCByteRing&) = delete;
    SPMCByteRing& operator=(const SPMCByteRing&) = delete;

    // ── producer ──
    // 由行情网关调用，写入 tick 数据
    bool push(const void* data, size_t len) {
        // 先读所有消费者的进度，找到最慢的一个
        size_t min_head = ~0ULL;
        for (size_t i = 0; i < num_consumers_; i++) {
            size_t h = heads_[i].load(std::memory_order_acquire);
            if (h < min_head) min_head = h;
        }

        size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail - min_head >= N) return false;  // 满了

        size_t mask = N - 1;
        size_t pos = tail & mask;
        size_t n1 = N - pos;
        if (n1 > len) n1 = len;
        if (n1 > 0) memcpy(buf_ + pos, data, n1);
        size_t n2 = len - n1;
        if (n2 > 0) memcpy(buf_, (const uint8_t*)data + n1, n2);

        tail_.store(tail + len, std::memory_order_release);
        return true;
    }

    // ── consumer ──
    // 由策略 Worker 调用，读取属于自己的 tick
    size_t pop(size_t consumer_id, void* buf, size_t len) {
        size_t tail = tail_.load(std::memory_order_acquire);
        size_t head = heads_[consumer_id].load(std::memory_order_relaxed);
        size_t avail = tail - head;
        if (avail == 0 || len == 0) return 0;

        size_t actual = (len < avail) ? len : avail;
        size_t mask = N - 1;
        size_t pos = head & mask;

        size_t n1 = N - pos;
        if (n1 > actual) n1 = actual;
        if (n1 > 0) memcpy(buf, buf_ + pos, n1);
        size_t n2 = actual - n1;
        if (n2 > 0) memcpy((uint8_t*)buf + n1, buf_, n2);

        heads_[consumer_id].store(head + actual, std::memory_order_release);
        return actual;
    }

    void set_num_consumers(size_t n) { num_consumers_ = n; }

private:
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) std::atomic<size_t> heads_[16]{};
    size_t num_consumers_ = 1;
    uint8_t buf_[N]{};
};
