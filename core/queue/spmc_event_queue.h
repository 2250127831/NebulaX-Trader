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
// ── 高性能优化（对齐 SPMC Ring 设计文档）──
// 1. Lazy Progress Update：消费者 pop 本地攒 kLazyBatch 个才 publish 一次
//    heads_[consumer_id]，降低高频 atomic 写 + cache line bouncing。
//    正确性：flush 在 pending==0 时由消费者补发（无数据时补齐进度，生产者
//    看到真实进度）；batch 边界最多滞后 kLazyBatch 条，容量大时可接受。
//    新增 progress_flush(id)：消费者在空循环处调用，补发本地进度。
// 2. Cache Line Padding：heads_[MAX_CONSUMERS] 每个消费者 alignas(64) 独占
//    一行，消除多消费者 false sharing。
// 3. Batch Reclaim：push 满时重读 min_consumed() 批量回收已消费区（一次
//    重读多个消费者进度），减少自旋重试次数。
//
// capacity 必须是 2 的幂（构造时校验）；MAX_CONSUMERS 是最大消费者数（编译期）。
// 存储空间由用户传入（堆分配 / 共享内存），队列不拥有。
//
// 用法：
//   MarketEvent* slots = new MarketEvent[1 << 16];
//   SPMCEventQueue<16> q(slots, 1 << 16);
//   q.set_num_consumers(3);
//   消费者循环：while (q.pop(id, ev)) { ... } q.progress_flush(id); // 空时补发
template <size_t MAX_CONSUMERS = 16>
class SPMCEventQueue {
    static_assert(MAX_CONSUMERS > 0, "need at least 1 consumer");

public:
    // 消费者本地攒批阈值：攒够 kLazyBatch 个才 publish 一次进度。
    static constexpr size_t kLazyBatch = 64;

    // 用户传入槽位数组 + 容量。capacity 必须 2 的幂（不满足返回 false，需检查 valid()）。
    SPMCEventQueue(MarketEvent* slots, size_t capacity)
        : slots_(slots), capacity_(capacity), valid_((capacity & (capacity - 1)) == 0) {
        // 每个消费者独立 eventfd + 阻塞掩码(blocked-mask): 消费者阻塞前登记位,
        // push 只写阻塞者的 fd。每消费者 poll 自己的 fd, 谁都不漏唤醒。
        for (size_t i = 0; i < MAX_CONSUMERS; ++i) {
            wake_fds_[i] = eventfd(0, EFD_NONBLOCK);
            if (wake_fds_[i] < 0) wake_fds_[i] = -1;
        }
    }

    ~SPMCEventQueue() {
        for (size_t i = 0; i < MAX_CONSUMERS; ++i)
            if (wake_fds_[i] >= 0) close(wake_fds_[i]);
    }

    // 默认构造：不绑定空间，需先 valid() 检查
    SPMCEventQueue() {
        for (size_t i = 0; i < MAX_CONSUMERS; ++i) wake_fds_[i] = -1;
    }
    SPMCEventQueue(const SPMCEventQueue&) = delete;
    SPMCEventQueue& operator=(const SPMCEventQueue&) = delete;

    bool valid() const { return valid_ && slots_ != nullptr; }
    size_t capacity() const { return capacity_; }

    // 生产者：写入一个事件。满时**批量回收一次**, 仍满才返回 false。
    // 元素槽队列: "清理已消费" = tail 覆盖 seq < min_head 的槽位(顺序写, 隐式回收)。
    //   满 = 无已消费区可覆盖 → 批量回收(重读 min_head) → 仍满返回 false(调用方决定)。
    //
    // Lazy Reclaim(cached min_head): 生产者私有缓存缓存最慢消费者进度。
    //   平时 push 用缓存判断空间(零遍历); 仅缓存判断空间不足才扫描 min_consumed() 更新。
    //   正确性: 缓存只可能滞后(消费者进度单调增, 缓存是上次扫描值), 滞后 → 假满 → 多扫一次,
    //   不丢数据。消除每次 push 的 O(consumers) 原子遍历(5M/s 吞吐下是每秒千万次原子读)。
    //
    // 唤醒: 每次成功 push 后查 blocked 掩码——有消费者阻塞(在 poll)才写其 fd(广播),
    //   无阻塞零 syscall。**不按"空→非空"门控**: 一个消费者阻塞(pending==0)时,
    //   其它消费者可能还落后, 队列未必全局空; 空转换门控会漏唤醒 → 无限阻塞的消费者饿死。
    bool push(const MarketEvent& ev) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        // 快路径: 用缓存的最慢消费者进度判断空间, 不遍历。
        if (tail - cached_min_head_ < capacity_) {   // 有空间(fast path)
            slots_[tail & (capacity_ - 1)] = ev;
            tail_.store(tail + 1, std::memory_order_release);
            notify_all();   // 有阻塞消费者才写其 fd(广播), 无则零 syscall
            return true;
        }
        // 缓存判断空间不足: 扫描一次 min_consumed() 更新缓存(消费者可能已推进)。
        cached_min_head_ = min_consumed();
        if (tail - cached_min_head_ < capacity_) {
            slots_[tail & (capacity_ - 1)] = ev;
            tail_.store(tail + 1, std::memory_order_release);
            notify_all();
            return true;
        }
        return false;   // 扫描后仍满, 调用方决定重试/失败策略
    }

    // 消费者：读取一个事件。读到即推进该消费者进度（可跳过不处理）。
    // Lazy Progress：本地攒 kLazyBatch 个才 publish 一次 heads_。
    // 队列空（该消费者已追上生产者）返回 false。
    bool pop(size_t consumer_id, MarketEvent& ev) {
        const size_t tail = tail_.load(std::memory_order_acquire);
        LocalState& ls = locals_[consumer_id];
        const size_t head = ls.head.load(std::memory_order_relaxed);
        if (tail == head) {
            progress_flush(consumer_id);   // 追平生产者, 补发本地进度(见 flush)
            return false;
        }

        ev = slots_[head & (capacity_ - 1)];
        size_t new_head = head + 1;
        ls.head.store(new_head, std::memory_order_relaxed);   // 本地推进(低开销)
        // 攒够批量 或 追平生产者 → 发布本地进度(避免生产者看到滞后进度误判满)。
        if (++ls.pending_publish >= kLazyBatch || new_head == tail) {
            progress_flush(consumer_id);
        }
        return true;
    }

    // 消费者：读到但不处理（跳过）。只推进进度，不返回事件。
    void skip(size_t consumer_id) {
        MarketEvent tmp;
        (void)pop(consumer_id, tmp);
    }

    // 消费者：把本地攒的进度发布到全局 heads_。空循环处调用（无数据时补齐进度，
    // 生产者 min_consumed() 才能看到真实进度, 避免误判满）。
    void progress_flush(size_t consumer_id) {
        LocalState& ls = locals_[consumer_id];
        size_t h = ls.head.load(std::memory_order_relaxed);
        size_t published = heads_[consumer_id].load(std::memory_order_relaxed);
        if (h != published) {
            heads_[consumer_id].store(h, std::memory_order_release);
        }
        ls.pending_publish = 0;
    }

    void set_num_consumers(size_t n) { num_consumers_ = n; }
    size_t num_consumers() const { return num_consumers_; }

    // 该消费者尚未消费的事件数。注意 lazy progress 下本地已消费但未发布的部分
    // 会计入 pending（尾部 - 已发布进度），消费者循环靠 pop 返回 false 判断空，
    // 不必依赖 pending 的精确值。
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

    // ── eventfd 广播唤醒（blocked-mask: 广播必唤醒, 无唤醒竞争）──
    // 每个消费者独立 eventfd + 原子阻塞掩码:
    //   消费者阻塞前在 blocked_ 登记自己的位, 才 poll 自己的 fd(无限阻塞)。
    //   生产者每次 push 后查 blocked_: 有阻塞者才写对应 fd, 无则零 syscall。
    // 解决两个历史问题:
    //   1) 共享单 fd: notify 计数被一个消费者消费后其它漏唤醒 → 200ms 超时兜底 → 101ms 延迟
    //   2) "空→非空"门控 notify(旧): 消费者已阻塞(pending==0)但其它消费者落后时,
    //      队列未全局空 → 漏唤醒 → 无限阻塞饿死(旧 200ms 超时恰好兜底了这个竞态)
    // 现在: 登记位 → 重查(注册间隙有数据则不阻塞) → poll; push 见位必写 fd, 广播必唤醒。

    // 生产者：push 后广播唤醒所有登记为阻塞的消费者(无阻塞零 syscall)。
    void notify_all() {
        uint32_t m = blocked_.load(std::memory_order_seq_cst);
        if (m == 0) return;
        uint64_t one = 1;
        for (size_t i = 0; i < num_consumers_; ++i)
            if ((m & (1u << i)) && wake_fds_[i] >= 0) {
                ssize_t r = write(wake_fds_[i], &one, sizeof(one)); (void)r;
            }
    }

    // 消费者(consumer_id)：阻塞等数据。阻塞前登记 blocked 位, 再 poll 自己的 fd(无限)。
    // 登记后重查 pending: 注册间隙已有数据 → 不阻塞直接返回(生产者对空 fd 的写无害)。
    // 唤醒保证(seq_cst 全序 + 重查, 两种交错都不饿死):
    //   - 生产者查 blocked_ 在登记之后 → 看到位 → 写 fd → poll 返回
    //   - 生产者查 blocked_ 在登记之前(没写) → 登记发生在 push 之后,
    //     重查在登记之后(happens-before 链)必然看到该 push 的数据 → 也不阻塞
    // 若 fd 无效退化为忙等返回 false。
    bool wait_for_data(size_t consumer_id) {
        if (consumer_id >= MAX_CONSUMERS || wake_fds_[consumer_id] < 0) return false;
        const uint32_t bit = 1u << consumer_id;
        blocked_.fetch_or(bit, std::memory_order_seq_cst);   // 登记阻塞(先于重查)
        if (pending(consumer_id) > 0) {   // 登记间隙有数据 → 不阻塞, 直接处理
            blocked_.fetch_and(~bit, std::memory_order_seq_cst);
            return true;
        }
        struct pollfd pfd = {wake_fds_[consumer_id], POLLIN, 0};
        int ret = poll(&pfd, 1, -1);   // 无限阻塞: 有阻塞消费者被 push 写 fd 才醒
        blocked_.fetch_and(~bit, std::memory_order_seq_cst);   // 醒来后注销
        if (ret > 0) {
            uint64_t ev;
            ssize_t r = read(wake_fds_[consumer_id], &ev, sizeof(ev)); (void)r;  // 消费计数
            return true;
        }
        return false;
    }

private:
    // 消费者本地状态: head 是本地消费游标(未发布), pending_publish 是攒批计数。
    // alignas(64) 独占 cache line, 与全局 heads_ 隔离, 消除 false sharing。
    struct alignas(64) LocalState {
        std::atomic<size_t> head{0};        // 本地消费进度(未发布到 heads_)
        size_t pending_publish = 0;         // 本地已消费但未发布的计数(仅本线程读写)
    };

    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) std::atomic<size_t> heads_[MAX_CONSUMERS]{};  // 全局发布进度(消费者 flush)
    LocalState locals_[MAX_CONSUMERS];      // 本地攒批(每消费者独立 cache line)
    // 生产者私有缓存: 最近一次扫描的 min_consumed()。thread_local → 每个解析器线程
    // 一份, 多生产者(V2.3 N 个解析器都 push)下无跨线程竞争。滞后 → 假满 → 多扫一次。
    static thread_local size_t cached_min_head_;
    size_t num_consumers_ = 1;
    MarketEvent* slots_ = nullptr;   // 用户传入的槽位数组（队列不拥有）
    size_t capacity_ = 0;            // 容量（运行时）
    bool   valid_ = false;           // 空间是否合法（2 的幂 + 已绑定）
    int    wake_fds_[MAX_CONSUMERS]; // 每消费者独立 eventfd(广播唤醒, 避免唤醒竞争)
    std::atomic<uint32_t> blocked_{0};  // 阻塞消费者掩码(bit=消费者id): push 只写阻塞者的 fd
};

// thread_local 静态成员定义(每个解析器线程一份)
template <size_t C>
thread_local size_t SPMCEventQueue<C>::cached_min_head_ = 0;
