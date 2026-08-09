#pragma once

#include "core/market_event.h"
#include "core/prof/lensx_probe.h"
#include "core/queue/spsc_event_ring.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <immintrin.h>   // _mm_pause (桶满背压)

// ── V3 分发器: 解析器 → 按 locate 分发到 N 条 SPSC + retry 桶 ──
//
// 单解析器(兼分发器)产出事件后:
//   owner = registry.lookup_or_register(ev.locate)   // locate → worker id
//   ├─ 直接 push spsc[owner] 成功 → 继续
//   └─ 满(false) → 塞入 retry_bucket[owner](每 SPSC 独立容量)
//       └ 该桶满才阻塞(只卡自己, 不拖累其他桶)
//
// 保序不变量(关键): 一旦 spsc[i] 在 retry 桶里有积压(active=true),
//   解析器对该 SPSC 的**所有**新事件必须进桶, 不能直接 push——
//   否则 worker 可能先消费后到的直接 push 事件, 乱序。
//   active 只在桶清空时清除 → 清除瞬间桶已全推, 后续直接 push 必在其后, 无乱序窗口。
//
//   worker 只 pop 自己的 spsc[i] → process, 无 skip、无 pop×4。
//
// 线程归属:
//   - dispatch(): 解析器线程(单写)。每个 spsc/桶 单写者。
//   - retry 线程: 桶的唯一读者(单读), 推 spsc。
//   - worker:     spsc 的唯一读者(单读)。
class Dispatcher {
public:
    static constexpr uint32_t kNone = UINT32_MAX;

    // spsc: N 条下游事件队列(每 worker 一条, 指针数组); retry: N 个待重试桶(指针数组)。
    // nworkers: worker 数。SPSCEventRing 含 eventfd 不可拷贝 → 用指针数组。
    Dispatcher(SPSCEventRing* const* spsc, RetryBucket* const* retry, size_t nworkers)
        : spsc_(spsc), retry_(retry), nworkers_(nworkers) {
        // owner_ 必须初始化为 kNone: atomic 默认构造不初始化(垃圾值), 否则 owner_[locate]
        // 恒 != kNone → 永不注册 → 所有事件归 worker0。V2 BookRegistry 有显式初始化。
        for (auto& a : owner_) a.store(kNone, std::memory_order_relaxed);
    }

    // 注册 locate → owner。返回 owner worker id。首次出现时注册到最清闲 worker。
    // 双键: cared_count 主键(处理事件数), registered_count 次键(破平局, 启动期轮转分散)。
    // 单解析器内联注册(无并发, V2.1 的 compare_exchange 竞争消失)。
    uint32_t lookup_or_register(uint32_t locate,
                                std::atomic<uint64_t>* cared,
                                std::atomic<uint64_t>* registered) {
        (void)cared;   // V3 生产者侧分发: 用 registered(无滞后), cared 留给 main 统计
        uint32_t cur = owner_[locate].load(std::memory_order_acquire);
        if (cur != kNone) return cur;
        // V3 分发在生产者侧(解析器): worker 处理数(cared)天然滞后(worker 处理到才涨),
        // 启动阶段全 0 → argmin 恒选 worker0, 负载失衡。改用 registered_count(已注册
        // locate 数)做主键——解析器实时可见、无滞后, 新 locate 轮流分散到各 worker。
        // (V2.1 的 cared 主键在消费者侧 skip 判定下成立, 生产者侧分发不适用。)
        uint32_t target = 0;
        uint64_t tr = registered[0].load(std::memory_order_relaxed);
        for (uint32_t i = 1; i < nworkers_; ++i) {
            uint64_t r = registered[i].load(std::memory_order_relaxed);
            if (r < tr) { target = i; tr = r; }
        }
        uint32_t expected = kNone;
        if (owner_[locate].compare_exchange_strong(expected, target,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
            registered[target].fetch_add(1, std::memory_order_relaxed);
            return target;
        }
        return expected;   // 别人注册了(先到先得, 单写者下几乎不发生)
    }

    // 分发一个事件到 owner 的 SPSC。满则进 retry 桶。
    // 保序: 桶 active(有积压)时事件必须进桶; 直接 push 成功则不入桶。
    void dispatch(const MarketEvent& ev,
                  std::atomic<uint64_t>* cared,
                  std::atomic<uint64_t>* registered) {
        uint32_t owner = lookup_or_register(static_cast<uint32_t>(ev.locate),
                                            cared, registered);
        // [LensX 消息级] 分发起点(parse_done→dispatch 段终点, 抽样)。
        if (ev.seq_id % lensx::kSample == 0) lensx::mark_dispatch(ev.seq_id);
        RetryBucket* rq = retry_[owner];
        if (rq->active.load(std::memory_order_acquire)) {
            // 桶有积压: 必须进桶保序(worker 还没消费完之前的, 直接 push 会乱序)。
            if (ev.seq_id % lensx::kSample == 0) lensx::mark_retry_in(ev.seq_id);
            while (!rq->bucket.push(ev)) _mm_pause();   // 桶满阻塞, 只卡该桶
            return;
        }
        if (spsc_[owner]->push(ev)) return;             // 直接推成功
        // 满 → 进桶 + 激活: 后续同 SPSC 事件必须进桶保序
        if (ev.seq_id % lensx::kSample == 0) lensx::mark_retry_in(ev.seq_id);
        while (!rq->bucket.push(ev)) _mm_pause();
        rq->active.store(true, std::memory_order_release);
    }

    // retry 线程: 桶 i 有积压吗(供 retry 决定 poll 哪些 fd)。
    bool bucket_pending(size_t i) const { return retry_[i]->bucket.pending() > 0; }

private:
    std::atomic<uint32_t> owner_[65536];   // locate → owner worker id(locate 16-bit)
    SPSCEventRing* const* spsc_;      // 指针数组(N 条下游队列)
    RetryBucket* const* retry_;       // 指针数组(N 个待重试桶)
    const size_t nworkers_;
};
