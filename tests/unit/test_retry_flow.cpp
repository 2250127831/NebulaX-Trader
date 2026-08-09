// 并发 retry 端到端测试（V3 retry 桶正确性）
//   模拟真实场景: 解析器大量分发到小容量 SPSC(强制满) → 事件进 retry 桶
//   → 并发 retry 线程推回 SPSC → worker 消费
//   验证: 零丢(所有事件最终被消费) + 保序(同 locate 顺序正确)
//
// 线程模型:
//   [生产线程] 单线程 dispatch N 个事件(模拟解析器产出, 同 locate 占多)
//   [retry 线程] 从 retry 桶 peek → 推回 SPSC(能推才 pop, 保序)
//   [消费线程] 从 SPSC pop, 验证收到全部 N 个且同 locate 保序
#include "core/dispatch/dispatcher.h"
#include "core/queue/spsc_event_ring.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <immintrin.h>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_ev(uint64_t seq, uint64_t locate) {
    MarketEvent ev{};
    ev.seq_id = seq;
    ev.locate = locate;
    ev.type = MarketEvent::Type::ADD;
    return ev;
}

int main() {
    // ── 1 worker 分发器, 小容量 SPSC(64)强制触发满 → retry 桶 ──
    constexpr size_t CAP = 64;       // 小容量, 大量事件必触发满
    constexpr int N = 10000;         // 总事件数(远超容量, 强制大量走 retry)
    constexpr int NLOC = 5;          // 5 个 locate, 每个 2000 事件(验证同 locate 保序)

    int wake_fd = eventfd(0, EFD_NONBLOCK);
    auto* slots = new MarketEvent[CAP];
    SPSCEventRing spsc(slots, CAP, wake_fd);
    int rwake_fd = eventfd(0, EFD_NONBLOCK);
    auto* rslots = new MarketEvent[CAP * 4];   // retry 桶容量更大(吸收积压)
    RetryBucket retry(rslots, CAP * 4, rwake_fd);
    SPSCEventRing* spsc_arr[1] = {&spsc};
    RetryBucket* retry_arr[1] = {&retry};
    Dispatcher dispatcher(spsc_arr, retry_arr, 1);
    std::atomic<uint64_t> cared[1]{0};
    std::atomic<uint64_t> registered[1]{0};

    std::atomic<bool> prod_done{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> consumed{0};
    std::atomic<bool> seq_ok{true};

    // ── retry 线程: 从桶 peek → 推回 SPSC(能推才 pop, 保序) ──
    std::thread retry_th([&] {
        MarketEvent ev;
        while (!stop.load(std::memory_order_acquire)) {
            while (retry.bucket.peek(ev)) {
                if (!spsc.push(ev)) break;   // SPSC 满, 头留桶下轮
                retry.bucket.pop(ev);         // 推成功才取走
            }
            if (prod_done.load(std::memory_order_acquire) && retry.bucket.pending() == 0)
                break;
            _mm_pause();
        }
        // 排空: 生产结束且桶清空
        while (retry.bucket.peek(ev)) {
            if (!spsc.push(ev)) break;
            retry.bucket.pop(ev);
        }
    });

    // ── 消费线程: 从 SPSC pop, 验证零丢 + 同 locate 保序 ──
    // 退出条件: stop 且 SPSC 空 且 retry 桶空(所有事件都进过 SPSC 且被消费)。
    // 若 retry 桶还有积压, retry 线程还在推回, 提前退出会丢事件。
    std::thread consume_th([&] {
        MarketEvent ev;
        // 每 locate 的 last seq(验证同 locate 递增)
        uint64_t last_seq[NLOC] = {0};
        while (!stop.load(std::memory_order_acquire) ||
               spsc.pending() > 0 || retry.bucket.pending() > 0) {
            if (spsc.pop(ev)) {
                ++consumed;
                uint64_t l = ev.locate % NLOC;
                if (last_seq[l] != 0 && ev.seq_id <= last_seq[l]) {
                    seq_ok.store(false, std::memory_order_relaxed);   // 同 locate 乱序!
                    printf("FAIL 乱序: locate=%llu seq=%llu last=%llu\n",
                           (unsigned long long)ev.locate, (unsigned long long)ev.seq_id,
                           (unsigned long long)last_seq[l]);
                }
                last_seq[l] = ev.seq_id;
            } else if (!stop.load(std::memory_order_acquire) ||
                       retry.bucket.pending() > 0) {
                _mm_pause();   // 等 retry 推回
            }
        }
    });

    // ── 生产线程(主): 大量分发, 同 locate 连续(模拟重标 burst + 满) ──
    // 交替 5 个 locate, 每个 2000 条连续(同 locate 内部 seq 递增)
    for (int i = 0; i < N; ++i) {
        uint64_t locate = i % NLOC;
        uint64_t seq = locate * 100000 + (i / NLOC) + 1;   // 同 locate 内递增
        dispatcher.dispatch(make_ev(seq, locate), cared, registered);
    }
    prod_done.store(true, std::memory_order_release);
    // 等消费线程排空
    stop.store(true, std::memory_order_release);
    // 唤醒 retry/consume 可能阻塞处(push 无条件唤醒, 但确保它们看到 stop)
    spsc.wake();
    retry.bucket.wake();
    retry_th.join();
    consume_th.join();

    // ── 验证: 零丢 + 保序 ──
    CHECK(consumed.load() == N);
    CHECK(seq_ok.load());
    printf("消费=%llu 期望=%d 保序=%s\n",
           (unsigned long long)consumed.load(), N, seq_ok.load() ? "是" : "否");

    delete[] slots;
    delete[] rslots;
    close(wake_fd);
    close(rwake_fd);

    if (g_failures == 0) {
        printf("并发 retry 端到端测试 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
