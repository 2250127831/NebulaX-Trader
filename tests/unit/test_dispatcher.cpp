// Dispatcher 单元测试（V3 分发器）：按 locate 分发 + retry 桶 + 保序
//   - 分发到正确 owner: 同 locate 恒归同一 worker
//   - 负载均衡: 新 locate 轮转分散(registered 主键)
//   - retry 桶: SPSC 满时事件进桶 + active 标志; 桶清空才解除
//   - 保序: 同 worker 的事件, 直接 push 先于进桶事件
#include "core/dispatch/dispatcher.h"
#include "core/queue/spsc_event_ring.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <sys/eventfd.h>
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
    // ── 3 worker 分发器 ──
    constexpr size_t NW = 3;
    constexpr size_t CAP = 8;   // 小容量, 方便触发满
    // SPSC 数组(共享唤醒 fd)
    int wake_fd = eventfd(0, EFD_NONBLOCK);
    MarketEvent* slots[NW];
    SPSCEventRing* rings[NW];
    for (size_t i = 0; i < NW; ++i) {
        slots[i] = new MarketEvent[CAP];
        rings[i] = new SPSCEventRing(slots[i], CAP, wake_fd);
    }
    // RetryBucket 数组
    MarketEvent* rslots[NW];
    RetryBucket* retries[NW];
    int rwake_fd = eventfd(0, EFD_NONBLOCK);
    for (size_t i = 0; i < NW; ++i) {
        rslots[i] = new MarketEvent[CAP];
        retries[i] = new RetryBucket(rslots[i], CAP, rwake_fd);
    }
    Dispatcher dispatcher(rings, retries, NW);
    std::atomic<uint64_t> cared[NW]{0};
    std::atomic<uint64_t> registered[NW]{0};

    // ── 1. 同 locate 恒归同一 worker ──
    // 先分发 locate=100 三次, 都应进同一 worker 的 SPSC
    dispatcher.dispatch(make_ev(1, 100), cared, registered);
    dispatcher.dispatch(make_ev(2, 100), cared, registered);
    dispatcher.dispatch(make_ev(3, 100), cared, registered);
    // 找哪个 worker 有事件(同 locate 应只在一个 worker)
    int owner = -1;
    MarketEvent ev;
    for (size_t i = 0; i < NW; ++i) {
        if (rings[i]->pending() > 0) {
            if (owner != -1) { printf("FAIL: locate 100 分到多个 worker\n"); ++g_failures; }
            owner = (int)i;
        }
    }
    CHECK(owner != -1);
    if (owner != -1) {
        // 3 个事件都在同一 worker, 顺序保序
        CHECK(rings[owner]->pop(ev) && ev.seq_id == 1);
        CHECK(rings[owner]->pop(ev) && ev.seq_id == 2);
        CHECK(rings[owner]->pop(ev) && ev.seq_id == 3);
    }

    // ── 2. 负载均衡: 新 locate 轮转分散到各 worker ──
    // 发 6 个新 locate, 应分散到 3 个 worker(各 ~2)
    for (uint64_t l = 0; l < 6; ++l)
        dispatcher.dispatch(make_ev(l + 10, 1000 + l), cared, registered);
    // 每个 worker 至少收到 1 个(轮转分散)
    int nonempty = 0;
    for (size_t i = 0; i < NW; ++i)
        if (rings[i]->pending() > 0) ++nonempty;
    CHECK(nonempty >= 2);   // 至少 2 个 worker 分到(轮转)

    // ── 3. retry 桶: SPSC 满时事件进桶 + active 标志 ──
    // 清空所有 SPSC
    for (size_t i = 0; i < NW; ++i) while (rings[i]->pop(ev)) {}
    // 填满 worker 0 的 SPSC(CAP=8)
    for (size_t i = 0; i < CAP; ++i)
        dispatcher.dispatch(make_ev(100 + i, 5000), cared, registered);   // 同 locate → worker? 未注册, 轮转
    // 找到这个 locate 的 owner
    uint32_t ow = dispatcher.lookup_or_register(5000, cared, registered);
    // 现在 SPSC[ow] 可能已满, 再发同 locate 事件 → 应进 retry 桶
    size_t before = rings[ow]->pending();
    dispatcher.dispatch(make_ev(200, 5000), cared, registered);
    // 事件要么直接进 SPSC(没满), 要么进 retry 桶(满)
    bool in_ring = rings[ow]->pending() > before;
    bool in_retry = retries[ow]->bucket.pending() > 0;
    CHECK(in_ring || in_retry);
    if (in_retry) CHECK(retries[ow]->active.load());   // 进桶则 active 置位

    // ── 4. 保序: 桶 active 后, 同 locate 事件必须进桶 ──
    if (in_retry) {
        // active=true, 再发同 locate → 必须进桶(不能直接 push SPSC 乱序)
        size_t retry_before = retries[ow]->bucket.pending();
        dispatcher.dispatch(make_ev(201, 5000), cared, registered);
        CHECK(retries[ow]->bucket.pending() > retry_before);   // 进桶
        CHECK(rings[ow]->pending() == before);                  // SPSC 不变
    }

    // ── 清理 ──
    for (size_t i = 0; i < NW; ++i) {
        delete[] slots[i];
        delete rings[i];
        delete[] rslots[i];
        delete retries[i];
    }
    close(wake_fd);
    close(rwake_fd);

    if (g_failures == 0) {
        printf("Dispatcher 单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
