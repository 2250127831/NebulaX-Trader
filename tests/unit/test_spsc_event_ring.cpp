// SPSCEventRing 单测：单生产者单消费者定长槽事件环形队列（V3 分发器下游）
//   - push/pop 基本 + 保序
//   - 满/空边界（容量 4，写入 4 个满，第 5 个 false）
//   - 跨回绕（写满一圈后继续，读回正确序）
//   - 单生产者线程推 N 事件 / 单消费者线程收 N 事件，零丢失零乱序
//   - 唤醒（消费者阻塞 poll，生产者 push 唤醒）
#include "core/queue/spsc_event_ring.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <immintrin.h>
#include <thread>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_ev(uint64_t seq, MarketEvent::Type type) {
    MarketEvent ev{};
    ev.seq_id = seq;
    ev.locate = seq;
    ev.type   = type;
    return ev;
}

int main() {
    // ── 基本 push/pop + 保序 ──
    MarketEvent* slots = new MarketEvent[4];
    SPSCEventRing q(slots, 4);

    CHECK(q.push(make_ev(1, MarketEvent::Type::ADD)));
    CHECK(q.push(make_ev(2, MarketEvent::Type::TRADE)));
    CHECK(q.push(make_ev(3, MarketEvent::Type::DELETE)));
    MarketEvent ev;
    CHECK(q.pop(ev) && ev.seq_id == 1 && ev.type == MarketEvent::Type::ADD);
    CHECK(q.pop(ev) && ev.seq_id == 2 && ev.type == MarketEvent::Type::TRADE);
    CHECK(q.pop(ev) && ev.seq_id == 3 && ev.type == MarketEvent::Type::DELETE);
    CHECK(!q.pop(ev));   // 空

    // ── 满边界 ──
    MarketEvent* slots2 = new MarketEvent[4];
    SPSCEventRing q2(slots2, 4);
    CHECK(q2.push(make_ev(1, MarketEvent::Type::ADD)));
    CHECK(q2.push(make_ev(2, MarketEvent::Type::ADD)));
    CHECK(q2.push(make_ev(3, MarketEvent::Type::ADD)));
    CHECK(q2.push(make_ev(4, MarketEvent::Type::ADD)));
    CHECK(!q2.push(make_ev(5, MarketEvent::Type::ADD)));   // 满
    CHECK(q2.pop(ev) && ev.seq_id == 1);
    CHECK(q2.push(make_ev(5, MarketEvent::Type::ADD)));    // 腾出后可写

    // ── 跨回绕: 写满一圈 + 读空 + 再写, 序正确 ──
    MarketEvent* slots3 = new MarketEvent[4];
    SPSCEventRing q3(slots3, 4);
    for (int i = 0; i < 4; ++i) CHECK(q3.push(make_ev(i + 1, MarketEvent::Type::ADD)));
    for (int i = 0; i < 4; ++i) { CHECK(q3.pop(ev) && ev.seq_id == (uint64_t)(i + 1)); }
    for (int i = 4; i < 8; ++i) CHECK(q3.push(make_ev(i + 1, MarketEvent::Type::ADD)));  // 回绕写
    for (int i = 4; i < 8; ++i) { CHECK(q3.pop(ev) && ev.seq_id == (uint64_t)(i + 1)); }

    // ── 单生产者 / 单消费者线程并发: N 事件零丢失零乱序 ──
    constexpr int N = 100000;
    MarketEvent* slots4 = new MarketEvent[1 << 16];
    SPSCEventRing q4(slots4, 1 << 16);
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) {}
        for (int i = 0; i < N; ++i)
            while (!q4.push(make_ev(i + 1, MarketEvent::Type::ADD))) _mm_pause();
        done.store(true, std::memory_order_release);
    });
    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) {}
        uint64_t expect = 1, got = 0;
        MarketEvent e;
        for (;;) {
            if (q4.pop(e)) {
                if (e.seq_id != expect) { printf("FAIL 乱序: got %llu expect %llu\n", (unsigned long long)e.seq_id, (unsigned long long)expect); ++g_failures; break; }
                ++expect; ++got;
                if (got == N) break;
            } else if (done.load(std::memory_order_acquire) && q4.pending() == 0) {
                break;   // 生产者结束且队列空
            }
        }
        CHECK(got == (uint64_t)N);
    });
    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();

    // ── 唤醒: 消费者阻塞 poll, 生产者 push 唤醒 ──
    MarketEvent* slots5 = new MarketEvent[4];
    SPSCEventRing q5(slots5, 4);
    std::atomic<bool> woke{false};
    std::thread blocker([&] {
        q5.set_blocked();
        woke.store(q5.wait_for_data(3000), std::memory_order_release);   // 等生产者 push
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));   // 确保 blocker 已 poll
    q5.push(make_ev(99, MarketEvent::Type::ADD));
    blocker.join();
    CHECK(woke.load());

    delete[] slots;
    delete[] slots2;
    delete[] slots3;
    delete[] slots4;
    delete[] slots5;

    if (g_failures == 0) {
        printf("SPSCEventRing 单测 PASS ✓\n");
        return 0;
    }
    printf("%d check(s) failed\n", g_failures);
    return 1;
}
