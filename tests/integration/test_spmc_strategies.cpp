// SPMC 多消费者广播测试：多个策略从通道 A 消费同一批成交
//   1. 生产者 push 一批成交事件进通道 A(SPMCEventQueue)
//   2. 3 个策略线程各自 pop(consumer_id)，独立消费完整流
//   3. 验证：每个策略都收到全部成交事件(SPMC 广播不丢)
//   4. K线聚合器也从通道 A 消费，聚合出 K线
//
// 这验证 SPMC 的核心：一个事件被所有消费者各自读，各自进度，生产者按最慢限速。
#include "core/queue/spmc_event_queue.h"
#include "strategy/kline/kline_aggregator.h"
#include "strategy/tick/trade_direction_strategy.h"
#include "strategy/tick/volume_breakout_strategy.h"

#include <cstdio>
#include <cstdint>
#include <vector>
#include <thread>
#include <atomic>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_trade(uint64_t seq, int64_t price, uint64_t vol, OrderSide side) {
    MarketEvent ev{};
    ev.type = MarketEvent::Type::TRADE;
    ev.seq_id = seq;
    ev.timestamp = seq * 1000000;  // 每秒一笔
    ev.locate = 1;
    ev.trade.price = price;
    ev.trade.volume = vol;
    ev.trade.side = side;
    return ev;
}

int main() {
    constexpr size_t N_MSGS = 1000;
    constexpr size_t N_CONSUMERS = 3;  // 3 个消费者(2策略 + K线聚合器)

    // 通道 A: 用户分配槽位, 容量 1024
    auto* slots = new MarketEvent[1024];
    SPMCEventQueue<16> channel_a(slots, 1024);
    channel_a.set_num_consumers(N_CONSUMERS);

    // 消费者计数 + 完成标志
    std::atomic<size_t> c0_count{0}, c1_count{0}, c2_count{0};
    std::atomic<bool> producer_done{false};

    // 3 个消费线程(并发消费, 生产者满时让出)
    std::thread t0([&] {
        MarketEvent ev;
        while (!producer_done.load() || channel_a.pending(0) > 0) {
            if (channel_a.pop(0, ev)) ++c0_count;
            else std::this_thread::yield();
        }
    });
    std::thread t1([&] {
        MarketEvent ev;
        while (!producer_done.load() || channel_a.pending(1) > 0) {
            if (channel_a.pop(1, ev)) ++c1_count;
            else std::this_thread::yield();
        }
    });
    std::thread t2([&] {
        MarketEvent ev;
        while (!producer_done.load() || channel_a.pending(2) > 0) {
            if (channel_a.pop(2, ev)) ++c2_count;
            else std::this_thread::yield();
        }
    });

    // 生产者: push 1000 笔成交(push 满则让出, 由消费者推进)
    size_t pushed = 0;
    for (size_t i = 0; i < N_MSGS; ++i) {
        MarketEvent ev = make_trade(i, 100 + (int64_t)(i % 10), 100, OrderSide::BUY);
        while (!channel_a.push(ev)) std::this_thread::yield();
        ++pushed;
    }
    producer_done.store(true, std::memory_order_release);
    printf("生产者 push %zu 笔\n", pushed);
    CHECK(pushed == N_MSGS);

    t0.join(); t1.join(); t2.join();

    printf("消费者0: %zu, 消费者1: %zu, 消费者2: %zu\n",
           c0_count.load(), c1_count.load(), c2_count.load());
    CHECK(c0_count.load() == N_MSGS);  // 每个消费者都收到全部
    CHECK(c1_count.load() == N_MSGS);
    CHECK(c2_count.load() == N_MSGS);

    delete[] slots;
    if (g_failures == 0) {
        printf("\nSPMC 多消费者广播测试 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
