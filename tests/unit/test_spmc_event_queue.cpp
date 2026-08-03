// SPMCEventQueue 单测：多消费者各自进度 + 最慢限速 + 跳过机制
//   - 两个消费者各自读，进度独立
//   - push 按最慢消费者限速（最慢没消费，生产者满）
//   - skip：消费者读到不处理，仍推进进度（生产者能覆盖）
#include "core/queue/spmc_event_queue.h"

#include <cstdio>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static MarketEvent make_ev(uint64_t locate, MarketEvent::Type type) {
    MarketEvent ev{};
    ev.locate = locate;
    ev.type   = type;
    return ev;
}

int main() {
    // 容量 4，2 个消费者。槽位数组由用户分配。
    MarketEvent* slots = new MarketEvent[4];
    SPMCEventQueue<16> q(slots, 4);
    q.set_num_consumers(2);

    // ── 多消费者各自进度 ──
    // 生产者写入 4 个事件（填满，最慢消费者没消费则满）
    CHECK(q.push(make_ev(1, MarketEvent::Type::ADD)));
    CHECK(q.push(make_ev(2, MarketEvent::Type::ADD)));
    CHECK(q.push(make_ev(3, MarketEvent::Type::ADD)));
    CHECK(q.push(make_ev(4, MarketEvent::Type::ADD)));
    CHECK(!q.push(make_ev(5, MarketEvent::Type::ADD)));  // 满：最慢消费者没消费

    // 消费者 0 读 1 个，消费者 1 读 2 个 → 各自进度独立
    MarketEvent ev;
    CHECK(q.pop(0, ev));
    CHECK(ev.locate == 1);
    CHECK(q.pop(1, ev));
    CHECK(ev.locate == 1);
    CHECK(q.pop(1, ev));
    CHECK(ev.locate == 2);
    printf("消费者0 进度: %zu, 消费者1 进度: %zu\n",
           q.pending(0), q.pending(1));
    CHECK(q.pending(0) == 3);  // 消费者0 还有 3 个未读
    CHECK(q.pending(1) == 2);  // 消费者1 还有 2 个未读

    // ── 最慢限速：消费者0 最慢(进度1)，消费者1 快(进度3) ──
    // 生产者能再写 1 个(容量4，最慢=head0=1，tail=4，tail-min=3<4 可写1)
    CHECK(q.push(make_ev(5, MarketEvent::Type::ADD)));
    CHECK(!q.push(make_ev(6, MarketEvent::Type::ADD)));  // 满：最慢(消费者0)没消费

    // 消费者0 读到尾，生产者才能继续
    CHECK(q.pop(0, ev));  // 消费第2个
    CHECK(q.pop(0, ev));  // 消费第3个
    CHECK(q.pop(0, ev));  // 消费第4个
    CHECK(q.pop(0, ev));  // 消费第5个（进度追上tail=5）
    CHECK(q.pending(0) == 0);
    CHECK(q.push(make_ev(6, MarketEvent::Type::ADD)));  // 消费者0 追上了，可写

    // ── skip：读到不处理，仍推进进度 ──
    q.skip(1);  // 消费者1 跳过1个
    printf("消费者1 skip 后 pending: %zu\n", q.pending(1));
    CHECK(q.pending(1) < 4);  // skip 推进了进度

    delete[] slots;

    if (g_failures == 0) {
        printf("\nSPMCEventQueue 单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
