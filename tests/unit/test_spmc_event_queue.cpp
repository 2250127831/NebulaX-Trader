// SPMCEventQueue 单测：多消费者各自进度 + 最慢限速 + 跳过机制
//   - 两个消费者各自读，进度独立
//   - push 按最慢消费者限速（最慢没消费，生产者满）
//   - skip：消费者读到不处理，仍推进进度（生产者能覆盖）
//   - Lazy Progress：pop 本地攒批, progress_flush() 补发, 生产者才能看到真实进度
//   - Cache Padding: heads_/locals_ 每消费者 alignas(64) 隔离
//   - Batch Reclaim: 满时批量回收(重读 min_consumed)
#include "core/queue/spmc_event_queue.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

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

    // Lazy Progress: 未攒够 kLazyBatch(64), 本地进度未发布, pending 虚高。
    // 必须 flush 后生产者才能看到真实进度。
    q.progress_flush(0);
    q.progress_flush(1);
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
    q.progress_flush(0);  // flush 后生产者才能看到进度追上
    CHECK(q.pending(0) == 0);
    CHECK(q.push(make_ev(6, MarketEvent::Type::ADD)));  // 消费者0 追上了，可写

    // ── skip：读到不处理，仍推进进度 ──
    q.skip(1);  // 消费者1 跳过1个
    q.progress_flush(1);
    printf("消费者1 skip 后 pending: %zu\n", q.pending(1));
    CHECK(q.pending(1) < 4);  // skip 推进了进度

    // ── 唤醒回归: 消费者阻塞时 push 必须广播唤醒(blocked-mask) ──
    // 旧实现按"空→非空"门控 notify: A 已阻塞(pending==0)但 B 落后时, 队列未全局空,
    // push 不 notify → A 在 poll 里饿死(旧 200ms 超时恰好兜底了这个竞态)。
    // 新实现: A 阻塞前登记 blocked 位, push 查掩码必写阻塞者的 fd, 广播必唤醒。
    {
        MarketEvent* s2 = new MarketEvent[4];
        SPMCEventQueue<16> q2(s2, 4);
        q2.set_num_consumers(2);

        // 填满; B(消费者1)滞后只消费 2 个, A(消费者0)消费全部 4 个追平(pending==0)
        for (int i = 1; i <= 4; ++i) CHECK(q2.push(make_ev(i, MarketEvent::Type::ADD)));
        q2.pop(1, ev); q2.pop(1, ev);          // B: head=2 (滞后)
        q2.pop(0, ev); q2.pop(0, ev);
        q2.pop(0, ev); q2.pop(0, ev);          // A: head=4=tail (追平, pop 内部已 flush)
        q2.progress_flush(1);                  // B flush 发布滞后进度
        CHECK(q2.pending(0) == 0);

        // A 阻塞等数据; 主线程 push 必须唤醒它(即使 B 还落后, 队列非全局空)
        std::atomic<bool> woke{false};
        std::thread t([&] { woke.store(q2.wait_for_data(0), std::memory_order_release); });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));   // 给 A 时间登记+进 poll
        CHECK(q2.push(make_ev(5, MarketEvent::Type::ADD)));           // B 未消费, 非空转换
        t.join();
        CHECK(woke.load(std::memory_order_acquire));                  // A 必须被唤醒
        CHECK(q2.pending(0) == 1);                                    // 且能读到新数据
        delete[] s2;
    }

    delete[] slots;

    if (g_failures == 0) {
        printf("\nSPMCEventQueue 单测 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
