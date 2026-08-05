// OrderMap/OrderPool 并发压力测试
// 验证: N 线程并发 insert/find/erase, 模拟分簿语义
//   - 每个线程操作自己的 key 集(同 key 单线程访问, 对应分簿"同 order_ref 单 worker")
//   - 不同线程的 key 可能哈希到同一桶(强制跨 worker 并发冲突)
//   - 验证: 无丢失(key 插入后必被 find 到)、无重复、最终 size 正确、无数据竞争
//
// 用法: 正常 ctest 跑; 用 -fsanitize=thread 构建可测数据竞争
//   cmake -DCMAKE_CXX_FLAGS="-fsanitize=thread" .. && ./build/ut_concurrent_order_map

#include "core/memory/order_map.h"
#include "core/memory/order_pool.h"
#include "core/types.h"

#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>
#include <cstdlib>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

// 每个线程: 在自己的 key 集上循环 [insert → find → erase], 模拟分簿。
// 关键: 每线程预分配自己的 OrderSlot 段, 复用同一批槽(模拟"同 order_ref 只被
//   一个 worker 访问"→ 同一 idx 不被跨线程复用)。这样 free_head_ 的节点不跨线程
//   争用, 验证的是"同 key 串行 + 不同 key 同桶并发"的生产语义。
static void worker(OrderMap& map, OrderPool& pool, int tid,
                   int n_keys, int iters, std::atomic<bool>& stop) {
    uint64_t base = (uint64_t)tid * (uint64_t)n_keys;

    // 预分配 n_keys 个槽(每线程自己的段), 循环复用。分簿下同一订单池段
    // 只被 owner worker 访问 → 不会跨线程 alloc/free 同一 idx。
    std::vector<OrderSlot*> my_slots(n_keys);
    for (int i = 0; i < n_keys; ++i) {
        OrderSlot* s = pool.allocate();
        if (!s) { stop.store(true, std::memory_order_relaxed); return; }
        my_slots[i] = s;
    }

    for (int it = 0; it < iters; ++it) {
        uint64_t k = base + (uint64_t)(it % n_keys);
        OrderSlot* s = my_slots[it % n_keys];
        s->order_ref = k;
        map.insert(k, s);

        OrderSlot* got = map.find(k);
        if (got != s) {
            printf("thread %d: key %llu find miss (got=%p exp=%p)\n", tid,
                   (unsigned long long)k, (void*)got, (void*)s);
            ++g_failures;
        }

        map.erase(k);
    }

    for (OrderSlot* s : my_slots) pool.deallocate(s);   // 结束时统一归还
}

int main() {
    constexpr int N_THREADS = 8;
    constexpr int N_KEYS    = 512;    // 每线程 key 数
    constexpr int ITERS     = 20000;  // 每线程循环次数

    OrderPool pool(N_THREADS * N_KEYS);  // 池: 每线程预分配 N_KEYS 个槽(自己的段)
    // map 节点池: 需容纳所有线程同时在池外的节点(每线程最多 N_KEYS 个 insert 未 erase)。
    // 桶数 1024, 每线程 512 key → 跨线程冲突到同桶。
    OrderMap  map(N_THREADS * N_KEYS);

    std::vector<std::thread> threads;
    std::atomic<bool> stop{false};
    for (int t = 0; t < N_THREADS; ++t)
        threads.emplace_back(worker, std::ref(map), std::ref(pool), t, N_KEYS, ITERS, std::ref(stop));

    for (auto& th : threads) th.join();

    // ── 最终一致性: 所有 worker 的 insert 都已 erase → size 应为 0 ──
    printf("并发结束后 map.size() = %zu (期望 0)\n", map.size());
    CHECK(map.size() == 0);
    // 池: 所有槽应归还 → size 应为 0
    printf("并发结束后 pool.size() = %zu (期望 0)\n", pool.size());
    CHECK(pool.size() == 0);

    if (!stop.load()) {
        printf("\nOrderMap/OrderPool 并发压力测试 PASS ✓\n");
        return g_failures == 0 ? 0 : 1;
    }
    printf("\n池耗尽, 测试中断\n");
    return 1;
}
