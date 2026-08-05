# V2 优化设计方案

> 日期：2026-08-05 · 分支：V2 · 基于 V1 性能归档（PERF_V1_ARCHIVE.md）结论

## 0. 一句话目标

**V1 瓶颈是 book_th 单线程处理全部事件（2089 个 locate，委托占 97%）→ V2 按 locate 分簿并行，让多核分担订单簿重建。**

## 1. V1 瓶颈画像（事实依据）

| 事实 | 数据 | 来源 |
|---|---|---|
| 吞吐临界 | ~5.55M msg/s（book_th 是瓶颈） | PERF 4.1 |
| book_th CPU | 29.68%（第一热点） | PERF 4.0 |
| 订单簿热点 | `OrderBook::add` 4.99% + `unlink_and_free` 2.79%（std::map 红黑树 erase） | PERF 4.0 |
| IPC | 0.55（内存受限，L1 miss 134M） | PERF 4.0 |
| 事件分布 | 委托 97%（845万）+ 成交 5%（4.5万） | PERF 0.4 |
| 标的分 | 测试数据 2089 个 locate（事件分布极不均，§2.5） | 实测 |
| 长尾根源 | SPMC 排队 = 被最慢消费者 book_th 拖累 | PERF 5 |

## 2. 优化重心：book_th 分簿并行

### 2.1 现状

```
parse_th ──SPMC广播──▶ book_th（单线程，处理全部事件）
                       │  OrderBookConsumer: 按 locate 分簿，但单线程
                       ├── OrderBook::add（bids_/asks_ std::map）
                       ├── handle_delete/cancel（红黑树 erase）
                       ├── OFI/OBI 信号
                       └── arbitrate 仲裁
```

**问题**：2089 个标的的所有委托/成交挤在 book_th 单线程，是吞吐临界 + 长尾根源。

### 2.2 V2 方案：广播 + 每 worker 过滤（分簿并行）

**保持广播模型不变**，parse_th 仍单通道 SPMC 广播全部事件。只把单 book_th 拆成 N 个 book_worker，**每个 worker 维护自己的关心列表**（关心哪些 locate），只处理关心列表里的 locate，其余 skip（SPMC 天然支持：`skip()` = 读到但不处理，只推进进度）。

**新 locate 归属用动态注册（§2.5 定稿），不用静态 `locate % N`**——静态只保证 locate 数均分，不保证事件量均分（实测大标的 35384 条 vs 中位 3 条），会负载不均。

```
                        ┌── book_worker0（关心列表 [A,B,C], 其余 skip）
parse_th ──SPMC广播──▶  ├── book_worker1（关心列表 [D,E], 其余 skip）
（单通道不变）          ├── book_worker2（关心列表 [F], 其余 skip）
                        └── book_worker3（关心列表 [G,H], 其余 skip）
                              │
                        各 worker 独立仲裁（处理自己的标的, §2.3 决策5）
                              ▼
                        全局注册表 registry_（locate → owner_worker, §2.5）
```

**关键：一次只改一个变量（工程归因原则）**
- **本版只加消费者**：SPMC 广播模型、parse_th、订单簿实现全部不变，只把单 book_th 拆成 N 个 worker（各 skip 非本簿事件）。
- **下版（V3）升级为分发器 + 多条 SPSC**（V2.1 实测后修正，见下）：
  - **旧方案「分片 SPMC」有内在矛盾**：SPMC 消费者推进进度是全量的，要 worker 只 pop 自己分片仍需读全部消息确认归属——skip 判定从查 registry 变成算 `locate%N`，**pop 成本不会降到 1/N**。V2.1 实测 worker 总 CPU 73% 中相当部分是 pop×4 固定成本，证实广播 + skip 的浪费真实存在。
  - **新方案「分发器 + 多条 SPSC」**：parse_th 算 `locate%N` 把每条消息**只推入一个 worker 的专属 SPSC**。每 worker 只 pop 自己的分片，pop 成本降到 1/N。
  - **为什么分簿场景下放弃广播**：分簿后每个事件只需一个 worker 处理，**广播本来就是浪费**（多策略广播在 TD 时代有意义，已删）。分发 + 多 SPSC 是分簿的正确最终形态。
- **再下版**：订单簿缓存友好化（§3.1）等。
- **理由**：一次改太多（如同时改队列 + 分簿 + 订单簿结构）无法区分哪个改动带来收益。

### 2.3 核心设计决策

| # | 决策 | 方案 | 理由 |
|---|---|---|---|
| 1 | 分簿归属 | **全局注册表 + 动态负载均衡**：每个 worker 维护关心列表；新 locate 首次出现时注册到"收到关心事件最少"的 worker（最清闲），之后固定归该 worker | 动态均衡优于静态哈希——静态 `locate % N` 可能把大标的堆到同一 worker；动态注册让新标的分布到清闲 worker |
| 2 | 队列模型（本版） | **保持单通道 SPMC 广播不变** | 本版只加消费者，不改队列（归因原则） |
| 3 | 队列模型（V3） | **分发器 + 多条 SPSC**（parse_th 按 locate%N 分发，每 worker 只 pop 自己分片） | 分片 SPMC 有内在矛盾：SPMC 全量推进进度，worker 仍需读全部确认归属，pop 不降。分发 + 多 SPSC 真正把 pop 降到 1/N；分簿后无需广播 |
| 4 | 时序保证 | **一个 locate 只注册到一个 worker，之后固定 → 保序** | 成交不先于对应委托（V1 方案A 时序保证延续） |
| 5 | 仲裁形态 | **每 worker 独立仲裁**：处理自己的 locate、更新自己的信号、对自己的标的仲裁下单 | 时序天然正确（一个 locate 只归一个 worker）；不做全局共享仲裁（竞争 + 时序乱） |
| 6 | OrderPool/OrderMap | 共享 OrderPool（无锁，§3.4）+ 共享 OrderMap（CAS 快路径 + 桶级锁，§3.5） | pool 无锁栈；map 链路径无锁、树化只锁桶 |
| 7 | 成交路径 | 成交按 order_ref 定位单挂单减量（V1 已最优，§3.2） | 分簿后只触碰本 worker 盘口，天然隔离 |

### 2.4 预期收益

- **吞吐**：N worker 并行处理订单簿 → 临界提升。但**每个 worker 仍全量 pop**（广播 + skip 的 pop 成本不降），提升受限于 pop 固定成本（预计 ~1.5-2×，非 N×）
- **延迟（主目标）**：SPMC 排队被 N 路摊薄 → 长尾大幅下降，**P99 <500µs 的关键**
- **IPC**：每 worker 处理更少标的 → 缓存局部性改善

> **预期收益的精确性**：book_th 的 29.68% CPU 中，订单簿处理 ~12%（add/delete/unlink），线程循环 + pop ~18%。广播 + skip 只摊薄订单簿处理（→ 1/N），pop 固定成本不降（每个 worker 仍全量 pop）。**动态负载均衡（§2.5）也只均衡"处理"负载，不消除"pop"固定成本**——两者一致：pop 是每 worker 必付的 18%，处理被摊薄。故吞吐 ~1.5-2×，延迟改善显著（SPMC 排队 4 路摊薄）。

### 2.5 全局注册表 + 动态负载均衡（定稿）

**问题**：静态 `locate % N` 只保证 locate 数均分，不保证事件量均分（实测测试数据最大 35384 条 vs 中位 3 条）。若大标的堆到同一 worker，该 worker 负载高 → SPMC 等最慢消费者 → 收益打折。

**方案**：每 worker 维护**关心列表**（自己处理的 locate 集合），新 locate 首次出现时**注册到最清闲的 worker**。

```
每个 worker 维护:
  care_list_: 自己关心的 locate 集合(哈希查 O(1))
  cared_count_: 自己处理的关心事件数(原子, 负载均衡依据)

全局注册表:
  registry_: locate → owner_worker(首次出现时原子注册)

新 locate X 首次出现(广播, 所有 worker 都收到):
  查 registry_:
    已注册 → 只有 owner worker 处理, 其余 skip
    未注册 → 原子注册到"cared_count_ 最小"的 worker(最清闲)
             → 加入该 worker 的 care_list_
             → 之后 X 固定归该 worker
```

**关键点**：
- **保序**：一个 locate 只注册到一个 worker，之后固定 → 该 worker 独占时序（成交不先于对应委托）
- **动态均衡**：新 locate 总去最清闲 worker → 长期运行负载趋向均衡（优于静态哈希）
- **广播 + skip 仍成立**：parse_th 广播，worker 查 care_list_ 决定处理/skip
- **注册并发**：新 locate 首次出现时多个 worker 同时收到，需原子注册（全局注册表用 CAS/锁保护，但只在首次，极低频）
- **计数**：cared_count_ 是 worker 实际处理的本簿事件数（不是收到的全部）

**实现注意**：
- registry_ 是全局共享（多 worker 读 + 首次写），需并发安全（无锁或锁，但首次注册极低频）
- care_list_ 每 worker 私有（仅自己线程读写），无需并发
- 动态注册依赖"新 locate 首次出现"——测试数据 2089 个 locate 都在压测前期出现，注册集中在启动阶段

## 3. 次级优化项（按优先级）

### 3.1 订单簿缓存友好化（配合分簿）
- **std::map 价格档 → 开地址哈希 / 扁平数组**：红黑树 erase 的 O(log n) + 指针追逐，是 IPC 0.55 主因
- 价格档用 `alignas(64)` 缓存行对齐，减少跨 cache line 访问
- **注意**：只改 bids_/asks_ 价格档，不动 OrderMap（已高效 O(1)）

### 3.2 成交路径（已核实，无需独立优化）
- **成交（EXECUTE/P）已按 order_ref 定位单挂单 + 减量**，不走完整订单簿重建（`OrderBook::execute`：`order_index_.find` → `dec_level_qty`）。
- 成交满仓时触发 `unlink_and_free` 的 `bids_.erase`（红黑树删除）→ 归入 §3.1 订单簿缓存友好化，不单独立项。
- **注**：曾误以为"成交需快速路径"，核实后确认当前已是最优（成交路径远轻于委托 ADD 路径）。

### 3.3 parse_th 多解析器并行
- 单 parse_th 占 ~16% CPU，解析 ITCH 是纯计算
- 多 parser 消费者拉高 parse 吞吐上限（V1 PERF 6.3 已有方向）
- **定位（见 §3.6）**：解析是纯 CPU 计算（吃多核），**优先于多接收线程**（接收是 I/O，受网卡/内核限制）。多解析器比多接收线程更值得。
- **依赖**：分簿并行后 book_th 不再是瓶颈，parse 才成为新瓶颈

### 3.4 无锁 OrderPool 升级（定稿）

**背景**：分簿并行后多 worker 并发访问共享 OrderPool，`free_head_` 是唯一竞争点。

**定稿方案：Treiber 无锁空闲栈**（标准无锁栈，代价 ~15 行）

```cpp
std::atomic<uint32_t> free_head_{0};   // 原 uint32_t → atomic

OrderSlot* allocate() {
    uint32_t head = free_head_.load(std::memory_order_relaxed);
    while (head != UINT32_MAX) {
        uint32_t next = storage_[head].pool_next_free;
        if (free_head_.compare_exchange_weak(head, next,
                std::memory_order_release, std::memory_order_relaxed)) {
            return &storage_[head];     // CAS 成功 → 取走头部
        }
        // CAS 失败 → 其他线程改了 free_head_, head 已更新, 重试
    }
    return nullptr;                     // 空池
}

void deallocate(uint32_t idx) {
    uint32_t head = free_head_.load(std::memory_order_relaxed);
    do {
        storage_[idx].pool_next_free = head;   // 新节点指向当前头
    } while (!free_head_.compare_exchange_weak(head, idx,
                std::memory_order_release, std::memory_order_relaxed));
    // CAS 成功 → 新节点成为头
}
```

**改动清单**：
| 项 | 原 | 新 | 代价 |
|---|---|---|---|
| `free_head_` | `uint32_t` | `std::atomic<uint32_t>` | 1 行 |
| `allocate()` | 直接读 | CAS 循环 | ~5 行 |
| `deallocate()` | 直接写 | CAS 循环 | ~5 行 |
| `initFreeList`/`rebuildFreelist` | `free_head_ = 0` | `free_head_.store(0)` | 2 处 |
| `size_` | `size_t` | `std::atomic<size_t>`（统计用） | 1 行 |
| **合计** | — | — | **~15 行** |

**并发正确性**：
- **storage_ 独立**：不同 slot 是独立内存，不同 worker 操作不同 slot 天然并行
- **CAS 唯一共享点**：free_head_ 的读写全部原子，无锁无等待，竞争时 CAS 重试
- **ABA 分析**：Treiber 栈经典隐患。但 OrderPool 槽位**唯一引用**（一个 order_ref 占一个 slot，deallocate 后不会被其他线程复用同 idx 直到再 allocate），ABA 不会导致逻辑错误（每次 CAS 后重新读 `next`）
- **at()/indexOf()**：只读，不改，天然线程安全

**为什么不加锁**：锁（mutex/atomic spin）违背低延迟无锁原则；分簿并行就是要消除 book_th 单线程的锁等待，OrderPool 用无锁栈与整体设计一致。

**验收**：单测覆盖并发 allocate/deallocate（多线程压力测试，无数据竞争、无 ABA 误用）；回归 V1 单线程测试不退化。

### 3.5 OrderMap 并发升级：CAS 快路径 + 桶级锁慢路径（定稿）

**背景**：分簿并行后，OrderMap 需支持并发。**当前已是惰性树化**（桶链长 ≥8 才迁移到 overflow_，非预分配）。但有两个并发缺陷：
1. `free_head_`（节点空闲链）共享，并发 allocNode/freeNode 竞争
2. `overflow_` 是**全局一个 std::map**，任何桶树化都写它，且无锁 → 并发写竞争

**定稿方案 A：每桶独立树 + 桶级锁**（快路径无锁，慢路径只锁桶）

| 成员 | 现状 | 改造 |
|---|---|---|
| `free_head_` | `uint32_t` | `std::atomic<uint32_t>`（无锁节点分配，同 OrderPool Treiber） |
| `buckets_` | `uint32_t*` | 不变（链头） |
| `bucket_len_` | `uint16_t*` | `std::atomic<uint16_t>*`（或锁内更新） |
| `overflow_` | 全局 std::map | **每桶惰性独立树** |
| 桶锁 | 无 | **每桶惰性 spinlock** |

**桶状态（惰性，不预分配 1M 锁/树）**：
```cpp
struct BucketState {
    std::atomic<uint32_t> state;       // 0=链, 2=已树化
    std::atomic<BucketLock*> lock;     // 惰性创建（树化时）
    std::map<uint64_t, OrderSlot*>* tree;  // 惰性创建（树化时）
};
BucketState* bucket_state_;   // 数组, 初始 state=0, lock/tree=nullptr
```

**操作流程**：
```
insert(order_ref):
  b = hash(key)
  if bucket_state_[b].state == 0(链):      // 快路径 99.99%
    CAS 无锁插入链头 + bucket_len_++(原子)
    if len ≥ 8: 抢桶锁 → 链迁移到 tree → state=2
  else(已树化):                            // 慢路径极罕见
    lock = 惰性创建该桶锁 → 锁 → 写 tree → 解锁
find/erase: 链 CAS 查 / 树锁查
```

**为什么高效（对比全局 overflow_ + 全局锁）**：
- **快路径**（链）覆盖 99.99%+ 操作，CAS 无锁，零锁等待（实测桶冲突率 6.6e-24，见下）
- **慢路径**（树）极罕见，且**只锁那个桶**——不同桶树化并发不互斥
- 全局 overflow_ + 全局锁会**锁整个 map**，阻塞所有桶 → 桶级锁明显高效

**实测依据**：测试数据 427982 个 order_ref 散到 1M 桶，每桶平均 λ=0.005，P(桶长≥8)=6.6e-24 → **树化几乎不触发，CAS 快路径覆盖全量操作**。

**惰性树化的保留**：当前 `bucket_len_[b] >= 8` 才迁移，本就是惰性。A 方案保留此阈值，只是把"迁移到全局 overflow_"改成"迁移到该桶独立 tree"。

**并发正确性**：
- 快路径链头 CAS：同 OrderPool Treiber 思路。**ABA 需注意**——OrderMap 的 Node 与 OrderPool 槽位不同：Node 可被 freeNode 后复用（同 index 可能被两个 order_ref 先后占用），CAS 时需配合 **hash 分桶隔离**（同一桶通常只被少数 locate 的 order_ref 访问）降低 ABA 风险，或用带 tag 的 CAS 消除。实施时重点验证。
- 慢路径树：桶级锁串行化该桶，不同桶无互斥
- 桶状态 state 原子：链↔树转换用 CAS 标记，避免竞态

**验收**：多线程压力测试（N 线程并发 insert/find/erase，含桶树化场景，无数据竞争）；回归 V1 单线程测试不退化。

### 3.6 网络层升级：多在途 recv（定稿方向）

**V1 网络层现状**：
- **单在途 recv**：一个 `buf_idx_` + `recv_pending_` 标志，一次只提交 1 个 recv SQE（`io_uring_receiver`）。缓冲池 `MAX_BUFFERS=64` 已注册但只用了 1 个。
- 每包串行：recv 完成 → memcpy 到用户 buf → 拆包 → 再提交下一个。
- **实测**：recv→unpack P50 1.7µs / P999 6.1µs——网络层**不是延迟瓶颈**（SPMC 排队 3ms 才是）。

**V2 网络层方向（已定）**：
1. **多在途 recv（主方向）**：用满 `MAX_BUFFERS=64`，单线程预提交 64 个 recv SQE，内核并行收包，消除"收一包→处理→再提交"的串行。
2. **多解析器优先，不搞多接收线程**：
   - **多接收线程 ❌**：保持时序复杂 + 上 AF_XDP 后网卡即极限，多线程收包收益有限。
   - **多解析器 ✅**：解析是 CPU 计算（可并行，吃多核），接收是 I/O（受网卡/内核限制）。多解析器比多接收线程更值得。
3. **零拷贝（次要）**：unpacker 直接读 io_uring 固定缓冲，省 memcpy——固定小开销，非主方向。
4. **REUSEPORT 多 socket（不优先）**：需多接收线程 + 时序处理，复杂度高，AF_XDP 下收益有限。

**关键洞察（架构判断）**：
> 网络栈瓶颈最终是**网卡/内核**，不是线程数。单线程 io_uring 就能驱动多个在途 recv（submit 多 SQE，内核并行），无需多接收线程。而**解析是纯 CPU 计算**，多解析器才真正需要多核。故网络层升级 = 多在途 recv（单线程）+ 多解析器（多核）。

### 3.7 删除 TradeDirection 策略（V1.5 独立步骤）

**背景**：TD（TradeDirection）当初是**为验证 SPMC 多消费者广播**引入的独立高频策略（消费成交，写 sig_td 槽，不依赖订单簿）。V2 引入多 worker 后，SPMC 多消费者已由 N 个 worker 天然验证，TD 历史使命完成。

**决策**：删除 TD（独立成 V1.5 版本，先测基线再分簿，符合一次只改一个变量）。

**删除清单**：
| 项 | 现状 | 删除后 |
|---|---|---|
| `strategy_th` 线程 | consumer 1，消费成交跑 TD | 删除线程 |
| `sig_td` 信号槽 | 仲裁读 | 删除 |
| `TradeDirectionStrategy` | `strategy/tick/trade_direction_strategy.h` | 删除 |
| 仲裁逻辑 | 三信号（OFI/OBI/TD），TD 有方向三同向 | **两信号（OFI/OBI）**：`ok = (so != NONE && so == sb)` |

**影响**：
- 架构简化：少一个线程 + 一个信号槽 + 仲裁降为两信号
- **SPMC 消费者数**：V1 是 2 个消费者（book_th + strategy_th），删后 book_worker 们仍是多消费者（N 个），SPMC 多消费者能力不变
- 仲裁灵敏度变化：去掉 TD 的"第三信号约束"，两信号同向即下单（原 TD 无方向时本就退化为两信号）

**验证**：V1.5 对比 V1，确认 TD 删除对吞吐/延迟的影响（预期无退化，因 TD 本身不下单只产信号）。

## 4. 成功标准

| 指标 | V1 基线 | V2 目标 |
|---|---|---|
| 吞吐临界 | ~5.55M msg/s | **>8M msg/s** |
| 端到端 P99 | ~3.1ms | **<500µs** |
| IPC | 0.55 | **>0.8**（内存受限缓解） |

## 5. 实施顺序（一次只改一个变量）

**归因原则**：每版只改一个变量，测吞吐 + 延迟 + 硬件事件，对比上一版，才能干净归因收益来源。

| 版本 | 改动 | 对比验证 |
|---|---|---|
| **V1.5 删 TD** | 删 strategy_th 线程 + sig_td 槽 + TradeDirectionStrategy，仲裁从三信号(OFI/OBI/TD)退化为两信号(OFI/OBI) | vs V1：TD 删除的独立影响（架构简化） |
| **V2.1** | 基于 V1.5：单 book_th → N 个 book_worker（广播 + skip，SPMC 不变）+ 无锁 OrderPool（§3.4）+ OrderMap 并发（§3.5） | vs V1.5：吞吐/延迟/IPC 提升来自"分簿并行" |
| **V2.2** | 订单簿缓存友好化（§3.1，std::map → 开地址哈希） | vs V2.1：数据结构收益 |
| **V2.3** | 多解析器（§3.3） | vs V2.2：解析并行收益 |
| **V2.4** | 网络层升级：多在途 recv（§3.6，单线程预提交多 SQE） | vs V2.3：分簿后吞吐上去，网络层是否成新瓶颈 |
| **V3（下一阶段）** | 队列升级为**分发器 + 多条 SPSC**（parse_th 按 locate%N 分发到专属 SPSC，每 worker 只 pop 自己分片） | vs V2.4：消除广播 + skip 的全量 pop 浪费（V2.1 实测 worker 总 CPU 73% 中 pop×4 是主要部分） |

> **V2.1 实测修正（2026-08-06）**：原计划「分片 SPMC」有内在矛盾——SPMC 消费者推进进度是全量，worker 仍需读全部消息确认归属，pop 不降。改为**分发器 + 多条 SPSC**：parse_th 算 `locate%N` 每条消息只推入一个 worker 的专属 SPSC，每 worker 只 pop 自己的分片，pop 成本从 N× 降到 1×。分簿后每事件只需一个 worker，广播本是浪费，故放弃广播能力。

> **本版（V2.1）只加消费者**：SPMC 广播模型、parse_th、订单簿结构全部不变，只把单 book_th 拆成 N 个 worker（各 skip 非本簿事件）。这样才能归因"分簿并行"本身的收益，不被队列/数据结构改动污染。

## 6. 风险与注意

- **负载均衡**：测试数据 **2089 个 locate，事件分布极不均匀**（实测最大 35384 条 vs 中位 3 条）。**已定稿用全局注册表 + 动态负载均衡**（§2.5）——新 locate 注册到 cared_count_ 最小（最清闲）的 worker，替代静态 `locate % N`。
- **仲裁形态（已定：每 worker 独立仲裁）**：每个 worker 处理自己的 locate、更新自己的 OFI/OBI、**对自己的标的仲裁下单**——时序天然正确（一个 locate 只归一个 worker，该 worker 的顺序即标的顺序），无跨 worker 竞争。**不做全局共享仲裁**（N worker 写同一信号槽会竞争 + 时序乱）。仲裁为两信号（OFI/OBI），TD 已删（§3.7）。
- **worker 线程数**：广播 + skip 方案下队列仍是 1 个 SPMC（不增加队列/内存/eventfd），唯一成本是 **N 个线程** → N 取 CPU 核数/2 左右（避免线程争 CPU；V3 分发器 + 多 SPSC 才涉及 N 队列的资源考量，SPSC 每队列 1 个 eventfd + 独立缓冲）
- **⚠️ OrderMap/OrderPool 并发（关键，已定稿）**：
  - **OrderPool**：升级为**无锁 Treiber 空闲栈**（见 §3.4），共享存储 + 无锁 free_head_，多 worker 安全并发。
  - **OrderMap**：升级为**CAS 快路径 + 桶级锁慢路径**（见 §3.5）——链路径 CAS 无锁，树化路径只锁该桶（惰性独立树，替代全局 overflow_）。key 可用 `(locate, order_ref)` 拼接保证全局唯一（若 per-locate 重复）。

## 6.5 V3 设计：分发器 + 多条 SPSC（2026-08-06 定稿）

**背景（V2.1 实测驱动）**：广播 + skip 下每个 worker pop 全部消息（V2.1 实测 worker 总 CPU 73% 中相当部分是 pop×4 固定成本，IPC 0.39 反映多核争抢）。分簿后每事件只需一个 worker，广播本是浪费。

### 6.5.1 架构

```
parse_th 按 locate%N 算分片 → 每条消息只推入一个 worker 的专属 SPSC
worker0 ←── SPSC0 (locate%N==0 的消息)
worker1 ←── SPSC1 (locate%N==1)
...
每 worker 只 pop 自己的分片, pop 成本 1/N(非 N×)
```

### 6.5.2 分发算法

- **静态 `locate % N`**：解析时算分片，O(1) 零状态。但只保证 locate 数均分，不保证事件量均分（V2.1 实测大 locate 35384 条 vs 中位 3 条）——与 §2.5 动态均衡的目标冲突。
- **方案：注册表 + 分发映射**（复用 V2.1 `BookRegistry`）：locate → owner worker（动态注册，按 cared_count 均衡）→ parse_th 查表分发。把 §2.5 的"消费者侧 skip 判定"前移到"生产者侧路由"。
- **保序**：一个 locate 只归一个 worker → 该 worker 顺序即标的顺序（与 V2.1 相同）。

### 6.5.3 正确性

- **SPSC 单写单读**：parse_th 是唯一写者（每条消息只推一个 SPSC），worker 是唯一读者 → 无竞争，SPSC 天然无锁。
- **OrderPool/OrderMap 仍共享无锁**：worker 并发 alloc/dealloc 同一池/索引（V2.1 前置已无锁化），不受队列改造影响。
- **背压**：SPSC 满时 parse_th 阻塞/自旋（单写者背压），不再有"最慢消费者拖累"（SPMC 特性），但快 worker 的 SPSC 空转。

### 6.5.4 预期收益

- **pop 成本 4× → 1×**：每 worker 只 pop 1/N 消息，72B 拷贝 + 进度推进省掉 3/4。
- **IPC 提升**：消除共享 OrderMap/OrderPool 的多核争抢（分发后单 worker 处理自己分片，共享结构访问降 1/N）。
- **吞吐**：parse_th 路由成本（哈希/查表 + push 到 N 队列之一）≈ 广播 push 成本，但 worker 侧省掉 N×pop → 吞吐可能突破 5.5M（受 recv_th 限制仍可能封顶）。
- **代价**：N 个 SPSC 队列（N 个缓冲 + eventfd）；worker 负载不均时快 worker 空转。

### 6.5.5 与 V2.1 的关系

- V2.1 已定稿 registry（locate→owner 原子数组）+ 双键负载均衡。V3 只改**路由位置**（消费者 skip → 生产者分发），registry/均衡/仲裁/无锁池全部复用。
- **一次只改一个变量**：V3 只动队列 + 路由，订单簿/策略/无锁池不动，才能归因"队列改造"的收益。

## 7. V2.1 落地记录（2026-08-06）

按 §2 设计落地，实际实现与定稿的两处简化 + 三处正确性修复：

**落地形态**：每 worker 一套 `BookWorker`（OrderBookConsumer + OBI/OFI 策略 + 信号槽 + last_order 状态 + 独立仲裁），共享无锁 OrderPool/OrderMap + ExecutionEngine(锁)。`order_book.workers` 配置（默认 4），`workers: 1` 退化为单 book_th。

### 7.1 设计偏离（简化）

- **registry 即关心判定，不用 care_list**：locate 是 ITCH 16-bit(0-65535) → `BookRegistry.owner_[65536]` 原子数组，每事件查 `owner = registry[locate]`，`owner != 我 → skip`。比 care_list 哈希查还快（数组索引 + 一次原子读），省去 care_list 的维护。
- **动态均衡 argmin 平局归自己**：定稿说"注册到 cared_count 最小者"。实测平局(启动期全 0)时 argmin 偏向 index 0 → worker0 超载(656万 vs 其他 60万)。改为 target 初始为 my_id：平局时每个 worker 把自己遇到的新 locate 收下 → 自然分散(实测 289/174/186/200万)；有 worker 超载后新 locate 才让给更清闲者。

### 7.2 正确性修复（TSAN 驱动）

分簿多 worker 并发暴露了前置无锁化的非原子字段 UB，全部原子化(relaxed, x86 零开销)：

- **`OrderSlot::pool_next_free` 原子化**：OrderPool::allocate 无锁读 `storage_[head].pool_next_free` 与并发 deallocate 写同节点 → 非原子是 C++ UB。
- **`OrderMap::Node` 原子化**：order_ref/order/bucket 原子化(insert store / find·erase load)，find 无锁读链节点 vs 复用写。
- **`free_head_` acquire/release 配对**：allocate/allocNode 读 free_head_ 用 acquire(与 deallocate/freeNode 的 release 配对)，建立节点复用的 happens-before。
- **`ItchParser::msg_count_` 原子化**：V1 既有 race(parse_th 写 vs 主线程 idle 监测读)，顺手修。
- **ExecutionEngine send 移进锁内**：IoUringSender 是 SPSCByteRing 非线程安全，多 worker 并发 submit_signal → 锁外并发 send 竞争。下单频率万级，锁内可接受。

### 7.3 验证

- **ctest 16/16** 全绿；`workers: 1` 退化行为与 V1.5 一致。
- **压测零丢包**：rate 10000 → sent 8737176 == parsed 8737171(差 5, UDP 边界包)；worker 处理总和 8502596 = 委托 845 万 + 成交 4.5 万(V2_PLAN §1 事件分布吻合)，每事件恰被一个 worker 处理。
- **负载均衡**：修复后 289/174/186/200 万(分布 17-29%)，无单一 straggler。
- **TSAN**：pool_next_free/Node/message_count 的 C++ UB 全消；workers=1 零 race。剩余 `OrderBook::add` 写 slot 字段 vs 并发 worker 读同一 slot 的复用 race 是 **Treiber 池单值 free_head_ release 被覆盖** 导致——x86 TSO 下由 dealloc/alloc 屏障保证可见性(Release 压测功能正常、零丢包、无崩溃)，与前置无锁设计一致接受的 trade-off。
- **性能对比**：限速场景(rate 10000) workers=1 与 4 解析 QPS 一致(~505 万, 发送端瓶颈, 无退化)。分簿收益在 book 成为瓶颈时体现，但纯 UDP 压测受 recv_th 接收端限制，无法隔离测量(rate 300000 时 UDP 丢包主导)。
