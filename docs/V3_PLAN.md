# V3 计划：分发器 + 多条 SPSC + retry 桶 + AF_XDP（定稿）

> 日期：2026-08-07 · 分支：V2（V3 从 V2 最终版 4f0c7f6 起）· 前置：V2 全版本完成
> 与 VERSION_PLAN.md 的关系：V3 是业务线"并行 Pipeline（Dispatcher 分发层）"的最后一环——
> V2 细化成 V2.1-V2.4（分簿/哈希/多解析器/多在途），Dispatcher 分发层顺延到 V3。

## 0. 目标（三个升级，一次只改一个变量）

1. **解析回退 SPSC + 单解析器**（延迟优先，V2.3 实测驱动）
2. **分发器 + 多条 SPSC**（parse_th 按 registry 分发，每 worker 只 pop 自己的分片）
3. **AF_XDP 网络后端**（突破 UDP 栈吞吐：驱动层零拷贝，绕过协议栈，UMEM 直搬）

**归因纪律**：V3 前两项只动队列 + 路由，订单簿/策略/无锁池不动，才能归因"队列改造"的收益；AF_XDP 是独立的网络后端（新增 `AF_XDPReceiver`，业务代码不改，走 `IMarketDataReceiver` 抽象）。

## 1. 背景与实测依据

- **V2.1 实测**：广播 + skip 下每个 worker pop 全部消息（worker 总 CPU 73% 中相当部分是 pop×4 固定成本，IPC 0.39 反映多核争抢）。分簿后每事件只需一个 worker，广播本是浪费。
- **V2.3 实测**：SPMC 多解析器（字节 ring claim/commit 开销）在真实市场速率下劣于 SPSC——SPSC 单解析器 P99=11.7µs（峰值 rate4000）vs SPMC parse2 35.8µs，**SPSC 快 3 倍**。真实市场峰值 ≤200 万条/秒 << SPSC 单解析能力 5M，SPMC 多解析器的高吞吐能力用不上（UDP 栈封顶），只剩延迟劣势。

## 2. 架构（定稿）

```
┌─ 解析线程(单, E16, 兼分发器) ──────────────────────────┐
│  recv_th → SPSCByteRing → 解析事件                      │
│  owner = registry[locate]                               │
│  ├─ 直接 push spsc[owner] 成功 → 继续                   │
│  └─ 满(false) → 塞入 retry_bucket[owner](每 SPSC 独立)   │
│       └ 该桶满才阻塞(只卡自己, 不拖累其他桶)              │
│       → wake retry → 继续                              │
└────────────────────────────────────────────────────────┘

┌─ retry 线程(常驻, E17) ────────────────────────────────┐
│  阻塞 poll(wake_fd)                                     │
│  唤醒 → 遍历 N 个桶:                                    │
│    桶 i: 尝试 push 到 spsc[i], 能推的推掉(同桶保序)      │
│          推不掉的留桶下轮                               │
│  全清空 → 阻塞等唤醒                                   │
└────────────────────────────────────────────────────────┘

worker0 ←── spsc0    worker1 ←── spsc1    ...    worker3 ←── spsc3
每 worker 只 pop 自己的 SPSC, pop 成本 1/N(非 N×), 无 skip
```

## 3. 组件变更清单

### 3.1 新增：固定槽 SPSC slot ring（下游事件通道）

- **不能用 SPSCByteRing**（那是 [头][体] 字节布局）。需要**定长 MarketEvent 元素**的 SPSC slot ring。
- tail 生产者推进、head 消费者推进，单写单读无锁（语义同现有 SPMCEventQueue 的单消费者路径，去掉 multi-consumer 的 heads_/min_head/blocked-mask）。
- 唤醒：每队列 1 个 eventfd + blocked-mask（复用现有 [spmc_event_queue.h](core/queue/spmc_event_queue.h) 的"push 只写阻塞者 fd"模式）。
- 容量：chan_slots(1M)/4 = 256K 槽/worker（真实速率下 ~0.5s 缓冲）。

### 3.2 新增：retry_bucket[]（每 SPSC 独立容量）

- N 个待重试桶，每桶 = 固定槽 SPSC（条目 = MarketEvent，单写解析器 / 单读 retry 线程）。
- **每 SPSC 独立容量**：单桶满只阻塞向该桶的 push，其他桶照常（解析器向慢桶塞不进时仍能向快桶塞）。
- retry 线程：阻塞 poll(wake_fd) → 唤醒 → 遍历 N 桶逐桶尝试 push 到对应 spsc[i] → 能推的推掉（同桶内按输入序）、推不掉的留桶下轮 → 全清空后阻塞等唤醒。

### 3.3 修改：ByteRingParser 回退 SPSC

- SPMCByteRing（多解析器版）→ SPSCByteRing + 单解析器，解析循环大幅简化（无 claim_lock/claim/commit/空洞/保序屏障）。
- **分发**：解析出事件后 `owner = registry.lookup(ev.locate)` → `push(spsc[owner], ev)`，满则 `retry_bucket[owner].push(ev)` + 唤醒 retry。
- SPMC 多解析器代码保留（git 历史保存，面向 AF_XDP 后高吞吐场景——AF_XDP 突破 UDP 栈封顶后，SPMC 多解析器的并行能力才能用上）。

### 3.4 修改：BookRegistry 分发侧

- V2.1 的 [BookRegistry](main.cpp) 从"消费者侧 skip 判定"改为"生产者侧路由"。**单解析器内联注册**（无并发，V2.1 的 compare_exchange 竞争消失），每事件 1 次原子读（O(1) 快路径）。双键负载均衡（cared_count 主 + registered_count 次）原样复用。

### 3.5 修改：worker 简化

- 每 worker 只 pop 自己的 SPSC → 直接 process，**无 skip、无每事件 registry 查表、无 pop×4**。SPMC 的 tail/head 缓存行由 5 线程争抢 → 每条 SPSC 只 2 线程碰。

### 3.6 配置

- `parse_workers`：固定 1（移除多解析器配置，或保留字段但限制 1）。
- 新增 `retry_workers`（默认 1）、每 SPSC/retry 桶容量。

### 3.7 绑核（与 V2 最终版一致）

```
recv = P5        高吞吐收包(不抢)
parse = E16      解析器 + 分发器(小核无 SMT 真独占)
retry = E17      常驻, 处理下游满(小核)
worker = E20-23  4 个小核真独占
fill/主线程 = P9 低频共享
```

## 4. 正确性

- **SPSC 单写单读**：解析器是 spsc[owner] 唯一写者（每条事件只推一个 SPSC），worker 是唯一读者 → 无竞争，SPSC 天然无锁。retry_bucket 同理（解析器单写 / retry 单读）。
- **保序**：一个 locate 只归一个 worker → 该 worker 顺序即标的顺序（与 V2.1 相同）。同 SPSC 的事件，直接 push 的必然先于丢进桶的（满时后续直接 push 必失败 → 进同桶）；retry 同桶按输入序推 → 不超车。跨 SPSC 无依赖。
- **OrderPool/OrderMap 仍共享无锁**：worker 并发 alloc/dealloc 同一池/索引（V2.1 前置已无锁化），不受队列改造影响。

## 5. retry 桶（慢消费者隔离）—— 诚实边界

**隔离能力**：分发器让快 worker 不饿死、慢 worker 由 retry 专泵；单桶满只卡该桶，解析器仍能向其他桶塞。**正常时 retry 零开销**（不满全直接 push，retry 线程休眠，只在满时被唤醒）。

**极端退路**：解析器仅在 `retry_bucket[owner]` 满时才阻塞（缓冲耗尽），靠每桶容量吸收重标 burst。

**诚实边界（必须写进结论）**：慢消费者隔离是**概率性**的——
- 重标 burst（实测最大 35384 条 ≈ 峰值 18ms）触发时，靠桶容量吸收，快 worker 不饿死；
- **持续**超载（重标的归属使某 worker 长期满载）解析器仍会被该桶阻塞 → 拖累全链。这与 V2.1"最慢消费者拖累 SPMC"本质相同——**分发器不解决负载不均，只把触发概率降 4×（每 worker 只处理 1/N 负载）**。彻底解决需拆大标的/动态迁移（已明确不做，分簿固有代价，归档已接受）。

## 6. 预期收益

- **pop 成本 4× → 1×**：每 worker 只 pop 1/N 消息，72B 拷贝 + 进度推进省掉 3/4。
- **IPC 提升**：消除共享 OrderMap/OrderPool 的多核争抢（分发后单 worker 处理自己分片，共享结构访问降 1/N）。
- **延迟**：worker 侧无 skip、无 pop×4，SPMC 排队消除 → 端到端 P99 有望再降（V2.4 队列段 6.1µs）。
- **吞吐**：parse_th 路由成本（查表 + push 到 N 队列之一）≈ 广播 push 成本，但 worker 侧省掉 N×pop → 吞吐可能突破（受 recv_th UDP 栈限制仍可能封顶）。
- **代价**：N 个 SPSC 队列（N 个缓冲 + eventfd）+ N 个 retry 桶（独立容量）。

## 7. 实现步骤与验证

```
1. 新增固定槽 SPSC slot ring + 单测(退化 N=1/回绕/保序) → 验证: ctest
2. ByteRingParser 回退 SPSC 单解析器 + 分发到 spsc[owner] → 验证: 压测零丢 + 保序
3. 新增 retry_bucket + retry 线程 → 验证: 慢消费者隔离(重标 burst 下快 worker 不饿死)
4. BookRegistry 生产者侧路由(双键均衡复用) → 验证: 负载分布实测
5. worker 简化(无 skip) → 验证: 压测 sent==parsed, 事件 seq 连续
6. 配置 + 绑核 → 验证: 压测 + LensX + perf(与 V1/V2 同颗粒度归档)
7. AF_XDPReceiver(零拷贝网络后端) → 验证: 用户自备真实设备, 对比 io_uring 延迟/吞吐
```

## 8. AF_XDP 网络后端（V3 实现内容）

- **目标**：突破 UDP 栈吞吐（临界 ~5.0M msg/s 被 io_uring 单线程焊死）。AF_XDP 驱动层零拷贝，绕过协议栈，UMEM 直搬 → 高吞吐 + 低延迟双收益。
- **实现**：新增 `AF_XDPReceiver`（基于 AF_XDP 的零拷贝网络后端，`IMarketDataReceiver` 的第二个实现），业务代码一行不改。**在模拟行情源下对比 io_uring 与 AF_XDP 的延迟/吞吐差异**。
- **依赖硬件**：AF_XDP 零拷贝需网卡驱动支持 + 真实网卡（驱动层零拷贝，本地回环无驱动层、无法体现收益）。**测试环境由用户自备**（两台设备网线直连）。
- **与 SPMC 多解析器的关系**：AF_XDP 解除 UDP 栈封顶后，SPMC 多解析器的高吞吐并行能力才用得上（本地回环阶段 SPSC 单解析器是延迟最优，V2.3 实测）。

**排除项**：DPDK（网卡不支持）；L2 真实行情（V5，后续阶段，走 IMarketDataReceiver 抽象，业务代码不改）。

## 9. 与 V2 的关系

- V2.1 已定稿 registry（locate→owner 原子数组）+ 双键负载均衡。V3 只改**路由位置**（消费者 skip → 生产者分发），registry/均衡/仲裁/无锁池全部复用。
- **一次只改一个变量**：V3 只动队列 + 路由，订单簿/策略/无锁池不动，才能归因"队列改造"的收益。
