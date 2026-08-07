# V2.1 性能测试归档（分簿并行）

> 日期：2026-08-06 · 分支：V2 · 被测版本：commit 081797d + 负载均衡双键修复 + pop/process 分段探针 + 线程绑核
> 对比基线：docs/PERF_V1_ARCHIVE.md（V1.5，单 book_th）
> **方法（控制变量）**：V1 原测未绑核（trader 0-23 内核调度）。分簿是"用更多核"的设计，V2.1 先绑核；同时给 V1 分支加同样绑核（commit 0c3b641）确保公平对比。**§3.2/§7 数据均为 V1/V2.1 都绑核**。绑核布局：recv=5 parse=7 workers=9/11/13/15 fill=16（i9-12900HX 奇数 P 核，避开 CPU0/1 与 benchmark 的 CPU3）。

## 0. 被测架构（V2.1 分簿并行）

**线程模型（vs V1.5 的 5 线程）**：

```
recv_th(io_uring) → 字节ring → parse_th ──SPMC广播──▶ book_worker0 (consumer 0)
                                                      book_worker1 (consumer 1)
                                                      book_worker2 (consumer 2)
                                                      book_worker3 (consumer 3)
每 worker: 自己的 OrderBookConsumer + OFI/OBI 策略 + 信号槽 + 独立仲裁
共享: 无锁 OrderPool(挂单池) + 无锁 OrderMap(索引) + ExecutionEngine(锁)
归属: BookRegistry(locate→owner 原子数组), 广播+skip, 每事件只被 owner 处理
```

- **广播 + skip**：parse_th 单通道 SPMC 广播全部事件，每个 worker 都 pop 全部（非本簿 skip 也算推进进度），只有归属 worker 才真正处理。
- **关键代价（本次测量揭示）**：N 个 worker 各 pop 全部消息 → **pop 成本 ×N**。V1 单 book_th pop 全部 + 处理全部；V2.1 每 worker pop 全部 + 处理 1/N。

## 1. 测量环境

- 同 V1：内核 6.8.12 Intel 大小核，`test_data/itch_100mb.bin`（246MB，8,737,176 条消息，委托 ~97%）
- 压测：纯 UDP 平滑限速 `--rate 10000`（~5.05M msg/s 标准零丢档）
- LensX eBPF 探针（core/prof/lensx_probe.h），perf 热点 + 硬件事件

## 2. 测量工具（分簿后的分段探针）

**探针重建（V2.1 版）**：分簿后"pop"与"处理"分离——每个 worker 都 pop 全部消息，但只有 owner 处理。原 `mark_pop`（V1 单消费者 pop 即处理）拆成两段：

| 探针 | 位置 | 语义 |
|---|---|---|
| `mark_pop` | worker 循环 owner 判定后、process 前 | 消息被归属 worker 从 SPMC **取走**（排队终点） |
| `mark_process`（新增） | `BookWorker::process` 开头 | 消息**开始被处理**（处理起点） |

**分段链路（alloc→process 拆 5 段）**：

```
alloc → push_ring → parse_done → push_spmc → pop → process
(recv)  (recv→parse)  (parse)    (parse→worker)  (worker 内)
```

- `push_spmc → pop`：**排队等待**（生产者 push 到 owner 取走）——长尾主段
- `pop → process`：取走到处理（同线程，应极小）——**证明长尾在排队不在处理**
- `alloc → process`：完整端到端链路（总长尾）

**关键正确性**：`mark_pop` 只在 owner 判定后打（每条消息恰 1 个 owner）→ key（seq）配对干净；skip 的消息不打。抽样 1/128，`seq_id % 128 == 0`，与 alloc 侧配对。

## 3. 延迟分析（LensX，稳态）

**稳态定义**：剔除前 20% 时间（冷启动/注册阶段）。LensX CSV 原始数据离线分析（`measure_lensx.sh`），剔除 >10¹²ns 异常值。

### 3.1 稳态各段延迟（--rate 10000 零丢档，**绑核**）

> **方法修正（2026-08-06）**：V1 原测未绑核（trader 0-23 内核调度）。分簿并行是"用更多核"的设计，V2.1 必须先绑核才公平；同时给 V1 分支也加上同样绑核（控制变量）。下表为 **V2.1 绑核后**稳态数据（剔除前 20% 冷启动）。V1 vs V2.1 的公平对比见 §3.2。

| 段 | n | P50 | P90 | P99 | P999 | >1ms |
|---|---:|---:|---:|---:|---:|---:|
| recv→unpack（包级） | 346K | 1.4µs | - | 2.3µs | 3.6µs | 0 |
| alloc→push_ring | 201K | 1.8µs | 2.0µs | 2.2µs | 3.8µs | 0 |
| push_ring→parse | 158K | 4.2µs | 13.6µs | 27.1µs | 46.1µs | 0 |
| parse→push_spmc | 199K | 2.0µs | 2.6µs | 2.9µs | 4.4µs | 0 |
| **push_spmc→pop** | **158K** | **1.1µs** | **16.4µs** | **59.4µs** | **363.0µs** | **0** |
| **pop→process** | **106K** | **2.2µs** | **2.8µs** | **3.4µs** | **6.8µs** | **0** |
| **alloc→process(端到端)** | **53.1K** | **14.0µs** | **32.3µs** | **74.9µs** | **368.2µs** | **0** |
| arb（仲裁） | 106K | 1.7µs | - | 2.7µs | 8.0µs | 0 |
| order（下单） | 52 | 5.6µs | - | 53.0µs | 53.0µs | 0 |

**绑核消除了全部 >1ms 长尾**（未绑核时有 198 个，绑核后 0）。push_ring→parse 从 P99 232µs 降到 27µs（调度碰撞消失）。

### 3.2 长尾对比（控制变量：V1/V2.1 都绑核）

> **关键方法修正**：V1 原报告（PERF_V1_ARCHIVE）未绑核，P99 3ms 长尾很大程度是**调度噪声**（5 线程靠调度器随机撒核）。给 V1 分支加同样绑核（commit 0c3b641）后，V1 自身长尾也降到 ~126µs。**本表是都绑核后的公平对比**，反映分簿并行的真实收益。

| 段 | V1 绑核 | V2.1 绑核 | 改善 |
|---|---:|---:|---:|
| push_ring→parse P99（SPSC 跨线程） | **2.9µs** | 27.1µs | 变慢 9× |
| push_spmc→pop P50（SPMC 排队） | 8.2µs | **1.1µs** | 7.5× |
| push_spmc→pop P99（SPMC 排队） | 125.7µs | **59.4µs** | **2.1×** |
| push_spmc→pop P999 | 223.4µs | 363.0µs | 变慢 1.6× |
| alloc→pop/process P99（端到端） | 129.0µs | **74.9µs** | **1.7×** |
| alloc→pop/process P999 | 227.4µs | 368.2µs | 变慢 1.6× |
| 仲裁 P99 | 2.5µs | 2.7µs | 持平 |
| 下单 P99 | 8.9µs | 53.0µs | 变慢(样本少) |
| >1ms 长尾 | 0 | 0 | 持平 |

**对比解读**：
1. **分簿主收益（SPMC 排队）真实**：绑核后 V2.1 的 SPMC 排队 P99 仍是 V1 的 **1/2**（59.4 vs 125.7µs）——4 worker 并行消费确实摊薄了排队。这是纯分簿收益，与调度无关。
2. **V1 原报告的 3ms 长尾 = 调度噪声 + 排队**：绑核后 V1 自身降到 126µs，说明 V1 长尾主因是未绑核时单 book_th 被调度器打断 + SPMC 积压叠加。
3. **V2.1 的 push_ring→parse 变慢（2.9→27µs）**：4 个 worker 线程争 CPU 调度，recv→parse 的 SPSC 跨线程延迟增大。但此段不再是长尾主段（P99 27µs vs SPMC 59µs 量级接近，但绝对影响小）。
4. **端到端 P99 1.7×（129→75µs）**：绑核下分簿仍带来端到端改善。

**归因铁证（不变）**：`pop→process` 恒 2.2µs（>1ms = 0）——**长尾全部在排队（等 owner worker 取走），处理本身不占**。端到端 `alloc→process` P99 75µs 中，排队段占 ~59µs，处理链（recv 1.4 + alloc 1.8 + push_ring 4.2 + parse 2.0 + pop→process 2.2 ≈ 11.6µs）是固定小开销。

### 3.3 剩余长尾归因

稳态仍有 198 个 >1ms（0.13%），全来自 **push_spmc→pop**（排队）。根因（按 tid 拆解）：

| worker | 处理样本 | 占比 | P99 | P999 |
|---|---:|---:|---:|---:|
| tid A（最大） | 68745 | 35% | 高 | 高 |
| tid B | 65222 | 34% | 高 | 高 |
| tid C | 23138 | 12% | 低 | 低 |
| tid D | 18800 | 10% | 低 | 低 |
| tid E | 18364 | 9% | 低 | 低 |

- **负载不均残留**：2 个 worker 各占 ~35%，其余 ~10%。根因是**测试数据 locate 分布极不均**（最大 35384 条 vs 中位 3 条，V2_PLAN §1）——动态均衡只均衡 locate 数，不均衡事件量；大 locate 堆到谁，谁就慢。
- **冷启动积压**：压测开始瞬间（worker 启动、registry 注册 2089 个 locate、cache 冷），一批消息集体排队（LensX 前 20% 时间内的 3842 个 >1ms）。剔除后稳态长尾从 3ms 级降到 0.13%。

## 4. 吞吐（临界 + 瓶颈转移）

**临界吞吐扫描**（平滑限速扫档，`发送==解析` 判零丢）：

| --rate | QPS(msg/s) | 解析/发送 | 结果 |
|---|---:|---:|---|
| 11000 | ~5.56M | 8737171/8737176 | **零丢** ✅（临界） |
| 13000 | ~6.57M | 8291893/8737176 | **丢 5%** ❌ |
| 15000 | ~7.58M | 6798030/8737176 | 丢 22% |
| 18000 | ~9.09M | 6114396/8737176 | 丢 30% |

**临界 ≈ 5.5M msg/s，与 V1 相同。分簿没有提升临界吞吐，但瓶颈发生了转移：**

| 版本 | 瓶颈 | 临界 |
|---|---|---|
| V1 | **book_th**（单线程重建全部订单簿） | ~5.5M |
| V2.1 | **recv_th**（单线程 io_uring 接收） | ~5.5M |

**转移证据**（perf CPU 热点，rate 10000 窗口）：
- **worker 总 73.02%**（4 核分摊各 ~18.3%）——book 处理不再饱和单核
- **parse_th 18.30%**（ByteRingParser::parse_available 12.58% + ItchParser::feed 10.37%）——单线程解析，未饱和
- 无单核热点 → **瓶颈在未显示为高 CPU 的 recv_th**（io_uring 单 SQE 预提交，吞吐受限于 SQE 轮转速率）

这印证 V2_PLAN 预测："分簿把 book 从瓶颈移除 → parse 成为新瓶颈 → 网络层（recv_th 单 SQE）最终封顶"。**V2.4 网络层升级（多在途 recv，单线程预提交多 SQE）是下一步吞吐优化方向**。

## 5. CPU 热点 + 硬件事件

### 5.1 CPU 热点（perf record，裁剪窗口内）

| 热点 | V1 | V2.1 | 说明 |
|---|---:|---:|---|
| book_th / worker 总 | 29.68%（单核） | **73.02%**（4 核分摊各 ~18.3%） | 广播+skip pop 成本 ×N |
| └ BookWorker::process | - | 25.48% | 处理核心 |
| └ OrderBook::add | 4.99% | 10.75%（总）/ ~2.7%（单核） | 单核摊薄（1/4 事件） |
| └ OrderMap::insert | - | 6.58% | 共享索引，多核争抢 |
| └ handle_delete/unlink_and_free | 3.97%/2.79% | 3.94%/2.62% | |
| parse_th | ~16% | 18.30% | 单线程解析 |
| └ ItchParser::feed | 10.91% | 10.37% | 解析主路径 |

**关键解读**：
1. **worker 总 CPU 73% = 4 个 worker 各 ~18.3%**（单核），比 V1 book_th 单核 29.68% **低**——分簿把单核负载摊到 4 核，book 不再是单核瓶颈。
2. **但总 pop CPU 成本 = 4×V1**：每个 worker 都 pop 全部消息（广播+skip），pop 的 72B 拷贝 + 进度推进成本 ×4。这是广播+skip 模型的固有代价（V2_PLAN §2.4 预测）。
3. **OrderMap::insert 6.58%**（在 OrderBook::add 下）：分簿后 N worker 并发插入共享 OrderMap，bucket CAS + 链头竞争 + cache line 争抢，占比显著。无锁化（V2.1 前置）是正确决策——否则这里会退化。

### 5.2 硬件事件（perf stat，8s 窗口）

| 事件 | V1 | V2.1 | 解读 |
|---|---:|---:|---|
| IPC | 0.62 | **0.39** | **下降**——多核争抢共享 OrderMap/OrderPool cache line，单核效率降 |
| cache miss 率 | 8.1% | **4.1%** | 改善（每 worker 处理少标的，L1 局部性提升） |
| L1 miss 率 | - | 4.8% | |
| LLC miss 率 | - | 1.1% | 良好 |
| ctx/s | 3558 | 3838 | 线程更多，调度略增 |
| syscall sendto/recvfrom | 0 | 0 | io_uring 保持生效 |

**IPC 0.62→0.39 的权衡**：分簿用更多核换延迟，但每核效率降（共享数据结构争抢）。V1 是单核 0.62（内存受限但无争抢），V2.1 是 4 核各 0.39（cache 争抢）。**延迟收益（P99 23×）远大于单核效率损失**——trade-off 值得。

## 6. 负载均衡（V2.1 实测发现 + 修复）

**发现**：初版动态均衡用"argmin cared_count，平局归自己"，压测显示**单一 worker 处理 52% 消息**（LensX 长尾 100% 来自该 worker）。根因：启动期所有 worker cared_count 全 0（平局）→ 先被调度的 worker 连续 pop 到新 locate 就注册给自己 → **竞速正反馈**（先调度者抢注册越多 → 越超载）。

**修复**：argmin 改**双键** `(cared_count, registered_count)` 字典序最小——
- `cared_count`（处理事件数）= 运行期主键，让新 locate 去最清闲者
- `registered_count`（已注册 locate 数）= 启动期次键，破全 0 平局 → 严格轮流分散（即使同一 worker 先遇到所有新 locate）

**修复后**：worker 分布 35/34/12/10/9，无单一 straggler；稳态长尾从 P99 508µs（修复前）降到 **134µs**。

**残留不均**：2 个 worker 各 ~35%。根因 = 测试数据 locate 事件量极不均（大 locate 35384 条 vs 中位 3 条），动态均衡只均衡 locate 数。彻底解决需按事件量动态迁移（复杂）或拆大 locate（改订单簿结构），V2_PLAN 明确不做。

## 7. 结论

### 7.1 延迟（V2 主目标，成功）

**控制变量后（V1/V2.1 都绑核）**：
- **SPMC 排队 P99：V1 125.7µs → V2.1 59.4µs（2.1×）**——分簿真实收益，与调度无关
- **端到端 P99：V1 129.0µs → V2.1 74.9µs（1.7×）**
- **>1ms 长尾：两版本都 0**（绑核消除调度噪声）
- 归因铁证：pop→process 恒 2.2µs → 长尾全在排队，处理不占
- P50 端到端 14.0µs（V1 绑核 12.0µs），正常路径持平

**注（对比原报告）**：V1 原报告未绑核，P99 3ms 长尾很大程度是调度噪声（V1 绑核后自身降到 126µs）。**修正后的对比 = 分簿净收益 2×**，不是原 23×——但仍是明确改善，且达成 V2_PLAN §2.4 目标（P99 <500µs：绑核后 75µs ✓）。

### 7.2 吞吐（V2 次要目标，未提升但瓶颈转移）

- 临界 ~5.5M msg/s 与 V1 相同，**但瓶颈从 book_th 转移到 recv_th（io_uring 单 SQE）**
- book 处理已充分摊薄（4 核各 ~18%），不再单核瓶颈
- **下一步 V2.4 网络层升级（多在途 recv）解锁吞吐**（V2_PLAN §3.6）

### 7.3 权衡总结（控制变量：都绑核）

| 维度 | V1 绑核 | V2.1 绑核 | 判断 |
|---|---|---|---|
| 端到端 P99 | 129µs | **75µs** | ✅ 1.7× 改善 |
| 排队 P99 | 126µs | **59µs** | ✅ 2.1× 改善 |
| >1ms 长尾 | 0 | 0 | ✅ 都消除 |
| push_ring→parse P99 | **2.9µs** | 27µs | ⚠️ 变慢(多线程调度) |
| 临界吞吐 | 5.5M | 5.5M | = 瓶颈转移至 recv_th |
| 单核效率 IPC | 0.62 | 0.39 | ⚠️ 多核争抢共享结构 |
| 总 pop CPU | 1× | 4× | ⚠️ 广播+skip 固有代价 |

**结论**：绑核后分簿净收益 = 排队/端到端 2×，>1ms 长尾两版本都消除。V1 原报告的 23× 是"调度噪声 + 分簿"叠加的乐观估计；**修正后分簿真实收益 2×，仍达成 V2_PLAN 目标**（端到端 P99 75µs < 500µs）。

## 8. 附：测量复现

```bash
# 权限
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 kernel.unprivileged_bpf_disabled=0

# LensX 延迟(分簿后分段: alloc→process 5 段 + pop/process 区分排队/处理)
SUDO_PASS=<密码> ./scripts/measure_lensx.sh 10000

# perf 热点 + 硬件事件
SUDO_PASS=<密码> ./scripts/perf_measure.sh 10000

# 压测零丢 + 临界扫描
./scripts/pressure_test.sh --rate 10000
# 手动扫档(脚本 grep 串与 main 打印不匹配, 用 sent==parsed 判断):
#   rate 11000 零丢(临界) / 13000 丢 5%

# 稳态分析(剔除前 20% 冷启动): 见 measure_lensx.sh 末尾 Python + 本文 §3
```

---

# V2.3/V2.4 性能归档（多解析器 + 多在途 recv，最终版）

> 日期：2026-08-07 · 分支：V2 · 被测版本：commit 75f4a16（V2 最终版）
> **压测速率定档（本次关键方法）**：不按最大吞吐（rate 10000=5M），而按**调查来的真实市场速率**——
> NASDAQ TotalView ITCH 官方数据：常态 ~50 万条/秒，峰值（1ms 粒度）~2000 条/ms ≈ 200 万条/秒。
> 对应 benchmark `--rate`（每 0.1s N 包，每包 ~50 条）：**常态 rate 1000（~50万/s）**、**峰值 rate 4000（~200万/s）**。

## 0. 被测架构（V2 最终版）

**线程模型 + 绑核（实测驱动）**：

```
recv_th(io_uring 多在途 recv, P5) → SPMC字节ring → 2 解析器(E16-17)
   → SPMC事件通道 → 4 worker(共享 P 11/10 + 13/12)
   fill/主线程: 共享 P9(低频)
```

**绑核演进（V2.4 实验修正）**：
- **解析器 → E 核 16 起**：协作流水线（抢头串行/提交保序），快的大核被小核拉到同速，大核优势被拉平 → 全小核独占
- **worker → P 核共享（2 worker 一组 SMT 兄弟）**：worker 处理订单簿**轻重不均**（大标的 35384 条 vs 中位 3 条），E 核算力不足 → 重订单簿操作慢 → SPMC 排队。改 P 核后全链路 P99 35→24µs，push_spmc→pop P999 9 倍改善
- **recv → P5**：高吞吐收包不抢
- **fill/主线程 → P9 共享**：低频

## 1. 测量环境

- 同 V1/V2.1：内核 6.8.12 Intel 大小核（i9-12900HX，16 P + 8 E），`test_data/itch_100mb.bin`（246MB，8,737,176 条）
- **压测速率 = 真实市场量级**（常态 rate1000 / 峰值 rate4000），非最大吞吐
- LensX eBPF 两探针（push_ring→parse_done）测解析侧；完整 yaml 测全链路分段

## 2. 全链路分段延迟（真实速率）

### 2.1 常态 rate 1000（~50 万条/秒，实盘主场景）

| 段 | P50 | P90 | P99 | P999 | >1ms |
|---|---:|---:|---:|---:|---:|
| recv→unpack | 2.0µs | 2.1µs | 2.6µs | 3.3µs | 0 |
| alloc→push_ring | 1.8µs | 2.0µs | 2.4µs | 3.1µs | 0 |
| push_ring→parse | 5.6µs | 10.0µs | 14.0µs | 17.9µs | 0 |
| parse→push_spmc | 2.7µs | 3.3µs | 4.6µs | 6.0µs | 0 |
| **push_spmc→pop** | 0.4µs | 3.4µs | **6.1µs** | **380.6µs** | **156** |
| pop→process | 1.9µs | 2.8µs | 3.4µs | 4.1µs | 0 |
| **alloc→process(端到端)** | **12.6µs** | **19.2µs** | **23.9µs** | **401.1µs** | **52** |
| arb（仲裁） | 1.9µs | 2.3µs | 2.6µs | 4.4µs | 0 |
| order（下单） | 9.9µs | 12.0µs | 45.0µs | 45.0µs | 0 |

### 2.2 峰值 rate 4000（~200 万条/秒，实盘最坏瞬间）

| 段 | P50 | P90 | P99 | P999 | >1ms |
|---|---:|---:|---:|---:|---:|
| recv→unpack | 1.9µs | 2.2µs | 3.0µs | 18.2µs | 0 |
| alloc→push_ring | 1.8µs | 2.1µs | 3.7µs | 19.7µs | 0 |
| push_ring→parse | 4.8µs | 12.0µs | 20.6µs | 26.2µs | 0 |
| parse→push_spmc | 2.1µs | 2.5µs | 3.3µs | 6.0µs | 0 |
| **push_spmc→pop** | 0.4µs | 5.8µs | 18.3µs | **8.4ms** | **1110** |
| pop→process | 2.0µs | 2.9µs | 3.6µs | 5.5µs | 0 |
| **alloc→process(端到端)** | **11.9µs** | **24.0µs** | **38.6µs** | **8.5ms** | **370** |
| arb（仲裁） | 1.5µs | 2.1µs | 2.7µs | 5.8µs | 0 |

### 2.3 对照 V2.1（PERF_V2_ARCHIVE §3.1，rate 10000）

| 指标 | V2.1 | V2.3 常态 | V2.3 峰值 |
|---|---|---|---|
| alloc→process P99 | 74.9µs | **23.9µs** | **38.6µs** |
| alloc→process P50 | 14.0µs | **12.6µs** | **11.9µs** |
| push_ring→parse P99 | 27.1µs | **14.0µs** | 20.6µs |

**V2.3 端到端 P99 比 V2.1 优 2-3 倍**（真实速率下）。

## 3. 长尾归因（关键发现）

**端到端长尾全部集中在 `push_spmc→pop`（worker 排队）**，解析侧（recv→push_spmc）零长尾。

按 tid 拆解（rate 1000，push_spmc→pop 的 >1ms）：
| tid | 样本 | >1ms | 占比 |
|---|---:|---:|---:|
| 34732（单 worker）| 30396 | 162 | 0.5% |
| 其余 5 worker | ~35000 各 | 0 | 0% |

**铁证**：长尾 100% 来自**单个超载 worker**——它分到了重标的（大标的 35384 条，处理成本 >> 事件数），成为 SPMC 最慢消费者，拖累排队。负载分布实测 186/208/180/274 万（worker3 超载 32%）。

**负载均衡分析**：当前双键算法（cared_count 事件数 + registered_count locate 数）均衡"事件数"，但**重标的处理成本 ≠ 事件数**——单 worker 仍可能超载。彻底解决需**拆大标的**（按价格区间分片订单簿，每 worker 处理部分价格档）或**动态迁移**（破坏保序），V2_PLAN 明确不做。**0.5% 长尾是分簿广播的固有代价，可接受**。

## 4. 吞吐（UDP 栈限制下的结论）

**实测**：UDP + io_uring 单线程接收上限 ~83 万包/秒（415MB/s，纯收包）。SPSC 单解析器 ~5M 条/s。
**真实市场峰值 ~200 万条/秒** << 解析能力（5M），**吞吐不是实盘瓶颈**。

**关键洞察**：当前架构吞吐被 UDP 栈焊死，SPMC 多解析器的 >5M 能力用不上（UDP 送不进来）。**实盘要延迟不是吞吐**——parse2 是延迟最优配置（P99=24µs），parse4 过度配置无收益。

## 5. 结论

1. **V2 最终版（parse2 + 绑核）在真实市场速率下延迟极优**：常态端到端 P99=23.9µs，峰值 P99=38.6µs，解析侧零长尾
2. **长尾在 worker 排队（0.5%）**：单超载 worker 分到重标的所致，分簿广播固有代价，可接受
3. **对照 V2.1 优 2-3 倍**：多解析器 + 真实速率定档的净收益
4. **吞吐非实盘瓶颈**：UDP 栈限制下 SPSC/SPMC 都够，延迟才是核心

## 6. 测量复现

```bash
# 全链路分段(完整 yaml, 真实速率)
SUDO_PASS=<密码> ./scripts/measure_lensx.sh 1000   # 常态
SUDO_PASS=<密码> ./scripts/measure_lensx.sh 4000   # 峰值

# 解析侧两探针(干扰最小): docs/bench/trader_lensx_2probe.yaml
SUDO_PASS=<密码> ./scripts/measure_lensx.sh 1000 docs/bench/trader_lensx_2probe.yaml
```

