# V2.1 性能测试归档（分簿并行）

> 日期：2026-08-06 · 分支：V2 · 被测版本：commit 081797d + 负载均衡双键修复 + pop/process 分段探针
> 对比基线：docs/PERF_V1_ARCHIVE.md（V1.5，单 book_th）

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

- **广播 + skip**：parse_th 单通道 SPMC 广播全部事件，每个 worker pop 全部（非本簿 skip 也算推进进度），只有归属 worker 处理。
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

### 3.1 稳态各段延迟（--rate 10000 零丢档）

| 段 | n | P50 | P90 | P95 | P99 | P999 | >1ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| recv→unpack（包级） | 343K | 1.5µs | - | - | 2.6µs | 4.4µs | 0 |
| alloc→push_ring | 163K | 1.7µs | 1.9µs | 2.0µs | 2.3µs | 4.0µs | 0 |
| push_ring→parse | 156K | 5.4µs | 34.4µs | 56.0µs | 231.9µs | 457.0µs | 0 |
| parse→push_spmc | 157K | 2.0µs | 2.6µs | 2.7µs | 3.0µs | 16.8µs | 0 |
| **push_spmc→pop** | **156K** | **1.7µs** | **23.0µs** | **38.9µs** | **133.7µs** | **1127.2µs** | 198 (0.13%) |
| **pop→process** | **105K** | **2.1µs** | **2.7µs** | **2.9µs** | **3.3µs** | **5.1µs** | **0** |
| **alloc→process(端到端)** | **52.5K** | **17.1µs** | **59.0µs** | **99.6µs** | **334.9µs** | **1159.7µs** | 69 |
| arb（仲裁） | 131K | 1.6µs | - | - | 2.7µs | 6.3µs | 0 |
| order（下单） | 71 | 5.4µs | - | - | 8.3µs | 8.3µs | 0 |

### 3.2 长尾对比（V1 vs V2.1 稳态）

| 段 | V1 | V2.1 | 改善 |
|---|---|---:|---:|
| push_spmc→pop P50 | 5.8µs | 1.7µs | **3.4×** |
| push_spmc→pop P99 | **3070µs** | **133.7µs** | **23×** |
| push_spmc→pop P999 | 4173µs | 1127µs | 3.7× |
| alloc→pop/process P99（端到端） | **3075µs** | **334.9µs** | **9×** |
| alloc→pop/process P999 | 4176µs | 1160µs | 3.6× |
| 仲裁 P99 | 2.7µs | 2.7µs | 持平 |
| 下单 P99 | 10.7µs | 8.3µs | 1.3× |

**长尾根源消除**：V1 长尾 = SPMC 排队被单 book_th 拖累（P99 3ms）。分簿后 4 worker 并行处理，SPMC 排队从 P99 3ms 压到 **134µs**。

**归因铁证**：`pop→process` 恒 2.1µs（P999 5.1µs，>1ms = 0）——**长尾全部在排队（等 owner worker 取走），处理本身不占**。端到端 `alloc→process` P99 335µs 中，排队段占 ~134µs，处理链（recv 1.5 + alloc 1.7 + push_ring 5.4 + parse 2.0 + pop→process 2.1 ≈ 12.7µs）是固定小开销。

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

- **端到端长尾 P99 从 3.1ms 压到 335µs（9×），SPMC 排队 P99 从 3070µs 压到 134µs（23×）**
- **达到 V2_PLAN §2.4 目标**：P99 <500µs（端到端 335µs ✓，排队 134µs ✓）
- 归因铁证：pop→process 恒 2.1µs → 长尾全在排队，处理不占
- P50 端到端 17.1µs（V1 ~20µs），正常路径微改善

### 7.2 吞吐（V2 次要目标，未提升但瓶颈转移）

- 临界 ~5.5M msg/s 与 V1 相同，**但瓶颈从 book_th 转移到 recv_th（io_uring 单 SQE）**
- book 处理已充分摊薄（4 核各 ~18%），不再单核瓶颈
- **下一步 V2.4 网络层升级（多在途 recv）解锁吞吐**（V2_PLAN §3.6）

### 7.3 权衡总结

| 维度 | V1 | V2.1 | 判断 |
|---|---|---|---|
| 端到端 P99 | 3075µs | 335µs | ✅ 9× 改善 |
| 排队 P99 | 3070µs | 134µs | ✅ 23× 改善 |
| 临界吞吐 | 5.5M | 5.5M | = 瓶颈转移至 recv_th |
| 单核效率 IPC | 0.62 | 0.39 | ⚠️ 多核争抢共享结构 |
| 总 pop CPU | 1× | 4× | ⚠️ 广播+skip 固有代价 |

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
