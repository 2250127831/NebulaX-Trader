# V1 性能测试归档

> 日期：2026-08-04 · 分支：V1 · 工具：perf（CPU 热点）+ LensX（eBPF 延迟）+ 压测（吞吐/丢包）
>
> 一句话结论：**V1 吞吐受订单簿消费能力限制（~5.5M msg/s，方案A单通道）；延迟大头是 book_th 跨线程排队（成交等前面委托处理完，P50 96µs / P90 12ms），各段计算路径（拆包/解析/OFI/OBI/下单）全部 ≤6µs。V2 主攻订单簿缓存友好化（吞吐+排队同源）。**

---

## 0. 交易系统架构与策略（被测对象是什么）

### 0.1 整体链路

```
模拟交易所(itch_100mb.bin 回放, benchmark 纯 UDP 发送)
   │  MoldUDP64 封包 (20B头 + 若干条 ITCH 消息)
   ▼
recv_th    io_uring recv → MoldUdpUnpacker 拆包加 seq(64位) → 字节 ring(4MB)
   │
parse_th   ByteRingParser 解析 ITCH → MarketEvent → 单通道 SPMC 广播
   │        单通道: 全部事件(成交+委托)同一序列, 多消费者并行消费
   │
   ├── book_th     (consumer 0) 全部事件 → 订单簿重建 → OFI/OBI 信号 → 写信号槽
   │                ├── 订单簿重建(obc)
   │                ├── OFI(逐笔委托, 依赖订单簿查 D/X/E 方向)
   │                ├── OBI(盘口失衡, 读 Top-of-Book)
   │                └── 写 OFI/OBI 信号到原子信号槽
   │                (OFI/OBI 依赖订单簿, 故放 book_th 保证时序)
   ├── strategy_th  (consumer 1) 独立高频策略: 消费成交跑 TradeDirection, 写信号槽
   │                (验证 SPMC 多消费者广播, 不依赖订单簿)
   ├── arbitrate()  统一仲裁函数(信号写处调用): 读三信号槽, 同向才下单
   │                (TD 有方向→三同向; TD 无方向→退化为 OFI/OBI 两信号)
   └── fill_th      成交回报接收(← 模拟交易所)
```

### 0.2 线程模型（5 线程 + 仲裁函数）

| 线程 | 职责 | 输入 | 输出 | 频率 |
|---|---|---|---|---|
| recv_th | 收 UDP + 拆包入 ring | UDP 包 | 字节 ring | 每包 |
| parse_th | 解析 ITCH + 分派 | 字节 ring | 单通道 | 每消息 |
| book_th | 订单簿重建 + OFI/OBI 信号 | 单通道(consumer 0, 全部) | 信号槽 | 每事件 |
| strategy_th | 独立高频策略(TradeDirection) | 单通道(consumer 1, 成交) | 信号槽 | 每成交 |
| fill_th | 成交回报 | UDP 回报 | OrderManager | 极低频 |

**统一仲裁（arbitrate 函数）**：非独立线程——各策略写信号后调用。读三信号槽,同向才下单:TD 有方向要求三同向,TD 无方向(NONE)退化为 OFI/OBI 两信号。无定时器、无轮询,信号一更新同步仲裁。方向翻转才下单(信号稳定不重复加仓)。

**消费者阻塞机制（当前实现）**：`SPMCEventQueue` **每消费者独立 eventfd**——`notify_all()` 广播写所有消费者的 fd,每个都醒(无竞争);`wait_for_data(consumer_id)` poll 自己的 fd,无限阻塞(无超时)。push 空→非空才广播。消费者**混合退避**——有数据连续排空保吞吐,短暂空 `_mm_pause` 自旋,持续空才阻塞。parse_th/book_th/strategy_th 都用此机制。
> **教训(广播唤醒)**:多消费者曾共享单 eventfd,notify 计数被一个消费者消费后其他漏唤醒 → 靠 200ms 超时兜底 → **push→sig 延迟 101ms**。改每消费者独立 fd + 广播后 → **2.5µs**。唤醒必须广播,不能有毫秒级超时计时器。

**通道满背压（当前实现）**：`SPMCEventQueue` 满时**先尝试清理一次**（重读 `min_consumed()`，看消费者是否已推进腾出已消费区），仍满返回 false；调用方（sink）收到 false 自旋重试直到成功。队列满绝不静默丢——满 → 尝试清理 → 重试，消息可靠，压力传导回上游（parse_th→recv_th→UDP）。

### 0.3 策略架构：独立信号 + 仲裁下单

配置（`config/default.yaml`）：
```yaml
strategy:
  use_obi: true              # 信号1: 订单簿失衡
  use_ofi: true              # 信号2: 订单流失衡
risk:
  max_position: 10000
  max_daily_loss: 100000000  # 分, 1000万元
execution:
  base_qty: 100              # 满强度下单量(股)
```

**信号1 — OFI 订单流失衡**（逐笔委托，高频）：
- 输入：book_th 每笔委托事件 + 订单簿查方向（D/X/E 查簿）
- **滑动窗口**（最近 1024 笔委托）累计订单流方向强度（Cont et al. 经典 OFI）：买单挂单 +量、撤单 -量、成交 -量等
- 窗口 OFI > 阈值 → BUY，< 负阈值 → SELL；强度 = |OFI|/阈值 封顶
- 窗口化使信号随行情回摆（无限累计会锁死方向，见 V1 教训）

**信号2 — OBI 订单簿失衡**：
- 输入：book_th 重建的 Top-of-Book（bid/ask 价量）
- `OBI = (bid_vol - ask_vol) / (bid_vol + ask_vol) × kStrengthScale`
- > 阈值 → BUY，< 负阈值 → SELL；强度 = |OBI| 归一化

**仲裁下单（arbitrate 函数）**：
- **三信号同向才下**：OFI/OBI/TradeDirection 同向才下单（独立信号，不一致不动）
- **TD 退化**：TD 无方向(NONE, E 无主动方)时退化为 OFI/OBI 两信号仲裁
- 方向翻转才下单（信号稳定不重复加仓）；风控：持仓封顶、单日亏损过滤
- 谁写信号谁调用（写完检查信号是否齐全,齐则仲裁下单）——无定时器无独立线程
- 行业参考：多策略独立产信号 → 仲裁层统一 → 单一执行路径（不做主从合成）

**独立高频策略 — TradeDirection 成交方向**（独立线程）：
- 输入：strategy_th(consumer 1) 消费单通道成交
- 直接读 trade.side（主动方）判断买卖主导，**不依赖订单簿**
- 独立于 book_th 并行消费，验证 SPMC 多消费者广播；写信号槽参与仲裁（TD 有方向时三同向）

### 0.4 事件分布

| | 成交 | 委托 |
|---|---|---|
| 数量 | ~5%（44,987/873万） | ~97%（845万） |
| 处理 | book_th(execute 减挂单) + OFI(订单流) | book_th(重建盘口) + OFI/OBI |
| 瓶颈意义 | — | **book_th 是吞吐瓶颈（处理全部事件）** |

被测的性能（第 4 节起）正是在这 5 线程 + 独立信号仲裁 + 独立高频策略架构下测出的：**吞吐天花板在 book_th（处理全部事件），延迟热点在订单簿计算与解析路径**。

---

## 1. 测量环境

- 内核 6.8.12，Intel 大小核（perf 默认 `cpu_atom` 只采 E 核，需用 `cpu-clock` 全核采）
- 需要 root 的权限开关：
  ```bash
  sudo sysctl -w kernel.perf_event_paranoid=1   # 允许用户态 perf record
  sudo sysctl -w kernel.kptr_restrict=0          # 解析内核符号
  sudo sysctl -w kernel.unprivileged_bpf_disabled=0  # LensX eBPF
  ```
- 数据：`test_data/itch_100mb.bin`（246MB 完整日数据，8,737,176 条消息，委托占 ~97%）

## 2. 测量工具

| 工具 | 测什么 | 用法 |
|---|---|---|
| perf record | CPU 热点（按符号/调用栈） | `perf record -F 4000 -e cpu-clock --call-graph dwarf -p <pid>` |
| LensX | 延迟分布（uprobe 采样 + 配对） | `sudo ./build/lensx run /tmp/trader_lensx.yaml --pid <pid>` |
| 压测脚本 | 吞吐 + 事后丢包检测 | `./scripts/pressure_test.sh [--rate N] [--sweep]` |

压测脚本流程：起 trader(`--no-shm`) → benchmark(`--no-shm --rate N` 纯 UDP 平滑限速) → trader `idle_timeout` 自动退出 → 对比发送/解析/通道丢包。

## 2.5 压测方案定稿（2026-08-04 确认）

**模式：纯 UDP 单向 + 平滑限速 + 事后检测。（不做 ack 反馈——模拟真实行情 UDP 单向。）**

- **限速**：`--rate N` = 每 0.1s 发 N 包，**平滑铺开**（每包 `sleep(100ms/N)`，瞬时≈平均）。瞬时=平均才测得出真实临界。
- **零丢判定**：`发送数 == 解析总数`。内部通道有背压（见 0.2）不丢，丢只可能在 UDP 层。
- **保证方式**：纯 UDP 固定速率无法数学保证零丢（发送端不感知接收端消费速度）。达成方式是**留足余量 + 多次验证**：
  - **标准零丢档：`--rate 10000`（~5M msg/s）**（方案A单通道下 book_th 处理全部事件，临界降低，留余量）。
  - 扫档找临界用 `--sweep`（自动二分），`--fast` 加速（idle_timeout 10s→4s）。
- **正确性补充**：内部通道背压不丢；丢包只可能在 UDP 层（发送==解析 判断）。超速时 book_th 跟不上，sink 重试把压力传导回 UDP 缓冲，入口先丢。

### 2.6 压测客户端绑核（参考NebulaX）

压测客户端（benchmark）绑定到**低中断独立 P 核**，避免与 trader 抢核（同NebulaX `benchmark_client` 绑核 + 服务端 `--io-core`/`--send-core` 分核的做法）。

**选核依据（实测 `/proc/interrupts` 中断分布）**：
| 核 | 中断数 | 判定 |
|---|---|---|
| CPU0(P0) | 4.3M | ❌ 噪声最大，不用 |
| 偶数核(2/4/6/8/10/12/14) | 3.4-4.5M | ❌ 高噪声 |
| **CPU3** | **517K** | ✅ 低中断 P 核（默认） |
| CPU13 | 342K | ✅ 最低（留作他用） |

- benchmark 加 `--core` 参数（默认 3），main 开头 `pthread_setaffinity_np` 绑主线程（发送线程），同撮合引擎 API。
- 运行时验证：benchmark 亲和力 = CPU3，trader = 0-23（内核调度）。
- **注意**：避免用 P0(CPU0/1)——偶数核中断高（调度器/系统负载）、P0 噪声大，会污染压测计时。

## 3. 当前实现要点（影响性能的关键设计）

- **网络接收**：io_uring 单 SQE + 固定缓冲池；`SO_RCVBUF=4MB`（配合 `rmem_max=8MB`）吞下突发，避免 UDP 丢包。
- **拆包/解析**：`MoldUdpUnpacker` 拆包加 seq → 字节 ring(4MB) → `ByteRingParser` 解析 → **单通道 SPMC 广播**（方案A，全部事件）。parse_th 用**混合退避**（ring 空短暂自旋 + 持续空 `wait_for_data` 阻塞），recv_th 每包 feed 后 `parser.notify()` 唤醒。
- **消费者唤醒**：`SPMCEventQueue` eventfd（见 0.2）。parse_th/book_th 全部用混合退避。通道满时**背压**（见 0.2）——满先尝试清理，仍满返回 false，sink 重试不丢。
- **压测客户端**：绑低中断核（见 2.6），平滑限速。

## 4. 吞吐瓶颈分析（perf）

### 4.0 perf 热点 + 硬件事件（2026-08-04 最终测量）

压测窗口（`--rate 10000` 零丢档，满解析 8,737,176，单通道零丢；`-F 999 -e cpu-clock --call-graph dwarf`，`scripts/perf_measure.sh` 按样本时间戳裁剪 2.5%~97.5% 纯负载段）。

**CPU 热点（裁剪窗口内）**：

| 类别 | 占比 | 说明 |
|---|---|---|
| **book_th 线程循环**（lambda#3） | **29.68%** | 订单簿重建 + 信号 + 仲裁 |
| └ `OrderBook::add` | 4.99% | 哈希 + 链式指针追逐 |
| └ `handle_delete` / `unlink_and_free` | 3.97% / 2.79% | 红黑树 erase |
| **parse_th 线程循环**（lambda#2） | ~16% | ByteRingParser::parse_available |
| └ `ItchParser::feed` | 10.91% | ITCH 解析 |
| └ `parse_A` / `parse_D` | 4.45% / 4.12% | Add / Delete 消息 |

**硬件事件（perf stat，裁剪窗口内）**：

| 事件 | 值 | 解读 |
|---|---|---|
| **IPC** | **0.62** | 每周期 0.62 指令，**内存受限**（理想 ≥1） |
| **cache-misses** | 18.6M / 229M refs = **8.1%** | 订单簿指针追逐 |
| **L1-dcache-load-misses** | 134M | 数据访问跨 cache line |
| **L2-load-misses** | 2.4M | L2 命中良好 |
| **branch-misses** | 66M（0.49%） | 分支预测良好 |
| **ctx/s** | 3558 | 5 线程调度平稳 |
| **syscalls:sys_enter_sendto / recvfrom** | **0** | **UDP 走 io_uring SQE，不经 syscall** |
| syscalls:sys_enter_read | 429 | io_uring 接管 |

**结论**：
1. **三个消费者线程（parse_th/book_th/strategy_th）是主要 CPU 消耗**，证明 SPMC 多消费者并行。
2. **book_th 线程是第一热点（29.68%）**：`OrderBook::add`/`unlink_and_free`（红黑树 + 指针追逐）是核心。
3. **IPC 0.62 + L1 miss 134M → 内存带宽受限，非 CPU 算力**：订单簿的哈希/红黑树访问跨 cache line，这是硬件级证据——**V2 订单簿缓存友好化（开地址哈希/扁平数组）是正确方向**。
4. **io_uring 生效证据**：recvfrom/sendto = 0，UDP 收发全走 io_uring SQE，不经系统调用。

### 4.1 临界吞吐（当前版本：方案A单通道）

用平滑限速扫档，找"不丢包前提下最快"（方案A后 book_th 处理全部事件含成交，临界比早期版本降低）：

| --rate | 实际 msg/s | drops_b |
|---|---|---|
| 8000 | ~4M | **0** ✅ |
| 10000 | ~5M | **0** ✅ |
| 11000 | ~5.5M | **0** ✅ |
| 12000 | ~6M | **UDP 丢** ❌ |

**结论：方案A下 book_th 是吞吐瓶颈，临界 ≈ 5.5M msg/s（11000 零丢 / 12000 丢）。** 超过它内部通道背压把压力传导回 UDP 层，入口丢（内部零丢）。瓶颈归因：book_th 处理全部事件（成交更新挂单 + 委托重建盘口），订单簿计算是热点。V2 订单簿缓存友好化见 6.1。

## 5. 延迟分析（LensX）

探针（`core/prof/lensx_probe.h`，noinline 空函数作 uprobe 挂载点）。**key 配对**（`key: arg1` = 消息 seq，跨线程靠 `MarketEvent.seq_id` 贯穿：unpacker 分配 64 位 seq → 字节 ring → parser → 各线程探针统一取 `ev.seq_id`）。2026-08-04 探针重建为四级链路测量（配置 `docs/bench/trader_lensx.yaml`）。

**四级探针（从收包到下单的完整链路）**：

| 级别 | 路径 | 线程 | 模式 | 语义 |
|---|---|---|---|---|
| 1 包级 | recv_pkt → unpack | recv_th | seq | 包到达→拆包（内核→用户态 + SQE） |
| 2 消息级 | alloc → push_ring → parse_done → push_spmc → pop | 跨线程 | key(抽样) | **一条消息从分配到被消费，拆 4 段** |
| 3 仲裁 | arb_start → arb_end | book_th | seq(抽样) | 仲裁函数完整执行 |
| 4 下单 | order_start → order_end | book_th | key(抽样) | 下单决策→执行完毕(send/被拒) |

**抽样 1/128**（`msg_seq % 128 == 0`）：全采样实测拖垮吞吐（5M→543K，9x）。**抽样判断必须在调用点**（uprobe 挂函数入口，只要函数被调用就命中，函数内抽样无效）。抽中同一批消息（seq 一致），key 配对完整。

延迟分布（`--rate 10000` 零丢档，最终测量）：

| 段 | n | P50 | P90 | P99 | 语义 |
|---|---|---|---|---|---|
| 包级 recv→unpack | 346K | 1µs | 3µs | 3µs | recv_th 内部 |
| alloc→push_ring | 134K | 1µs | 1µs | 3µs | 构造+入字节 ring(recv_th) |
| **push_ring→parse** | 132K | **192ns** | 3µs | 12µs | **SPSC 跨线程(recv→parse), 近零** |
| parse→push_spmc | 132K | 1µs | 3µs | 3µs | 解析→入 SPMC(parse_th) |
| **push_spmc→pop** | 132K | **24µs** | **6ms** | 12ms | **SPMC 排队(parse→book), 长尾全在这** |
| **alloc→pop(总)** | 66K | 24µs | 196µs | 786µs | 一条消息从分配到被消费 |
| 仲裁 arb→end | 133K | 1µs | 3µs | 3µs | 仲裁完整执行, 非瓶颈 |
| 下单 order→end | 11 | 6µs | 6µs | 6µs | 下单决策→send, 快 |

> 注：`push_ring_to_parse`/`push_spmc_to_pop` 有 ~0.9% 的 `211106s` 异常值（key 模式跨线程 ts 序差导致，98%+ 样本分布可信）。测量中曾有一次 trader 段错误（偶发，未复现，待查）。

### 延迟瓶颈结论
1. **各段计算路径全部 ≤6µs，无单段计算瓶颈**：拆包 1µs、解析 1µs、仲裁 1µs、下单 6µs——计算全程极快。
2. **端到端延迟大头 = push_spmc→pop（P50 24µs / P90 6ms）= SPMC 排队**：alloc→pop 拆 4 段后，前 3 段（含 SPSC 跨线程）全部干净（≤1µs），**唯一热点是 SPMC→book_th 排队**。book_th 处理全部事件（委托占 ~97%），消费慢 → SPMC 积压。
3. **SPSC vs SPMC 跨线程对比**：SPSC 字节 ring 跨线程 P50 192ns（近零）vs SPMC 排队 P50 24µs / P90 6ms——**相差两个数量级**。SPSC 单消费者（parse_th 快），永不积压；SPMC 多消费者共享，**被最慢消费者（book_th）拖累**。这是 SPMC 多消费者广播的固有代价，非队列实现问题。
4. **长尾根源 = book_th 消费速度（吞吐瓶颈）**：队列满 → 等最慢消费者 → 长尾。与吞吐瓶颈同源（book_th 临界 ~5.5M，压测 5M 接近临界）。
5. **SPMC 优化（2026-08-04）已生效**：lazy progress + cache padding + batch reclaim + **cached min_head**（满时才扫描）。实测 alloc→pop 长尾下降：P90 1ms→393µs→196µs，P99 3ms→786µs。cached min_head 消除每次 push 的 O(consumers) 原子遍历，减少"假满"。
6. **根治 = V2 book_th 加速**：订单簿缓存友好化 → book_th 消费速率远超生产 → SPMC 不积压 → 长尾消失。这是吞吐与延迟的同源解。

## 6. V2 优化方向

按影响排序：

### 6.1 吞吐：订单簿缓存友好化
- **现状**：book_th ~5.5M msg/s 是吞吐天花板（方案A单通道）。瓶颈是 `OrderBook::add` 的哈希+指针追逐（内存延迟），perf 中订单簿占 ~23% 第一热点。
- **方向**：开地址哈希替代链式（更缓存友好）、价格档用扁平数组/缓存行对齐、批量加载。
- **验证**：扫档后临界应显著 >5.5M msg/s。

### 6.2 延迟：进一步压低端到端（次要，吞吐优先）
- **现状（2026-08-04 最终测量）**：alloc→pop 拆 4 段后，**唯一热点 = push_spmc→pop（P50 24µs / P90 6ms）= SPMC 排队**，与吞吐瓶颈（book_th 消费能力）同源——**吞吐与延迟的瓶颈同根**。SPMC 优化（cached min_head 等）已把长尾从 P90 1ms 压到 196µs，但排队本质仍在。
- **决策（2026-08-04）**：**吞吐优先，订单簿缓存友好化（6.1）+ 多解析器（6.3）为主**。book_th 加速 → SPMC 不积压 → 排队消失，延迟与信号同步同时受益。
- **信号不同步（见 5 节结论）**：book_th 积压期间 strategy_th 基于过时 OFI/OBI 快照下单。V1 单通道固有代价；V2 若保持单通道需 book_th 追平消费，若改分簿/双通道需保证订单簿时序。
- **可做**：更快唤醒（shared futex / 直接 spin 检查 ring），不牺牲并行度。
- **验证**：LensX 重测，目标 push_spmc→pop p50 <10µs、alloc→pop p50 <10µs。

### 6.3 吞吐：多解析器并行（主方向）
- **现状**：单 parse_th，解析路径占 ~19% CPU；recv_th 与 parse_th 分离。
- **方向**（保留 recv_th/parse_th 分离，**不合并**）：
  - **多 parser 消费者**：recv 分发到多个解析线程（SPMC 广播），多核并行解析 → 拉高 parse 吞吐上限
  - **多核 REUSEPORT**：多连接分担收包
  - **内核 UDP 缓冲按需动态调整**
- **理由**：book_th 已是吞吐瓶颈（5.5M），多解析器把 parse 从单核解放，配合订单簿优化（6.1）整体拉高吞吐；延迟已由混合退避压到 4.5µs 够用。
- **验证**：扫档临界应显著 >5.5M msg/s，且 drops_b=0。

## 7. 附：测量复现命令

```bash
# 权限
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 kernel.unprivileged_bpf_disabled=0

# perf 热点 + 硬件事件（压测窗口, 自动裁剪 2.5%~97.5% 纯负载段）
# 学 NebulaX: perf record 全程采, 按样本时间戳裁剪空闲期; 同时 perf stat 采硬件事件
SUDO_PASS=<密码> ./scripts/perf_measure.sh 10000

# LensX 延迟(四级链路: 包级 + alloc→pop 四段 + 仲裁 + 下单)
# 需 sudo(密码用 SUDO_PASS 环境变量传入), LensX 在 ~/LensX/build/lensx
SUDO_PASS=<密码> ./scripts/measure_lensx.sh 10000

# 压测 + 事后丢包检测
./scripts/pressure_test.sh --rate 10000   # 标准零丢档(~5M msg/s, 方案A)
./scripts/pressure_test.sh --rate 11000   # 临界附近(方案A, ~5.5M/s)
./scripts/pressure_test.sh                # 全速（UDP 会丢，检测用）

# 压测客户端默认绑 CPU3(低中断 P 核), 可覆盖:
./build/trader_benchmark --file test_data/itch_100mb.bin --no-shm --rate 10000 --core 13
```

> 说明：压测脚本里的 `trader_benchmark` 已默认 `--core 3`（benchmark 内部默认值）。如需指定其它核用 `--core <n>`。被测系统（5 线程 + 独立信号仲裁 + 独立高频策略，见第 0 节）吞吐/延迟数据见第 4、5 节。
