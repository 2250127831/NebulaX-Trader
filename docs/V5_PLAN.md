# V5 计划：业务层对齐真实交易系统（补包 + 订单生命周期 + 一致性 + 会话）

> 日期：2026-08-09 · 分支：V5（从 main b81298b 起）· 前置：V4 网络后端完成
> 与 VERSION_PLAN.md 的关系：V5 原定"L2 真实行情接入"。2026-08-09 转向：**先对齐真实交易系统
> 的业务逻辑**（数据正确性 + 订单生命周期 + 容错），L2 接入顺延。补包是首个且核心课题。

## ⚠️ V5 实际交付（2026-08-12 定稿）

V5 实际转向**订单链路实盘协议化**（用户决策链），完成以下（非下述原计划的补包/一致性/会话——顺延 V6）：

| 交付 | 内容 | commit |
|---|---|---|
| **协议解耦** | IOrderCodec 接口 + CustomOrderCodec（业务不碰协议字节）| f54f252 |
| **OUCH 4.2** | OuchOrderCodec + 'O'/'A'/'E'/'C'/'J' + token↔order_id 映射 | bb67521 |
| **TCP 全双工** | IoUringSender UDP→TCP + recv()，订单/回报同一连接 | bb67521 |
| **回报语义** | on_order_report 按 A/E/C/J 分发 + 半成交/撤单/拒单 | bb67521 |
| **委托簿** | OrderManager 完整状态机 + PENDING_CANCEL + symbol/strategy 索引 + 撤单链路 | 4601b91 |
| **回撤风控** | 盯市净值 + 峰值 + 分档触发（暂停/平仓）| 051e664 |

**顺延 V6**：补包（数据正确性）、订单簿一致性校验、会话心跳、L2 真实行情。
**完整交易链路已打通**：下单('O')→接受('A')→半成交('E')→撤单('X')→撤回报('C')→查询(open_orders) → 回撤触发暂停/平仓。


## 0. 目标（一次一个变量，每个都可端到端正确性验证）

真实 HFT 系统业务层对照当前代码的差距，V5 选做四个（高价值 + 可测）：

| # | 业务 | 真实系统 | 当前代码 | V5 做 |
|---|---|---|---|---|
| 1 | **丢包恢复（补包）** | MoldUDP64 补包 / TCP 重传 | ❌ 无 | ✅ 核心 |
| 2 | **订单生命周期状态机** | 提交→活→部分→全成/撤/拒, 状态流转 | 仅 3 状态 | ✅ |
| 3 | **订单簿一致性校验** | 引用缺失单/重复单告警 | ❌ 无 | ✅ |
| 4 | **会话心跳/超时** | 行情连接心跳、重连、告警 | ❌ 无 | ✅ |

**排除**（V6+）：订单簿快照重建、成交回报补拉、时间同步（PTP）、监控告警平台化。
**归因纪律**：每项独立可测，不互相依赖；补包是数据正确性根基，最先做。

## 1. 背景与现状

### 1.1 补包（核心）

- `MoldUdpUnpacker::feed()` 只按 `包seq + 包内偏移` 给消息加序号，**从不检查连续性**。
  UDP 丢包时下游订单簿静默错乱（缺失 ADD → 后续 DELETE/TRADE 引用孤儿 order_ref → 数据损坏）。
- 全仓库无 gap 检测/补包逻辑（`docs/PACKET_LOSS_NOTES.md` 明确说 V6 再设计），无 TCP socket。
- benchmark 限速不发丢包（shm 模式发一包等一包）→ 丢包场景从未暴露。

### 1.2 订单生命周期

- `oms/order_manager.h` 目前只有 FILLED / REJECTED / PENDING 三态，无状态机。
- 真实系统：提交(PENDING)→活(ACCEPTED)→部分成交(PARTIAL)→(全成 FILLED / 撤单 CANCELLED / 拒 REJECTED)。
- 缺：非法流转校验（如已成交的订单不能被撤）、部分成交累积、状态变更回调。

### 1.3 订单簿一致性

- `OrderMap::find` 引用缺失 order_ref 时无校验（find 失败静默）。
- 真实系统：引用单不存在、重复单、价格越界 → 告警/拒收，防止脏数据污染簿。

### 1.4 会话心跳

- 无行情连接存活检测。真实系统：UDP 行情流心跳（定期会话消息）、超时告警/重连。

## 2. 补包设计（定稿，用户已确认）

### 2.1 机制（真实 NASDAQ MoldUDP64 模型）

```
主通道: UDP 多播(有损) ──► unpacker(seq 连续性检测)
                              ├─ 无缺口 → 直接 push 字节 ring(快速路径)
                              └─ 有缺口 → 发补包请求(TCP) + reorder 暂存缺口后消息
补包通道: TCP(可靠) ──► benchmark 补包服务器(缓存最近 1000 包) → 找缺失包发回
                              └─ 补包到达 → 填 reorder → 按序释放 → 连续喂 ring
```

**缺包当下为何不阻塞单个 SPSC**（用户问题的答案，写入计划）：
- 缺包内容未知（包没到手，不知道消息体/ locate），**无法路由到"那个 SPSC"**——信息不可得。
- 能定位 SPSC 的唯一时刻是补包到达后，但那时后续消息可能已解析/分发/进订单簿，SPSC 是 FIFO 无法插队。
- 所以**交付层有界等待**：reorder 缓冲（有界）暂存缺口后的消息，补包到达（TCP 快速）填缺口后统一释放。
- 解析器/worker 线程照常运行，只是暂时无新消息喂入——类似 V3 retry 桶短暂卡解析器，其他 worker 照常。

### 2.2 关键约束：SPSC 单生产者

字节 ring（`SPSCByteRing`）单生产者单消费者无锁。**只有 recv_th 能写 ring**：
- 补包线程**绝不碰 ring**，只把补包消息塞进 reorder 缓冲。
- reorder 状态由 recv_th 独占；补包线程与 recv_th 通过互斥锁保护 reorder。
- **快速路径（无缺口）零锁**。
- 停止时主线程 `finalize()` 把 reorder 排空到 ring（recv_th 已 join，无并发生产者）。

### 2.3 补包协议（oms/retransmit_protocol.h，新文件）

风格对齐 `oms/order_protocol.h`（定长、大端、magic 字节、边界校验）：

```
请求(trader → benchmark, 17 字节):
  [1]'R' [8]lo(缺失首 seq, BE) [8]hi(缺失区间上界, 不含, BE)
响应(benchmark → trader, TCP 流, length-prefix 分帧):
  frame = [2]len + [len] MoldUDP64 完整包字节
  benchmark 从缓存找 [first_seq, first_seq+count) ∩ [lo,hi) 非空的所有包, 逐个发回
```

### 2.4 组件变更

**MoldUdpUnpacker**（market/pipeline/mold_udp_unpacker.h）：
- 追加（默认关闭，快速路径零开销）：`retransmit_enabled_`、`on_gap_`（缺口回调）、
  `expected_seq_`（下一期望 seq）、`gap_open_`、`std::mutex mu_`、`std::map<seq, bytes> reorder_`。
- `feed()` 快速路径：`seq == expected && !gap_open` → push ring + `expected += count`（无锁）。
- 慢路径：有缺口 → `on_gap_(expected, seq)`（锁外）+ 缺口后消息入 reorder + flush 连续前缀。
- 超时兜底：`gap_open` 超 `retrans_timeout_ms` → 放弃缺口（expected 跳到缓冲最低），flush 现有 + 日志。
- `fill_retrans(pkt,len)`（补包线程调）：缺口消息入 reorder，不 flush（flush 归 recv_th）。
- `finalize()`（停止后）：reorder 按 seq 序全 push ring。

**oms/retransmit_protocol.h**（新）：请求/响应编解码。

**benchmark/main.cpp**：
- Config 加 `drop_rate`、`retrans_port`、`retrans_cache=1000`；`--drop-rate`/`--retrans-port`/`--retrans-cache`。
- 缓存 `std::deque<CachedPkt>` + mutex，容量 1000（CachedPkt = {first_seq, count, raw bytes}）。
- 发送循环：构建包 → **先入缓存**（无论发不发）→ `rand() < drop_rate` 跳过 sendto（不增 sent）else sendto。
- TCP 补包服务器线程（模板 = `run_sim_exchange`）：accept → 读 17 字节请求 → 锁缓存找相交包 → 发 frame。

**main.cpp（trader）**：
- config 加 `execution.retransmit_enabled/retrans_port/retrans_timeout_ms`。
- `RetransmitClient`（内联小类）：TCP 连 `host:retrans_port`；`request(lo,hi)`；线程读 frame → `fill_retrans`。
- `unpacker.set_retransmit(true, [&](lo,hi){ client.request(lo,hi); })`。
- 停止序列调整：`recv_th.join()` → `client.stop()+join()` → `unpacker.finalize()` → `parse_th.join()`。
  **finalize 必须在 parse 排空前**（reorder 缓冲消息要先进 ring 才被解析）。

## 3. 订单生命周期状态机（oms/order_manager.h）

- 状态扩展：`PENDING → ACCEPTED → PARTIAL_FILLED → (FILLED | CANCELLED | REJECTED)`。
  `PARTIAL_FILLED` 记录 `filled_qty` 累积。
- `OrderManager` 加状态机校验：`on_order_fill` 只在 ACCEPTED/PARTIAL 允许；`cancel` 只在活态允许；
  非法流转 → 记录错误日志 + 拒收（不静默）。
- 状态变更回调：`on_order_state_change`（真实系统用于告警/对账）。
- 现有 `count_by_status` / `OrderStatus` 枚举扩展，保持向后兼容（REJECTED/FILLED/PENDING 保留）。

## 4. 订单簿一致性校验（market/book/order_book_consumer.cpp）

- `OrderMap::find` 引用缺失 order_ref 时 → 现在静默，改为：`on_stale_ref(locate, order_ref, event_type)`
  告警回调 + 计数器（`stale_ref_count`），不崩溃、不污染簿（跳过该事件）。
- 重复单：同 `order_ref` 重复 ADD → 告警 + 拒收。
- 价格/数量越界（price<0、shares==0 等非法值）→ 告警 + 拒收。
- 全部可配置开关（默认告警不拒收，保持现有行为），可端到端测试。

## 5. 会话心跳（market/gateway/ 或新增）

- 行情流存活检测：跟踪 `expected_seq` 增长 + 最近包时间戳。
- 超时（`session_timeout_ms` 无新包）→ 告警 + 统计（`session_breaks`）。
- 不自动重连（V6 做），只检测 + 记录。

## 6. 测试

### 6.1 单测
- **test_unpacker_retransmit.cpp**（核心，纯逻辑无 TCP）：乱序暂存、补包填缺口按序吐、重复补包幂等、超时放弃。
- **test_order_lifecycle.cpp**：状态机合法流转、非法流转拒收、PARTIAL 累积。
- **test_book_consistency.cpp**：缺失引用告警、重复单告警、价格越界告警（默认告警不拒收）。

### 6.2 集成（端到端）
- **test_retransmit_verify.cpp**：仿 test_benchmark_verify，fork benchmark 加 `--drop-rate 0.1 --retrans-port 9092`，
  trader 开 retransmit。断言 `parsed == 文件非R消息数`（丢包后零丢失）+ seq 连续 + sent==parsed。
- 现有 test_benchmark_verify（无丢包）回归。

### 6.3 回归
- 所有新功能**默认关闭** → 现有 21/21 测试不变。
- drop-rate=0 + retransmit 开 → 快速路径，仍零丢失。

## 7. 配置

```yaml
execution:
  retransmit_enabled: false      # 补包: 默认关(现有行为)
  retrans_port: 9092
  retrans_timeout_ms: 500
  session_timeout_ms: 5000       # 会话心跳超时
order_book:
  consistency_check: true        # 一致性校验(默认告警不拒收)
```

## 8. 实现步骤与验证

```
1. 补包协议 + reorder 状态机 → 验证: test_unpacker_retransmit(乱序/填缺/幂等/超时)
2. benchmark 缓存 + 丢包注入 + TCP 补包服务器 → 验证: 手动丢包可见补包日志
3. main.cpp RetransmitClient + 停止序列 → 验证: test_retransmit_verify(丢包后零丢失)
4. 订单生命周期状态机 → 验证: test_order_lifecycle(合法/非法流转)
5. 订单簿一致性校验 → 验证: test_book_consistency(告警不拒收)
6. 会话心跳 → 验证: 超时告警 + 统计
7. 全量 ctest + 三后端回归(io_uring/af_xdp/dpdk 不受影响)
```

## 9. 与 VERSION_PLAN 的关系

- V5 原定 L2 真实行情接入 → 顺延（V6/V7）。V5 先做业务层对齐（数据正确性 + 订单生命周期 + 容错），
  因为真实行情接入需要先有正确的数据底座（补包）和订单语义。
- L2 接入时业务层已就绪：补包机制直接复用（真实行情也是 MoldUDP64 类协议），状态机/一致性/会话通用。
