# 端到端验证：benchmark 发送 ↔ 接收抽象接口

## 作用

验证完整链路的字节一致性：真实压测脚本 `trader_benchmark` 读取 ITCH 二进制流经 UDP 发送，接收端通过抽象接口 `IMarketDataReceiver` 接收，逐字节核对收到的二进制流与源文件**内容完全一致**。

覆盖：
- `trader_benchmark` 的 ITCH 消息切分与发送
- **接口抽象**：测试主体只操作 `IMarketDataReceiver*`，具体后端由 `make_receiver()` 工厂实例化
- 后端实现（当前为 `IoUringReceiver`：固定缓冲区、POLL_FIRST）
- UDP 传输（无丢包前提下字节完整）

## 测试形态

`tests/integration/test_benchmark_verify.cpp`（`ut_benchmark_verify`）：

1. 通过抽象接口 `IMarketDataReceiver*` 创建接收端（`make_receiver()` 工厂，换后端只改这一处）
2. **接收线程**先以非阻塞模式预热一次 recv（submit SQE 进内核），置 `armed` 就绪屏障
3. 主线程等 `armed` 屏障后才 fork + exec `trader_benchmark` 子进程发送
4. 接收线程阻塞收满 `期望字节数`；主线程等 `done` 有 8s 超时，超时则 `stop()` 打断
5. 逐字节比对收到的字节流与期望流

**期望流**：用与 benchmark 相同的**长度前缀逻辑**（2 字节 big-endian）从源文件重建，`rebuild_expected()`。修复解析后，期望流 = 源文件完整字节流。

### 接收就绪屏障（时序安全）

接收线程**先提交 SQE 进内核、再放行主线程 fork**，保证无论接收端准备多久，子进程都不可能早于"内核已在收包"之前发包。相比"fork 后边收边等"，这是设计保证而非碰巧安全。

## 关键设计点

### 阻塞模式必须用于高速收包

`IoUringReceiver` 有阻塞 / 非阻塞两种模式：

| 模式 | 行为 | 高速收包 |
|---|---|---|
| 阻塞 | recv 阻塞，`submit_and_wait_timeout` 隐含持续 submit SQE | 无空窗，不丢包 ✅ |
| 非阻塞 | recv 立即返回，需显式 `submit_recv_now` 提交 SQE | 提交/peek 循环有空窗，高速丢包 ❌ |

**高速收包（如 benchmark 全量发送）必须用阻塞模式**。非阻塞模式的 SQE 提交空窗会导致 UDP 缓冲溢出丢包。

### backlog 限速防丢包

`trader_benchmark` 默认 `--backlog 1000000` 不降速，本地高速发送会溢出 UDP 接收缓冲区（212KB）导致丢包。
测试用 `--backlog 5000` 触发 benchmark 的自适应调速，保证不丢包。

## 运行

```bash
# 全量测试（含此测试）
ctest --test-dir build --output-on-failure

# 单独运行（第 4 参可选指定后端，默认 io_uring）
./build/ut_benchmark_verify test_data/itch_test_small.bin 8080 ./build/trader_benchmark io_uring
```

## 测试数据

- `test_data/itch_test_small.bin` — 前 500 条 ITCH 消息（20KB），从 `itch_100mb.bin` 头部切出
- `test_data/itch_100mb.bin` — 100MB gz 解压后 245.7MB，已剥离末尾截断
- `test_data/itch_100mb_head.gz` — 从 NASDAQ 下载的 100MB 压缩包头部

> `test_data/` 已被 `.gitignore` 忽略，不入库。

## 输出示例

```
源文件 20407 字节, benchmark 期望发送 20407 字节
ready=true, fork benchmark...
benchmark 子进程 exit: 0 (OK)

=== 端到端比对 ===
UDP 包数:   500
接收字节:   20407
期望字节:   20407

端到端验证 PASS ✓ (20407 字节逐字节一致)
```

> benchmark 修复前：按 type 查表解析，误判 836 字节为"无效数据"，只发 474 条。
> 修复后：按 2 字节 big-endian 长度前缀解析，500 条全发、Extra bytes skipped: 0。
> 期望流 = 源文件完整字节流（不再需要重建 benchmark 的特殊发送序列）。
