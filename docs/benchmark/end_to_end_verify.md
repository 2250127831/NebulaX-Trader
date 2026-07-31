# 端到端验证：benchmark 发送 ↔ IoUringReceiver 接收

## 作用

验证完整链路的字节一致性：真实压测脚本 `trader_benchmark` 读取 ITCH 二进制流经 UDP 发送，`IoUringReceiver` 接收，逐字节核对收到的二进制流与源文件**内容完全一致**。

覆盖：
- `trader_benchmark` 的 ITCH 消息切分与发送
- `IoUringReceiver` 的 io_uring 接收（含固定缓冲区、POLL_FIRST）
- UDP 传输（无丢包前提下字节完整）

## 测试形态

`tests/integration/test_benchmark_verify.cpp`（`ut_benchmark_verify`）：

1. 用 `IoUringReceiver` 绑定测试端口（阻塞模式）
2. 置 flow_control `ready`，fork + exec `trader_benchmark` 子进程发送
3. 子进程发完即止；父进程阻塞收满 `期望字节数`
4. 逐字节比对收到的字节流与"期望流"

**期望流**：用与 benchmark 相同的 `MSG_BODY_LEN` 逻辑，从源文件重建 benchmark 应发送的字节流（`rebuild_expected()`）。

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

# 单独运行
./build/ut_benchmark_verify test_data/itch_test_small.bin 8080 ./build/trader_benchmark
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
