# 压测客户端 (trader_benchmark) 使用说明

## 作用

读 ITCH 二进制文件，以 UDP 全量逐包发送到 NX-Trader，模拟实盘行情流。

## 编译

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

产物：`build/trader_benchmark`

## 用法

```bash
build/trader_benchmark [选项]
```

### 选项

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--file <path>` | `test_data/itch_sample.bin` | ITCH 二进制文件路径 |
| `--host <ip>` | `127.0.0.1` | 目标地址 |
| `--port <port>` | `8080` | 目标 UDP 端口 |
| `--backlog <n>` | `10000` | 积压上限，超过则降速 |

### 示例

```bash
# 默认配置（本机回环）
build/trader_benchmark

# 指定文件和外网地址
build/trader_benchmark --file /data/itch_full.bin --host 192.168.1.100 --port 9090

# 收紧积压上限，更激进地降速
build/trader_benchmark --backlog 5000
```

## 与 NX-Trader 协作流程

```
终端 1：                       终端 2：
build/trader_benchmark          build/trader

1. mmap 读 ITCH .bin
2. 等待 ready ←──── 共享内存 ────→ 3. ready = true
4. 检测到 ready，开始发包
5. 每发一条 UDP  → sent++
                     ──UDP──→    6. 每收一条 UDP  → received++
7. 每 256 条检查 sent - received
   积压过大 → sleep 增加降速
   积压很小 → sleep 减少加速
8. 文件发完，输出统计
```

## 调速机制

采用比例控制器，每次检查调整 ±100ns：

- `backlog > max_backlog` → 降速
- `backlog < 100` → 加速
- 其余区间 → 维持

最终收敛到 NX-Trader 不丢包的最大速度。

## 输出示例

```
Waiting for NX-Trader ...
NX-Trader ready.  Start replay...

=== Done ===
  Messages sent:   386711
  Extra bytes skipped: 0
  Total time:      1.234 s
  Send rate:       313394 msg/s
```
