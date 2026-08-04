# V1 测试手册

V1（单线程交易闭环 + 方案A单通道 + 三信号仲裁）的性能测试与复现指南。

> 详细归档见 [PERF_V1_ARCHIVE.md](PERF_V1_ARCHIVE.md)。本手册只讲**怎么测**。

---

## 1. 测试资产清单

| 资产 | 位置 | 用途 |
|---|---|---|
| 压测脚本 | `scripts/pressure_test.sh` | 纯 UDP 吞吐 + 丢包检测 |
| 延迟测量脚本 | `scripts/measure_lensx.sh` | LensX eBPF 延迟（包级/消息级/仲裁/下单） |
| 硬件事件脚本 | `scripts/perf_measure.sh` | perf record + stat（IPC/cache/ctx/syscall） |
| LensX 探针配置 | `docs/bench/trader_lensx.yaml` | 四级链路探针定义（stage/matcher/report） |
| 测试数据 | `test_data/itch_100mb.bin`（246MB） | 完整日数据 8,737,176 条消息 |
| 被测程序 | `build/trader` / `build/trader_benchmark` | 主程序 + 压测客户端 |

## 2. 外部依赖

| 依赖 | 位置 | 用途 | 缺失后果 |
|---|---|---|---|
| **LensX**（eBPF 工具） | `/home/qiwang/LensX/build/lensx` | 延迟探针 attach | 无法测延迟 |
| sudo/root | — | eBPF 加载 + perf | 无法 attach 探针/采硬件事件 |

- LensX 是**外部项目**（不在本仓库）。换机器需先构建：`cmake -B ~/LensX/build && cmake --build ~/LensX/build`。
- 可用 `LENSX` 环境变量覆盖二进制路径：`LENSX=/path/to/lensx ./scripts/measure_lensx.sh`。

## 3. 测量前准备

```bash
# 权限（eBPF + perf 需要）
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 kernel.unprivileged_bpf_disabled=0

# 构建
cd build && cmake --build . -j$(nproc)
```

## 4. 三类测量

### 4.1 吞吐 + 丢包（不需要 root）

```bash
# 标准零丢档（~5.05M msg/s）
./scripts/pressure_test.sh --rate 10000

# 临界附近（~5.55M）
./scripts/pressure_test.sh --rate 11000

# 自动扫档找临界
./scripts/pressure_test.sh --sweep
```

**看什么**：`解析总数`、`解析 QPS`、`单通道丢`、`零内部丢包 ✅`。

### 4.2 延迟（LensX，需 root）

```bash
# 完整四级链路（包级 + alloc→pop 四段 + 仲裁 + 下单），自动输出 CSV 离线分析
SUDO_PASS=<密码> ./scripts/measure_lensx.sh 10000
```

**看什么**：末尾的 `CSV 离线分析`（各段 P50/P99/P999）。完整原始数据在 `/tmp/trader_lensx.csv`。

### 4.3 硬件事件（perf，需 root）

```bash
# IPC / cache-miss / L1/L2/LLC / ctx/s / syscall 计数 + CPU 热点
SUDO_PASS=<密码> ./scripts/perf_measure.sh 10000
```

**看什么**：`硬件事件(perf stat)`、`派生指标`（IPC/miss率/ctx/s）、`CPU 热点 Top`（裁剪窗口内）。

## 5. QPS 换算

`--rate N` = 每 0.1s 发 N 包 = **QPS ≈ 505N msg/s**（每包平均 50.5 条，`benchmark/main.cpp`）。

| --rate | QPS | 用途 |
|---|---|---|
| 10000 | ~5.05M | 标准零丢档 |
| 11000 | ~5.55M | 临界 |
| 12000 | ~6.06M | 超临界（UDP 丢） |

## 6. 已知注意事项

- **测量窗口**：perf 按样本时间戳裁剪 2.5%~97.5% 纯负载段（学 NebulaX），不靠 sleep 猜。
- **探针抽样**：消息级探针抽样 1/128（全采样拖垮吞吐 5M→543K）。抽样判断在调用点（uprobe 挂函数入口）。
- **异常值**：key 模式跨线程偶发 `211106s` 假样本，CSV 分析已剔除（`>10¹²ns`）。
- **偶发段错误**：带探针高压下偶发（未复现，待查）。
- **LensX 输出**：以 `CSV 离线分析` 为准，终端 Results 的 dump 样本量随 SIGINT 时机波动。
