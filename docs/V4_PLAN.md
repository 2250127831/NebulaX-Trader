# V4 计划：网络后端（AF_XDP + DPDK，接口 + 正确性）

> 日期：2026-08-09 · 分支：V4 · 前置：V3（分发器 + retry 桶）已合并 main
> 与 VERSION_PLAN.md 的关系：V4 原定只做 AF_XDP、V6 做 DPDK；2026-08-09 决策**两个后端都归 V4**
> （网络基础设施线收敛），V6 改为真实硬件的性能对比。

## 0. 目标（网络后端独立抽象层，业务代码一行不改）

1. **IMarketDataReceiver 抽象完整**：io_uring（V1）+ **AF_XDPReceiver** + **DPDKReceiver** 三个后端。
2. **应用层正确性本地可测**（零丢失 + seq 连续），性能留待真实硬件。
3. **AF_XDP/DPDK 收到【完整 L2 帧】**（[以太头14][IPv4 20][UDP 8][载荷]，与真实网卡收包一致，
   **含 IP 头之前的 MAC 头**；DPDK 只是 PMD 换成软件模拟 vdev）。
4. **recv() 语义统一**：receiver **内部自动剥帧头**（extract_udp_payload），recv() 返回
   **纯 UDP 载荷**（MoldUDP64 包）—— 与 io_uring 一致，**解析器不感知帧头**，非目标帧在 receiver 内跳过。

## 1. 关键实测发现（决定测试载体）

### 1.1 lo 上 AF_XDP 收不到帧（实测内核 6.8）

- lo 帧退化为 **2 字节 ethertype**（无 14 字节 MAC 头，直接 `08 00` + IP）。
- libbpf 默认 XDP 程序按 `frame[12:14]` 解析 h_proto → 读到的是 IP 头里的字节 → 不匹配 IPv4 → **不 redirect**。
- 探针实证：lo 上 `xsk_socket__create` 成功、XDP 程序 attach 成功（generic id 413）、
  generic XDP 计数程序在 lo 上确实运行（11 次），但 rx ring 0 帧。
- **结论**：lo 本来就不满足"要有 IP 头之前的东西"——正确测试载体是 **veth 对**（真实 14 字节以太头）。

### 1.2 veth 对提供真实 L2 帧（AF_XDP 正确性载体）

- veth0 挂 XDP+xsk，veth1 发包。实测收帧 66B 布局正确：
  `[dst MAC 6][src MAC 6][08 00][IPv4 20][UDP 8][载荷]`。
- 支持 XDP_SKB 模式（generic XDP）。真实网卡（r8169 本机不支持 XDP_DRV_MODE）行为与 veth 一致。
- **测试链路**：benchmark(真实 UDP socket) → 路由到 veth1 → veth0(XDP redirect) → xsk。
  静态邻居（`ip neigh ... nud permanent`）免 ARP，关 IPv6 免噪声帧，接口上只有目标 UDP 帧。

### 1.3 AF_XDP UMEM 单帧上限 = 一页（4096B）

- 内核硬约束 `XDP_UMEM_MAX_CHUNK_SIZE = PAGE_SIZE`：frame_size=8192 报 EINVAL（实测）。
- 真实网络包 ≤ MTU(1500B) 就是为此。超 4KB 的 MoldUDP64 包需 IP 分片，AF_XDP 收到分片帧
  （重组属上层职责，V4 不做）。
- **测试约束**：benchmark `--pack-max 8`（8×202B ≈ 1.6KB + 42B 头 < 4096），与真实 MTU 语义一致。

### 1.4 DPDK vdev 软件 PMD（本机 r8169 无 DPDK PMD）

- DPDK 21.11.9（apt libdpdk-dev + librte-net-tap22）。
- `net_tap` vdev：DPDK 建 vdev 时创建内核 tap（dtap0），内核路由到 dtap0 的帧被 rx_burst 读到。
- EAL 参数：`-l 0 --no-pci --no-huge -m 128 --vdev=net_tap0 --file-prefix=nxtest`
  （--no-pci 跳过本机无 PMD 的 r8169；--no-huge 免大页；纯软件 vdev）。
- 换真实 PMD（mlx5/i40e）只改 EAL 参数，应用代码零改动。

## 2. 架构

```
IMarketDataReceiver (core/net/i_market_data_receiver.h)
        ▲
  ┌─────┼──────┐
  │     │      │
IoUring  AF_XDP  DPDK          (三个后端, 业务代码只依赖抽象)
（V1）  (V4)   (V4)

收包: AF_XDP/DPDK 收到完整 L2 帧(与真实网卡一致)
     recv() 内部 extract_udp_payload 剥 [以太14][IPv4][UDP] 头 → 返回纯载荷
     (io_uring 由 UDP socket 天然剥头) —— 三者 recv() 语义统一

正确性测试(本地虚拟模式):
  AF_XDP: benchmark → veth1 → veth0(XDP redirect) → xsk → [完整L2帧] → recv() 剥头
  DPDK  : benchmark → 内核路由 dtap0 → net_tap rx_burst → [完整L2帧] → recv() 剥头
  → recv() 返回纯载荷 → MoldUdpUnpacker → ByteRingParser → 零丢失 + seq 连续
```

## 3. 组件变更清单

### 3.1 新增：AF_XDPReceiver（core/net/af_xdp_receiver.h/.cpp）

- libbpf 0.5.0 xsk 统一 API：`xsk_umem__create` + `xsk_socket__create`。
- **XDP_SKB 模式 + XDP_COPY**（generic XDP，veth/网卡通用；零拷贝 DRV_MODE 需支持 XDP 的网卡）。
- UMEM：frame_size=4096(一页, 内核上限) × 1024 帧 = 4MB；rx/fill/tx ring 256。
- recv() 内部 `extract_udp_payload` 剥帧头，返回**纯载荷**；非目标帧（其他端口/协议）跳过。
  阻塞模式 poll(xsk_fd + wake_fd)，stop() 写 wake_fd 打断。
- 帧过滤：libbpf 默认 XDP 程序重定向所有 IPv4/IPv6 帧（含其他端口），recv() 内按端口剥帧跳过。
  生产可自编 XDP 程序按行情端口过滤（省 CPU），V4 只做接口正确性，测试靠 veth + 静态邻居隔离。

### 3.2 新增：DPDKReceiver（core/net/dpdk_receiver.h/.cpp，LIBDPDK 条件编译）

- rte_ethdev 统一 API：EAL 初始化（进程级幂等）→ `rte_eth_dev_get_port_by_name` → 配置/起队列 → rx_burst。
- vdev_spec + eal_args 构造参数：换 PMD/接口不改代码，硬件差异通过参数体现。
- recv() 内部 `extract_udp_payload` 剥帧头，返回**纯载荷**；非目标帧跳过。fd() = -1（无 fd，纯轮询）；
  阻塞模式轮询 rx_burst + stop 标志打断。
- mbuf 默认 RTE_MBUF_DEFAULT_BUF_SIZE（~2KB 数据区），pack-max 控制帧 < MTU。

### 3.3 新增：core/net/frame_util.h

- `extract_udp_payload()`：校验 [eth_type=0x0800][proto=17][dport] + 提取 UDP 载荷（MoldUDP64 包）。
  剥帧职责收进 receiver 层（AF_XDP/DPDK recv() 内部调用），下游解析器只看到纯载荷。

### 3.4 新增：正确性测试（tests/integration/，root-only + ci-skip）

- **ut_af_xdp_verify**：veth 对 + AF_XDP，端到端。
- **ut_dpdk_verify**：net_tap vdev + dtap0，端到端。
- 链路：fork benchmark(真实 UDP socket) → 后端收完整帧 → recv() 剥头返回纯载荷 → 拆包 → 解析 → 断言
  **parsed == 文件非R消息数 + seq 全局连续**（零丢失 + 零错漏）。
- 非 root 运行 → SKIP(返回 0)，CI 用 -LE ci-skip 排除。

### 3.5 CMake

- core 链接 libbpf；LIBDPDK 找到时条件编译 dpdk_receiver + 注册 ut_dpdk_verify。

## 4. 验证结果（2026-08-09 实测）

| 后端 | 发送 | 拆包 | 解析 | 消费 | seq 连续 | 结果 |
|---|---:|---:|---:|---:|---:|---|
| **AF_XDP**(veth0) | 12932 (2901包) | 12932 | 12932 | 12932 | yes | **PASS** |
| **DPDK**(net_tap0) | 12932 (2928包) | 12932 | 12932 | 12932 | yes | **PASS** |

- 非目标帧（AF_XDP 8 / DPDK 9，veth/tap 上 IPv6 链路本地残留）由 recv() 内部剥帧跳过，测试层看不到。
- 全量 ctest 21/21（原有回归 + 2 个 V4 测试 root 实测）全绿。

### 4.1 配置切换 + 主程序（trader）三后端正确性验证

**配置切换入口**：`market.backend`（io_uring / af_xdp / dpdk）+ `market.ifname`（af_xdp 绑定接口）+
`market.vdev`（dpdk vdev 规格）+ `market.eal_args`（dpdk EAL 参数）。`make_receiver()` 按配置实例化，
recv_th 按后端分流（io_uring 走多在途 batch，AF_XDP/DPDK 走单包 recv），下游拆包/解析/分发逻辑**三后端共用**。

**配置**：
```yaml
# config/default.yaml (默认 io_uring, 真实网卡 UDP)
market:
  backend: io_uring
# config/backend_af_xdp.yaml (本地正确性: veth0 挂 XDP+xsk)
market:
  backend: af_xdp
  ifname: "veth0"
# config/backend_dpdk.yaml (本地正确性: net_tap0 → 内核 tap dtap0)
market:
  backend: dpdk
  vdev: "net_tap0"
  eal_args: ["-l", "0", "--no-pci", "--no-huge", "-m", "128", "--file-prefix", "nxtrader"]
```

**一键验证**：`scripts/v4_backend_verify.sh` 逐后端起 trader(shm 握手) → benchmark(发到对应网络载体)
→ 对比 `Messages sent` vs 解析总数（正确性 = sent == parsed）。

| 后端 | 网络载体 | benchmark sent | trader parsed | 结果 |
|---|---|---|---:|---:|---|
| **io_uring** | 127.0.0.1 UDP socket | 12932 | 12932 | **PASS** |
| **AF_XDP** | veth0(veth1 发包, 完整 L2 帧) | 12932 | 12932 | **PASS** |
| **DPDK** | net_tap0(内核 tap dtap0) | 12932 | 12932 | **PASS** |

**主程序停止时序修复（正确性根基）**：原 shm 模式 done 后立即 stop recv + parse_th 以 done 退出，
最后一包（sendto 后 in-flight 未收）在途丢失 → sent 与 parsed 差 1~3 条。修复：
1. parse_th 统一以 `stop` 退出（持续解析直到主线程确认收完），不再以 done 退出；
2. 主线程 done 后先等 `fc->received == fc->sent`（所有已发包被接收）再 stop。
三后端实测 sent == parsed 零丢失。

## 5. 结论

1. **IMarketDataReceiver 抽象完整**：io_uring / AF_XDP / DPDK 三后端，业务代码零改动（测试直接复用
   MoldUdpUnpacker + ByteRingParser + Dispatcher 管线）。
2. **AF_XDP/DPDK 收到完整 L2 帧**（含 IP 头之前的 MAC 头），与真实网卡收包一致；DPDK 用软件 PMD 模拟。
3. **recv() 语义统一**：receiver 内部自动剥帧头（extract_udp_payload），返回纯载荷 —— 解析器不感知帧头。
4. **lo 不适合测 AF_XDP**（帧退化为 2 字节 ethertype，XDP 无法按 offset 12 解析），veth 是正确载体。
5. **AF_XDP UMEM 单帧上限一页(4KB)**：真实 MTU 语义，大包 IP 分片属上层职责。
6. **性能留待真实硬件**：本机 r8169 无 XDP DRV_MODE / 无 DPDK PMD。V6 = 真实硬件横向性能对比。

## 6. 测量复现

```bash
# 需要 root(建 veth/绑 xsk/DPDK 运行)。非 root 自动 SKIP。
SUDO_PASS=<密码> sudo -S bash -c 'cd build && ctest -R "ut_af_xdp_verify|ut_dpdk_verify" --output-on-failure'
# 或直接跑
sudo ./build/ut_af_xdp_verify test_data/itch_chain_sample.bin ./build/trader_benchmark
sudo ./build/ut_dpdk_verify   test_data/itch_chain_sample.bin ./build/trader_benchmark
```

## 7. 与 VERSION_PLAN 的关系

- V4 原定只做 AF_XDP（V6 才做 DPDK）。2026-08-09 决策：AF_XDP + DPDK 两个后端**都归 V4**
  （接口 + 正确性，性质相同的一层抽象）。V6 保留为**真实硬件的三后端横向性能对比**
  （本机无支持 XDP/DPDK 的网卡，性能验证等硬件升级）。
