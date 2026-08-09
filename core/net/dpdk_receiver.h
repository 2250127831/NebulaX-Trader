#pragma once

#include "i_market_data_receiver.h"

#include <rte_config.h>
#include <rte_ethdev.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

// ── DPDK 后端实现（V4 新增）──
// 通过 rte_ethdev 轮询收包。与 io_uring（UDP socket 剥离以太头）不同，
// DPDK 收到【完整 L2 帧】：[以太头14][IPv4 20][UDP 8][载荷] —— 与真实网卡收包一致
// （含 IP 头之前的 MAC 头）。但 recv() 内部自动剥帧头（extract_udp_payload），返回
// 【纯 UDP 载荷】(MoldUDP64 包)，与 io_uring recv() 语义一致 —— 下游解析器只看到载荷，
// 不感知帧头。非目标帧（其他端口/协议）在 recv() 内部跳过。
//
// 正确性本地验证载体 = vdev 软件 PMD（PMD 用软件模拟真实收包）：
//   - net_tap（推荐）：DPDK 建 vdev 时创建内核 tap 设备（默认 dtap0），
//     内核路由到 tap 的帧被 net_tap 的 rx_burst 读到。链路 = 完整 L2 帧。
//   - net_af_packet：绑到内核接口（veth/物理网卡），经 AF_PACKET 收帧。
//   - 硬件差异（r8169 无 DPDK PMD）通过 EAL 参数体现：换 PMD/接口不改应用代码。
//
// DPDK 依赖进程级 EAL 初始化（rte_eal_init 消费 argv，须在创建线程前调用）。
// start() 首次调用时初始化 EAL（进程内幂等），fd() = -1（无 fd，纯轮询）。
//
// 用法（线程由使用方组织，与 IoUringReceiver 一致）：
//   DPDKReceiver rcv("net_tap0", 8080, {"-l", "0", "--no-pci", "--no-huge", "-m", "128"});
//   rcv.start();
//   while (running) { n = rcv.recv(buf, sizeof(buf)); ... }   // 纯载荷
//   rcv.stop();
class DPDKReceiver : public IMarketDataReceiver {
public:
    // vdev_spec : DPDK 虚拟设备（net_tap0 → 内核 tap dtap0；net_af_packet0,iface=eth0）
    // port      : 目标 UDP 端口（recv() 内按此剥帧, 非此端口帧跳过）
    // eal_args  : 额外 EAL 参数（"-l" 绑核 / "--no-pci" / "--no-huge" / "-m" 内存）
    DPDKReceiver(const std::string& vdev_spec, uint16_t port,
                 const std::vector<std::string>& eal_args = {"-l", "0"});

    bool start() override;
    void stop() override;
    void set_blocking(bool blocking) override;
    ssize_t recv(uint8_t* buf, size_t len) override;
    int fd() const override;

    static constexpr uint16_t kRxRing = 256;
    static constexpr uint16_t kTxRing = 64;
    static constexpr size_t   kMbufPool = 2048;

private:
    // 进程级 EAL 初始化（幂等）。返回 0 成功。
    static int eal_init(const std::vector<std::string>& args);

    std::string vdev_spec_;
    uint16_t port_;
    std::vector<std::string> eal_args_;

    bool eal_ready_ = false;
    bool port_started_ = false;
    uint16_t port_id_ = 0;
    struct rte_mempool* mbuf_pool_ = nullptr;
    struct rte_mbuf* mbufs_[1];

    std::atomic<bool> running_{false};
    std::atomic<bool> blocking_{true};
};
