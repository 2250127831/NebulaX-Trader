#pragma once

#include "i_market_data_receiver.h"

#include <bpf/xsk.h>

#include <atomic>
#include <cstdint>
#include <string>

// ── AF_XDP 后端实现（V4 新增）──
// 通过 XDP 程序把网卡（或 veth）收到的原始 L2 帧重定向到用户态 socket。
// 与 io_uring（UDP socket，内核剥离以太头）不同，AF_XDP 收到【完整 L2 帧】：
//   [以太头14][IPv4 20][UDP 8][载荷] —— 与真实网卡收包一致（含 IP 头之前的 MAC 头）。
// 但 recv() 内部自动剥帧头（extract_udp_payload），返回【纯 UDP 载荷】(MoldUDP64 包)，
// 与 io_uring recv() 语义一致 —— 下游解析器只看到载荷，不感知帧头。
// 非目标帧（其他端口/协议）在 recv() 内部跳过。
//
// 正确性本地验证载体 = veth 对（veth0 挂 XDP+xsk，veth1 发包）：
//   - lo 的帧退化为 2 字节 ethertype（无 14 字节 MAC 头），XDP 默认程序按 offset 12
//     解析 h_proto 得到垃圾 → 不重定向。lo 上 AF_XDP 收不到帧（实测 6.8 内核）。
//   - veth 提供真实 14 字节以太头（实测收帧 66B 布局正确），且支持 XDP_SKB 模式。
//   - 真实网卡（r8169 本机不支持 XDP_DRV_MODE）行为与 veth 一致：完整 L2 帧。
//
// 帧过滤：libbpf 默认 XDP 程序重定向接口上所有 IPv4/IPv6 帧（含其他端口）。
//   recv() 内按端口剥帧跳过；生产可在 XDP 层加端口过滤（省 CPU），V4 只做接口正确性。
//
// 用法（线程由使用方组织，与 IoUringReceiver 一致）：
//   AF_XDPReceiver rcv("veth0", 8080);
//   rcv.start();
//   while (running) { n = rcv.recv(buf, sizeof(buf)); ... }   // 阻塞或非阻塞, 纯载荷
//   rcv.stop();
class AF_XDPReceiver : public IMarketDataReceiver {
public:
    // ifname : 绑定的接口（真实网卡 / veth 端）
    // port   : 目标 UDP 端口（recv() 内按此剥帧, 非此端口帧跳过）
    AF_XDPReceiver(const std::string& ifname, uint16_t port, uint32_t queue_id = 0);

    bool start() override;
    void stop() override;
    void set_blocking(bool blocking) override;
    ssize_t recv(uint8_t* buf, size_t len) override;
    int fd() const override;

    // UMEM 配置。kFrameSize = 4096(一页): 内核 xsk 硬约束
    // XDP_UMEM_MAX_CHUNK_SIZE = PAGE_SIZE —— 单帧最多 4KB(实测 8192 报 EINVAL)。
    // 真实网络包 ≤ MTU(1500B) 就是为此; 超 4KB 的 MoldUDP64 包需 IP 分片,
    // AF_XDP 收到的是分片帧(重组属上层职责, V4 不做)。
    static constexpr size_t kFrameSize = 4096;    // 一页(内核上限)
    static constexpr size_t kNumFrames = 1024;    // UMEM 帧数(总 4MB)
    static constexpr size_t kRingSize  = 256;     // rx/fill/tx 环深度

private:
    // 把 UMEM 里处理完的帧地址归还 fill ring（内核复用）
    void refill(uint64_t addr);

    std::string ifname_;
    uint16_t port_;
    uint32_t queue_id_;

    void* umem_buf_ = nullptr;            // 对齐分配（4KB 页对齐）
    struct xsk_umem* umem_ = nullptr;
    struct xsk_ring_prod fill_{};
    struct xsk_ring_cons comp_{};
    struct xsk_ring_cons rx_{};
    struct xsk_ring_prod tx_{};
    struct xsk_socket* xsk_ = nullptr;

    int wake_fd_ = -1;                    // 打断阻塞 poll（stop 时写）
    std::atomic<bool> running_{false};
    std::atomic<bool> blocking_{true};
};
