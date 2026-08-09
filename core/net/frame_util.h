#pragma once
// 网络后端帧头剥离工具。
// AF_XDP / DPDK 的 recv() 返回【完整 L2 帧】(与真实网卡收包一致: 含 IP 头之前的 MAC 头)，
// 本工具从帧里提取目标 UDP 载荷(MoldUDP64 包)，把剥帧职责收进 receiver 层，
// 下游解析器只看到纯载荷(与 io_uring recv() 语义一致)。
//   帧布局: [以太头14][IPv4 20][UDP 8][载荷(MoldUDP64 包)]

#include <cstddef>
#include <cstdint>

// 校验帧头 + 提取目标 UDP 载荷。返回载荷长度；0 = 非目标帧(调用方应跳过)。
//   frame     完整 L2 帧(AF_XDP/DPDK recv 返回)
//   frame_len 帧长
//   dport     目标 UDP 端口(只提取行情端口帧)
//   payload   输出: 载荷指针(MoldUDP64 包起始)
// 校验: eth_type==0x0800(IPv4) && proto==17(UDP) && dport 匹配
static inline size_t extract_udp_payload(const uint8_t* frame, size_t frame_len,
                                         uint16_t dport, const uint8_t** payload) {
    const size_t kEthLen = 14, kMinIpLen = 20, kUdpLen = 8, kMinPayload = 20;
    if (frame_len < kEthLen + kMinIpLen + kUdpLen + kMinPayload) return 0;

    const uint16_t eth_type = (uint16_t)((frame[12] << 8) | frame[13]);
    if (eth_type != 0x0800) return 0;                    // 仅 IPv4

    const uint8_t ihl = frame[14] & 0x0f;                // 单位 4 字节
    if (ihl < 5) return 0;
    const size_t ip_off = kEthLen;
    if (frame[ip_off + 9] != 17) return 0;               // 协议 = UDP

    const size_t udp_off = ip_off + (size_t)ihl * 4;
    if (udp_off + kUdpLen > frame_len) return 0;
    const uint16_t f_dport = (uint16_t)((frame[udp_off + 2] << 8) | frame[udp_off + 3]);
    if (f_dport != dport) return 0;                      // 只收行情端口

    const uint16_t udp_len = (uint16_t)((frame[udp_off + 4] << 8) | frame[udp_off + 5]);
    if (udp_len < kUdpLen + kMinPayload) return 0;       // 至少一个 MoldUDP64 头

    const size_t payload_off = udp_off + kUdpLen;
    const size_t plen = (size_t)udp_len - kUdpLen;
    if (payload_off + plen > frame_len) return 0;        // 帧内一致(防御截断)
    *payload = frame + payload_off;
    return plen;
}
