#pragma once

#include "core/queue/spsc_byte_ring.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

// ── MoldUDP64 拆包器 ──
// 接收一个 MoldUDP64 包（20 字节头 + 若干条 ITCH 消息），拆出每条消息，
// 在每条消息前加 2 字节 seq（包 seq + 包内偏移），推入字节 ring。
//
//   [收包] recv → MoldUDP64 包 ──► MoldUdpUnpacker ──► SPSCByteRing
//                                                    （[seq][len][体][seq][len][体]...）
//
// 为什么在 recv 侧拆包加 seq（方案 X）：
//   - 字节 ring 里是"已加序号的裸消息流"，自包含，下游只管读
//   - AF_XDP 升级：这段"读包头 + 加序号"可放内核（XDP hook），
//     直接产出已加序号的流写入用户态，不搬原始 UDP 包
//
// MoldUDP64 头（20 字节）:
//   [Session: 10][Sequence: 8(包内第一条消息序号)][Message Count: 2]
// 每条消息: [len: 2 字节][消息体]（len = 消息体字节数）
//
// 用法：
//   MoldUdpUnpacker<RingCap> unpacker;
//   unpacker.feed(packet_data, packet_len);   // 收包后调用，推入 ring
//   unpacker.ring()                            // 下游 ByteRingParser 消费
class MoldUdpUnpacker {
public:
    using Ring = SPSCByteRing;

    static constexpr size_t kHeaderLen = 20;      // MoldUDP64 头
    static constexpr size_t kMsgHeader = 4;       // 加序号后: [seq 2][len 2]

    // 构造传入共享 ring 引用（由 QueueManager 持有，unpacker 写、bp 读）。
    explicit MoldUdpUnpacker(SPSCByteRing& ring) : ring_(ring) {}

    // 处理 MoldUDP64 包流（一个或多个连续包）。返回成功拆出的消息数。
    // 每个包: [20 字节头][消息1][消息2]...；处理完一个包的 count 条后，
    // pos 指向下一个包的 20 字节头，循环处理。
    size_t feed(const uint8_t* pkt, size_t pkt_len) {
        size_t pos = 0;
        size_t unpacked = 0;
        uint8_t msg_buf[4 + 200];  // [seq][len][消息体]

        while (pos + kHeaderLen <= pkt_len) {
            uint64_t seq  = be64(pkt + pos + 10);    // 包头 seq = 包内第一条消息序号
            uint16_t count = be16(pkt + pos + 18);   // 包内消息数
            pos += kHeaderLen;

            for (uint16_t i = 0; i < count; ++i) {
                if (pos + 2 > pkt_len) break;
                uint16_t body_len = be16(pkt + pos);
                if (body_len < 1 || body_len > 200) break;
                size_t msg_len = 2 + body_len;
                if (pos + msg_len > pkt_len) break;

                // 每条消息前加 2 字节 seq（包 seq + 包内偏移）
                uint64_t msg_seq = seq + i;
                be16_store(msg_buf, static_cast<uint16_t>(msg_seq));  // [seq 2]
                msg_buf[2] = pkt[pos];        // [len 2 高字节]
                msg_buf[3] = pkt[pos + 1];    // [len 2 低字节]
                memcpy(msg_buf + 4, pkt + pos + 2, body_len);  // 消息体

                // 推入 ring: 一次 push 整条消息(消息级, 让 push 处理跨回绕空洞)。
                // push 返回 0 = 空间不足(含空洞跨越后仍不足), 忙等消费者释放。
                while (ring_.push(msg_buf, msg_len + 2) == 0)
                    __builtin_ia32_pause();

                pos += msg_len;
                ++unpacked;
            }
            // 包结束: pos 指向下一个包的头或流末尾
        }
        return unpacked;
    }

    Ring& ring() { return ring_; }

private:
    static uint64_t be64(const uint8_t* p) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
        return v;
    }
    static uint16_t be16(const uint8_t* p) {
        return (static_cast<uint16_t>(p[0]) << 8) | p[1];
    }
    static void be16_store(uint8_t* p, uint16_t v) {
        p[0] = static_cast<uint8_t>(v >> 8);
        p[1] = static_cast<uint8_t>(v & 0xFF);
    }

    Ring& ring_;
};
