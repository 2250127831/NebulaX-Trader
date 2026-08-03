#pragma once

#include "core/market_event.h"
#include "core/queue/spmc_event_queue.h"
#include "core/queue/spsc_byte_ring.h"
#include "market/parser/itch_parser.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

// ── 字节 Ring 解析器 ──
// 从 SPSCByteRing 读连续字节流，原地解析 ITCH 消息。
//
//   [网络接收线程] recv → push 字节 ring
//   [解析线程]     read_acquire(连续内存) → 读长度前缀 → 按消息长度原地解析 → read_release
//                    → MarketEvent → push 事件队列
//
// 为什么 read_acquire 原地解析（而非 pop 取出）：
//   V2 升级 SPMC 时，多解析消费者共享同一个字节 ring，不能"取出"，
//   必须各自 read_acquire 到自己的位置，原地解析，按顺序 read_release。
//   现在就用这个模式，为 SPMC 铺路。
//
// 跨回绕处理：
//   read_acquire 最多返回 ring 尾部的连续字节，消息跨回绕会被截断。
//   此时 fallback 到 pop（memcpy 拼两段），保证正确性。
//   消息最大 200 字节，ring 8MB，跨回绕极罕见，fallback 不影响性能。
//
// 用法：
//   ByteRingParser parser;
//   parser.set_sink([](const MarketEvent& ev){ /* 处理 */ });
//   while (running) parser.parse_available();  // 解析线程循环
class ByteRingParser {
public:
    using Ring = SPSCByteRing;
    using EventQueue = SPMCEventQueue<16>;

    // ── eventfd 唤醒（学撮合引擎 poll + eventfd 方案）──
    // 解析线程 poll(wake_fd) 阻塞等数据；生产者 push 完 write(wake_fd) 唤醒。
    // V2 多消费者：所有消费者 poll 同一个 wake_fd，写一次全醒（广播）。
    // 构造传入共享 ring（unpacker 写、bp 读）+ 通道 A（成交）+ 通道 B（委托）。
    //   成交事件(TRADE/EXECUTE) → 通道 A（低频策略消费）
    //   委托事件(ADD/DELETE/CANCEL/REPLACE) → 通道 B（订单簿/逐笔委托策略消费）
    ByteRingParser(SPSCByteRing& ring, EventQueue& channel_a, EventQueue& channel_b)
        : ring_(ring), channel_a_(channel_a), channel_b_(channel_b) {
        wake_fd_ = eventfd(0, EFD_NONBLOCK);
        parser_.set_sink([this](const MarketEvent& ev) {
            if (ev.type == MarketEvent::Type::TRADE ||
                ev.type == MarketEvent::Type::EXECUTE) {
                channel_a_.push(ev);   // 成交事件 → 通道 A（低频策略消费）
            } else {
                channel_b_.push(ev);   // 委托事件 → 通道 B（订单簿/逐笔策略消费）
            }
        });
    }
    ~ByteRingParser() {
        if (wake_fd_ >= 0) { uint64_t one = 1; ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r; close(wake_fd_); }
    }
    ByteRingParser(const ByteRingParser&) = delete;
    ByteRingParser& operator=(const ByteRingParser&) = delete;

    int wake_fd() const { return wake_fd_; }

    // 生产者：唤醒可能睡眠的解析线程（写完 ring 后调用）
    void notify() {
        if (wake_fd_ >= 0) {
            uint64_t one = 1;
            ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r;
        }
    }

    // 解析线程：阻塞等数据。poll(wake_fd) 直到被唤醒（或 timeout_ms 超时）。
    // 返回 true 表示有唤醒（可能有数据），false 表示超时。
    // 唤醒后应调 parse_available() 消费。
    bool wait_for_data(int timeout_ms = 1000) {
        struct pollfd pfd = {wake_fd_, POLLIN, 0};
        int ret = poll(&pfd, 1, timeout_ms);
        if (ret > 0) {
            uint64_t ev;
            ssize_t r = read(wake_fd_, &ev, sizeof(ev)); (void)r;  // 消费唤醒计数（EFD_NONBLOCK）
            return true;
        }
        return false;  // 超时或错误
    }

    // 解析 ring 里所有完整消息。返回处理了多少条消息。
    //
    // 不跨回绕：read_acquire 拿完整消息连续内存 → 原地解析 → read_release（零拷贝）。
    // 跨回绕：read_acquire 只能拿连续部分（到尾为止），用 pop（原子读，不足不释放）
    //   拼段读完整消息 → 解析。跨回绕罕见（消息 ≤200B，ring 大），不影响性能。
    // 不足：pop 返回 0 不释放，break 等更多数据。
    size_t parse_available() {
        size_t parsed = 0;
        uint8_t tmp[512];
        for (;;) {
            // 1. 读头（4 字节: [seq 2][len 2]）。n 是连续部分；used 是 ring 实际占用。
            const uint8_t* p;
            size_t n = ring_.read_acquire(reinterpret_cast<const void*&>(p), 4);
            size_t used = ring_.tail() - ring_.head();

            uint16_t seq, body_len;
            if (n == 4) {
                // 头完整：直接读
                seq      = (static_cast<uint16_t>(p[0]) << 8) | p[1];
                body_len = (static_cast<uint16_t>(p[2]) << 8) | p[3];
            } else if (used >= 4) {
                // 头跨回绕（used 够但连续部分 < 4）：
                // 尾部 n 字节 + ring 头部补足，拼出完整头。
                uint8_t hdr[4];
                memcpy(hdr, p, n);
                memcpy(hdr + n, ring_.raw_buffer(), 4 - n);
                seq      = (static_cast<uint16_t>(hdr[0]) << 8) | hdr[1];
                body_len = (static_cast<uint16_t>(hdr[2]) << 8) | hdr[3];
            } else {
                break;  // 数据不足，等生产者
            }

            if (body_len < 1 || body_len > 200) {
                ring_.read_release(n);  // 损坏前缀：释放已读的连续部分（尽力对齐）
                continue;
            }
            size_t msg_len = 4 + body_len;  // [seq 2][len 2][消息体]

            // 2. 整条消息必须完整在 ring 里才消费，否则等生产者推完。
            //    防止头跨回绕时直接读整条读到野内存。
            if (used < msg_len) break;

            // 3. 读整条消息。n 是连续部分；若 < msg_len，跨回绕（尾部 + 头部）。
            n = ring_.read_acquire(reinterpret_cast<const void*&>(p), msg_len);
            if (n == msg_len) {
                parser_.feed(p + 4, body_len, seq);  // 零拷贝：整条连续
                ring_.read_release(msg_len);         // 处理完才释放整条
            } else {
                // 跨回绕：copy 整条到临时 buffer，整体处理，处理完才释放。
                size_t n1 = n;              // 尾部段（到 ring 末尾）
                size_t n2 = msg_len - n1;   // 头部段（ring 头部）
                memcpy(tmp, p, n1);
                memcpy(tmp + n1, ring_.raw_buffer(), n2);
                parser_.feed(tmp + 4, body_len, seq);
                ring_.read_release(msg_len);  // 处理完才释放整条
            }
            ++parsed;
        }
        return parsed;
    }

    Ring& ring() { return ring_; }
    uint64_t message_count() const { return parser_.message_count(); }

private:
    Ring& ring_;
    EventQueue& channel_a_;       // 成交事件广播通道（低频策略消费）
    EventQueue& channel_b_;       // 委托事件广播通道（订单簿/逐笔策略消费）
    ItchParser parser_;
    int wake_fd_ = -1;
};
