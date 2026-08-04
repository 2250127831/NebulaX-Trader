#pragma once

#include <cstdio>
#include "core/market_event.h"
#include "core/queue/spmc_event_queue.h"
#include "core/queue/spsc_byte_ring.h"
#include "market/parser/itch_parser.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <immintrin.h>   // _mm_pause (背压重试)
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
    // 单通道广播(方案A): 所有事件(成交+委托)进同一队列, 多消费者各自消费关心的类型。
    //   - 策略消费者: 只处理成交(TRADE/EXECUTE), 委托 skip
    //   - 订单簿消费者: 处理全部(成交更新挂单量 + 委托重建盘口)
    // 同一序列 → 订单簿时序正确(成交不会先于对应委托处理)。
    ByteRingParser(SPSCByteRing& ring, EventQueue& channel)
        : ring_(ring), channel_(channel) {
        wake_fd_ = eventfd(0, EFD_NONBLOCK);
        parser_.set_sink([this](const MarketEvent& ev) {
            // 背压: push 满(尝试清理后仍满)返回 false, 使用者重试直到成功(不丢消息)。
            while (!channel_.push(ev)) _mm_pause();
        });
    }
    ~ByteRingParser() {
        if (wake_fd_ >= 0) { uint64_t one = 1; ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r; close(wake_fd_); }
    }
    ByteRingParser(const ByteRingParser&) = delete;
    ByteRingParser& operator=(const ByteRingParser&) = delete;

    int wake_fd() const { return wake_fd_; }
    uint64_t drops_a() const { return drops_a_.load(std::memory_order_relaxed); }
    uint64_t drops_b() const { return drops_b_.load(std::memory_order_relaxed); }

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
        for (;;) {
            // 1. 读头（10 字节: [seq 8][len 2]）。n 是连续部分；used 是 ring 实际占用。
            //    实验A(头帧不跨回绕)由 SPSCByteRing 保证 kHeaderBytes=8 字节头完整。
            const uint8_t* p;
            constexpr size_t kHeadBytes = 10;   // [seq 8][len 2]
            constexpr size_t kSeqBytes  = 8;
            size_t n = ring_.read_acquire(reinterpret_cast<const void*&>(p), kHeadBytes);
            size_t used = ring_.tail() - ring_.head();

            uint64_t seq = 0;
            uint16_t body_len = 0;
            if (n == kHeadBytes) {
                // 头完整：直接读
                for (int i = 0; i < 8; ++i) seq = (seq << 8) | p[i];
                body_len = (static_cast<uint16_t>(p[8]) << 8) | p[9];
            } else if (used >= kHeadBytes) {
                // 尾部空洞(尾部剩余<kHeadBytes, 生产者跳物理开头)。跳到物理开头。
                size_t aligned = (ring_.head() / ring_.capacity() + 1) * ring_.capacity();
                if (aligned > ring_.tail()) break;   // 生产者还没跨越, 等
                ring_.read_release(aligned - ring_.head());
                continue;
            } else {
                break;  // 数据不足，等生产者
            }

            if (body_len < 1 || body_len > 200) {
                ring_.read_release(n);  // 损坏前缀：释放已读的连续部分
                continue;
            }
            size_t msg_len = kHeadBytes + body_len;  // [seq 8][len 2][消息体]

            // 2. 整条消息必须完整在 ring 里才消费。
            if (used < msg_len) break;

            // 3. 读整条消息。实验A: 头帧完整, 消息体可能跨回绕。
            n = ring_.read_acquire(reinterpret_cast<const void*&>(p), msg_len);
            if (n == msg_len) {
                parser_.feed(p + kHeadBytes, body_len, seq);  // 零拷贝：整条连续
                ring_.read_release(msg_len);                  // 处理完才释放整条
            } else {
                // 消息体跨回绕: 拼段(尾部体部分 + 物理开头体部分)。
                size_t n1 = n;              // 头帧+部分体(尾部连续)
                size_t n2 = msg_len - n1;   // 物理开头的体部分
                // 验证: 头部+体都在已发布区(生产者已写物理开头)
                if (ring_.tail() - ring_.head() < msg_len) break;
                uint8_t tmp[512];
                memcpy(tmp, p, n1);
                memcpy(tmp + n1, ring_.raw_buffer(), n2);
                parser_.feed(tmp + kHeadBytes, body_len, seq);
                ring_.read_release(msg_len);
            }
            ++parsed;
        }
        return parsed;
    }

    Ring& ring() { return ring_; }
    uint64_t message_count() const { return parser_.message_count(); }

private:
    Ring& ring_;
    EventQueue& channel_;         // 单通道广播(方案A): 全部事件(成交+委托)
    ItchParser parser_;
    int wake_fd_ = -1;
    std::atomic<uint64_t> drops_a_{0};   // 成交通道满被丢弃的事件数
    std::atomic<uint64_t> drops_b_{0};   // 委托通道满被丢弃的事件数
};
