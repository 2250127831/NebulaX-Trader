#pragma once

#include <cstdio>
#include "core/market_event.h"
#include "core/queue/spmc_byte_ring.h"
#include "core/queue/spmc_event_queue.h"
#include "market/parser/itch_parser.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <immintrin.h>   // _mm_pause (背压重试)
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

// ── 字节 Ring 解析器（V2.3 多解析器版）──
// N 个解析器从 SPMCByteRing 并行抢消息、并行解析，提交严格保序。
//
//   [网络接收线程] recv → SPMCByteRing.push (单生产者)
//   [N 个解析线程] claim_lock(抢头串行) → peek 头 → claim_end(移动占槽指针)
//                    → 解析 body(并行) → wait_commit(保序屏障) → push 事件 SPMC
//                    → release_commit(推进消费指针)
//
// 抢头串行、解析并行、提交保序:
//   - claim_lock_ CAS 单持有权: 一次一个解析器读头+移动 claim_ (串行抢头)
//   - 解析 body 各解析器独立 (并行)
//   - wait_commit 等 commit_==自己位置才 push (保序, 事件 SPMC 保持单生产者)
//   - release_commit 推进全局消费指针
//
// 每个解析器一个 ByteRingParser 实例(own ItchParser / staging / wake_fd),
// 共享 SPMCByteRing& + 事件 SPMCEventQueue&。
class ByteRingParser {
public:
    using Ring = SPMCByteRing;
    using EventQueue = SPMCEventQueue<16>;

    // 消息体最大长度(ITCH 消息 ≤200 字节)
    static constexpr size_t kMaxBody = 200;

    ByteRingParser(Ring& ring, EventQueue& channel)
        : ring_(ring), channel_(channel) {
        wake_fd_ = eventfd(0, EFD_NONBLOCK);
        // sink 只暂存到 staging(不 push): push 必须等 commit 屏障保序,
        // 由解析循环在 wait_commit 通过后统一 push。
        parser_.set_sink([this](const MarketEvent& ev) {
            staging_[staging_len_++] = ev;
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
    bool wait_for_data(int timeout_ms = 1000) {
        struct pollfd pfd = {wake_fd_, POLLIN, 0};
        int ret = poll(&pfd, 1, timeout_ms);
        if (ret > 0) {
            uint64_t ev;
            ssize_t r = read(wake_fd_, &ev, sizeof(ev)); (void)r;
            return true;
        }
        return false;
    }

    // 解析 ring 里所有可抢的消息(并行抢 + 并行解析 + 保序提交)。返回处理条数。
    //
    // 抢头(串行): claim_lock 拿独占权 → 读当前 claim_ → peek 头得长度 →
    //   claim_end 移动 claim_ + 释放独占权(下一条给别人抢)。
    //   空洞跳过: claim_ 在物理尾部保护区(头放不下) → 跳下一圈开头。
    // 解析(并行): 各解析器独立解析自己 claim 的消息 body。
    // 提交(保序): wait_commit 等 commit_==自己位置 → push staging 到事件 SPMC →
    //   release_commit 推进全局消费指针。
    size_t parse_available() {
        size_t parsed = 0;
        for (;;) {
            // ── 抢头临界区(串行) ──
            if (!ring_.claim_lock()) break;   // 没拿到独占权, 让给别的解析器
            size_t cl = ring_.claim_pos();
            // 空洞: 逻辑位置在物理尾部碎片(producer 跨回绕跳过, 无消息)。
            //   producer 已跨越 aligned 时: 等 commit==cl(保序) → 推进 commit 到 aligned
            //   (空洞无消息, commit 直接跳过) → skip_gap 到 aligned(空洞消息 cl=aligned)。
            //   否则等生产者跨越。
            if (ring_.has_gap(cl)) {
                if (ring_.gap_writable(cl)) {
                    ring_.wait_commit(cl);
                    ring_.release_commit(cl, ring_.aligned_of(cl) - cl);   // commit 跳过空洞
                    ring_.skip_gap(cl);                                    // claim_ → aligned
                    ring_.claim_release();
                    continue;
                }
                ring_.claim_release();       // 等生产者跨越空洞
                break;
            }
            // 头没到(生产者没写) → 释放, 等数据
            if (cl + Ring::kHeaderBytes > ring_.tail()) {
                ring_.claim_release();
                break;
            }
            size_t phys = ring_.phys_of(cl);
            uint16_t body_len = ring_.peek_header_at(phys);
            size_t msg_len = Ring::kHeaderBytes + body_len;
            if (body_len < 1 || body_len > kMaxBody) {
                // 损坏前缀(读到非法 body_len): 数据未就绪或损坏, 跳 kHeaderBytes 保持同步
                ring_.claim_end(cl, Ring::kHeaderBytes);
                ring_.wait_commit(cl);
                ring_.release_commit(cl, Ring::kHeaderBytes);
                continue;
            }
            // 整条没到 → 释放, 等生产者
            if (cl + msg_len > ring_.tail()) {
                ring_.claim_release();
                break;
            }
            // 抢到 [cl, cl+msg_len), 移动 claim_ 释放独占权
            ring_.claim_end(cl, msg_len);

            // ── 解析(并行, 各解析器独立) ──
            staging_len_ = 0;
            uint8_t body[kMaxBody];
            ring_.read_body_at(phys, msg_len, body);
            uint64_t seq = ring_.peek_seq_at(phys);
            parser_.feed(body, body_len, seq);

            // ── 提交(保序) ──
            ring_.wait_commit(cl);                    // 等 commit_==cl(前面提交完)
            for (size_t i = 0; i < staging_len_; ++i) {
                const MarketEvent& ev = staging_[i];
                while (!channel_.push(ev)) _mm_pause();
                if (ev.seq_id % lensx::kSample == 0) lensx::mark_push_spmc(ev.seq_id);
            }
            ring_.release_commit(cl, msg_len);        // 推进消费指针
            ++parsed;
        }
        return parsed;
    }

    Ring& ring() { return ring_; }

    // 消息总数(跨实例求和由 main 汇总)
    uint64_t message_count() const { return parser_.message_count(); }

private:
    Ring& ring_;
    EventQueue& channel_;
    ItchParser parser_;
    int wake_fd_ = -1;
    MarketEvent staging_[16];       // 暂存本次消息产出的事件(commit 屏障后 push)
    size_t staging_len_ = 0;
    std::atomic<uint64_t> drops_a_{0};
    std::atomic<uint64_t> drops_b_{0};
};
