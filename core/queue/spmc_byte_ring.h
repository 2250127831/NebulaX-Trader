#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <immintrin.h>   // _mm_pause (背压/自旋)

// ── SPMCByteRing: 单生产者多消费者的字节环形缓冲区 ──
//
// 主链路: [recv_th] MoldUdpUnpacker(单写) → SPMCByteRing → N 个解析器(多读)
// 与 SPSCByteRing 的区别: 消费者由 1 个变 N 个, 各解析器抢占不同的消息。
//
// 三指针 + 单持有权模型(逻辑位置, 非物理偏移), 不变量 commit_ ≤ claim_ ≤ tail_:
//   tail_        生产者(unpacker)写推进
//   claim_       下一个可抢位置, 解析器持有者读头后移动(占槽指针)
//   claim_lock_  CAS 单持有权标志, 抢头临界区 test-and-set → 一次一个解析器读头+移动
//   commit_      下一个可释放位置, 解析器按序推进(全局单消费指针)
//
// 为什么抢头串行: 读头要知道长度(len 1-200)才能移动 claim_, 而 claim_ 一次只能被
//   一个解析器持有(否则两个解析器同时读同一条头、都以为独占)。CAS 单持有权标志把
//   "读头 + 移动"串行化 → 一次一人, 天然无竞争。N=1 时退化为 SPSC 语义。
//
// 头不跨回绕(kHeaderBytes=10, [seq8][len2])是正确性根基: push 保证头整体物理连续,
//   解析器抢头时直接读。体(消息剩余)可跨回绕 → 解析器算回绕点拼段读。
//   (kHeaderBytes 8→10 见 spsc_byte_ring.h 的修复记录。)
//
// 存储空间由用户传入(堆 / 共享内存 / 内核映射), 队列不拥有。
// capacity 必须是 2 的幂。
class SPMCByteRing {
public:
    // 头帧长度(与 SPSCByteRing 对齐): [seq 8][len 2] 整体不跨回绕
    static constexpr size_t kHeaderBytes = 10;

    explicit SPMCByteRing(uint8_t* buf, size_t capacity)
        : buf_(buf), capacity_(capacity), mask_(capacity - 1) {}

    // ── 生产者(recv_th): 写入一条消息, 返回实际写入字节数(0=满/物理开头未消费, 忙等重试) ──
    // 空间 = capacity - (tail - commit_): 只能回收已 commit(消费) 的字节。
    //
    // 与 SPSCByteRing 相同的"头不跨回绕、体可跨回绕"布局(消费者已对齐):
    //   - 整条可放尾部(不跨回绕): 直接写, 物理位置 = 逻辑 & mask。
    //   - 尾部剩余 >= kHeaderBytes: 头(10B)写队尾不跨回绕, **只有体**跨回绕
    //     (尾部剩余 + 物理开头拼段)。tail 正常 +len, 无空洞。
    //   - 尾部剩余 <  kHeaderBytes: 跳过尾部碎片(空洞), 整条(含头)写物理开头,
    //     tail 跳变到 aligned。
    // **物理开头覆盖保护**: 跨回绕写物理开头 [0..len) 前, 必须保证其当前内容
    //   (属上一圈 [aligned-cap, aligned-cap+len))已全部 commit → aligned+len <= commit+capacity。
    //   (ring 满时 commit 滞后 → return 0 等解析器推进, 但 tail 不再增长, 无死锁:
    //   解析器持续解析已写数据推进 commit, 最终满足 → 可写)
    // **空洞(物理尾部碎片, 逻辑 [tail, aligned))由消费者 has_gap→skip_gap 跳过**。
    size_t push(const void* data, size_t len) {
        size_t commit = commit_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t pos = tail & mask_;

        // 整条可放尾部(物理连续): 写尾, 与 SPSC 同款 free 检查
        if (pos + len <= capacity_) {
            size_t free = capacity_ - (tail - commit);
            if (free < len || len == 0) return 0;
            memcpy(buf_ + pos, data, len);
            tail_.store(tail + len, std::memory_order_release);
            return len;
        }

        // 跨回绕: 头不跨回绕、体可跨回绕(与 SPSC 对齐)。
        //   aligned = 下一圈开头。物理开头 [0..len) 可写当 commit >= aligned-cap+len
        //   (其当前内容属上一圈 [aligned-cap, aligned-cap+len), 已消费才可覆盖)。
        size_t aligned = (tail / capacity_ + 1) * capacity_;
        if (aligned + len > commit + capacity_) return 0;   // 物理开头有未 commit 字节, 等
        const auto* db = static_cast<const uint8_t*>(data);
        size_t tail_room = capacity_ - pos;
        if (tail_room >= kHeaderBytes) {
            // 头(10B)写队尾不跨回绕, 体跨回绕(尾部剩余 + 物理开头拼段)。
            // tail 正常 +len, 无空洞。
            memcpy(buf_ + pos, db, kHeaderBytes);
            size_t body_room = tail_room - kHeaderBytes;     // 尾部给体的空间
            size_t body_len = len - kHeaderBytes;
            if (body_len > body_room) {
                memcpy(buf_ + pos + kHeaderBytes, db + kHeaderBytes, body_room);
                memcpy(buf_, db + kHeaderBytes + body_room, body_len - body_room);
            } else {
                memcpy(buf_ + pos + kHeaderBytes, db + kHeaderBytes, body_len);
            }
            tail_.store(tail + len, std::memory_order_release);
            return len;
        }
        // tail_room < kHeaderBytes: 跳过尾部(空洞), 整条(含头)写物理开头, tail 跳变到 aligned。
        memcpy(buf_, data, len);
        tail_.store(aligned + len, std::memory_order_release);
        return len;
    }

    // ── 消费者(解析器) ──

    // 抢头临界区入口: CAS 拿单持有权(claim_lock_ 0→1)。返回 true=拿到独占权。
    // 拿到后必须调 claim_end() 释放; 未拿到继续自旋重试。
    bool claim_lock() {
        uint32_t expected = 0;
        return claim_lock_.compare_exchange_strong(expected, 1,
                    std::memory_order_acquire, std::memory_order_relaxed);
    }

    // 当前占槽位置(仅 claim_lock 持有者调用, 临界区内)
    size_t claim_pos() const { return claim_.load(std::memory_order_relaxed); }
    // tail acquire: 解析器靠 "cl+len <= tail" 判断消息已完整写入(push 的 release store
    //  建立 happens-before), 之后 peek/read_body 读的字节才与 push 写同步(TSAN 也认这条边)。
    size_t tail() const { return tail_.load(std::memory_order_acquire); }
    size_t capacity() const { return capacity_; }

    // 空洞检测: 逻辑位置 cl 落在物理尾部碎片(producer 跨回绕跳过了, 空洞无消息)。
    //   pos + kHeaderBytes > capacity → 头在物理尾放不下 → 空洞。
    bool has_gap(size_t cl) const {
        return (cl & mask_) + kHeaderBytes > capacity_;
    }

    // 空洞跳跃: claim_ 前进到 aligned(空洞无消息, 下一条空洞消息 cl=aligned 物理 0)。
    void skip_gap(size_t cl) {
        claim_.store((cl / capacity_ + 1) * capacity_, std::memory_order_release);
    }

    // 空洞是否可跳(producer 已跨越 aligned, 空洞消息已写物理开头): aligned <= tail。
    bool gap_writable(size_t cl) const {
        size_t aligned = (cl / capacity_ + 1) * capacity_;
        return aligned <= tail_.load(std::memory_order_acquire);
    }

    // 下一圈开头(空洞跳跃目标)。
    size_t aligned_of(size_t cl) const { return (cl / capacity_ + 1) * capacity_; }

    // 消息头物理起点 = 逻辑位置 & mask(恒成立):
    //   - 非空洞消息: 头在队尾(producer 跨回绕时头仍留队尾, 只有体跨回绕)。
    //   - 空洞消息(tail_room<10 被跳过): cl=aligned → cl&mask=0, 整条(含头)在物理开头。
    // 两种情况下头(10B)都物理连续, 物理起点都是 cl&mask。
    size_t phys_of(size_t cl) const { return cl & mask_; }

    // 读指定物理位置的头 body_len(供空洞 double-peek)。
    uint16_t peek_header_at(size_t phys) {
        uint8_t* p = buf_ + phys;
        return (static_cast<uint16_t>(p[8]) << 8) | p[9];
    }

    // 读头(10 字节, 物理连续)。返回消息体长度 body_len。调用方需处理空洞 double-peek。
    uint16_t peek_header(size_t cl) {
        return peek_header_at(cl & mask_);
    }

    // 读头 seq(前 8 字节), 指定物理位置。
    uint64_t peek_seq_at(size_t phys) const {
        const uint8_t* p = buf_ + phys;
        uint64_t seq = 0;
        for (int i = 0; i < 8; ++i) seq = (seq << 8) | p[i];
        return seq;
    }
    uint64_t peek_seq(size_t cl) const { return peek_seq_at(cl & mask_); }

    // 消息逻辑终点: 恒 = cl + len(消息逻辑连续, 空洞由 has_gap→skip_gap 跳过 claim_,
    // 不进入消息序列)。claim_/commit_ 逻辑连续, 物理 = 逻辑 & mask。
    size_t next_pos(size_t cl, size_t len) const { return cl + len; }

    // 抢头完成: 移动占槽指针 claim_ 到 cl+len, 释放单持有权。
    void claim_end(size_t cl, size_t len) {
        claim_.store(cl + len, std::memory_order_release);
        claim_lock_.store(0, std::memory_order_release);
    }

    // 抢头放弃(等生产者/让出): 不移动 claim_, 只释放单持有权。
    void claim_release() {
        claim_lock_.store(0, std::memory_order_release);
    }

    // 读消息体(头在 phys_of(cl), 体从 phys+kHeaderBytes 起, 可能跨物理回绕拼段)。
    size_t read_body_at(size_t phys, size_t len, uint8_t* out) {
        size_t body_len = len - kHeaderBytes;
        size_t body_start = (phys + kHeaderBytes) & mask_;
        if (body_start + body_len <= capacity_) {
            memcpy(out, buf_ + body_start, body_len);          // 体不跨回绕
        } else {
            size_t n1 = capacity_ - body_start;                // 体跨回绕(物理尾+物理开头)
            memcpy(out, buf_ + body_start, n1);
            memcpy(out + n1, buf_, body_len - n1);
        }
        return body_len;
    }

    // 保序屏障: 等 commit_ == pos(前面所有消息已提交)。自旋。
    void wait_commit(size_t pos) {
        while (commit_.load(std::memory_order_acquire) != pos)
            _mm_pause();
    }

    // 推进全局消费指针 commit_ 到消息逻辑终点(跳过空洞, 与 tail/claim 跳变对齐)。
    void release_commit(size_t pos, size_t len) {
        commit_.store(next_pos(pos, len), std::memory_order_release);
    }

    size_t commit() const { return commit_.load(std::memory_order_relaxed); }

    // 全局 drain 判定: 所有消息已提交(解析器可退出)
    bool drained() const { return commit_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire); }

    uint8_t* raw_buffer() { return buf_; }

private:
    alignas(64) std::atomic<size_t> tail_{0};       // 生产者写推进
    alignas(64) std::atomic<uint32_t> claim_lock_{0}; // 抢头单持有权(CAS test-and-set)
    alignas(64) std::atomic<size_t> claim_{0};      // 占槽指针(解析器移动)
    alignas(64) std::atomic<size_t> commit_{0};     // 全局单消费指针(按序推进)
    uint8_t* const buf_;
    const size_t capacity_;
    const size_t mask_;
};
