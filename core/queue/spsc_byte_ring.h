#pragma once

#include <atomic>
#include <cstddef>
#include <cstring>
#include <sys/uio.h>

// ── SPSCByteRing: 单生产者单消费者的字节环形缓冲区 ──
//
// push(data, len): 写入尽可能多的字节，返回实际写入数
// pop(buf, len):   读出尽可能多的字节，返回实际读出数
// read_acquire / read_release: 零拷贝读（获取内部指针）
//
// 存储空间由用户传入（堆分配 / 共享内存 / 内核映射），队列不拥有。
// capacity 必须是 2 的幂（构造时校验）。
//
// 用法：
//   uint8_t* buf = new uint8_t[1 << 20];
//   SPSCByteRing ring(buf, 1 << 20);
class SPSCByteRing
{
public:
    // 用户传入存储空间 + 容量。capacity 必须 2 的幂（不满足返回 false，需检查 valid()）。
    SPSCByteRing(uint8_t* buf, size_t capacity)
        : buf_(buf), capacity_(capacity), valid_((capacity & (capacity - 1)) == 0) {}

    // 默认构造：不绑定空间，需先 valid() 检查（或用于测试）
    SPSCByteRing() = default;

    SPSCByteRing(const SPSCByteRing&) = delete;
    SPSCByteRing& operator=(const SPSCByteRing&) = delete;

    // 空间是否合法（2 的幂 + 已绑定）
    bool valid() const { return valid_ && buf_ != nullptr; }
    size_t capacity() const { return capacity_; }

    // ── producer ──

    size_t push(const void* data, size_t len)
    {
        // SPSC 下 head 只被消费者 read_release 单调推进。push 读 head(acquire,
        // 保证看到消费者已 release 的进度), 计算空闲空间。
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t mask = capacity_ - 1;
        size_t pos  = tail & mask;

        // 不跨回绕: 整条放尾部剩余, 正常写
        if (pos + len <= capacity_) {
            size_t free = capacity_ - (tail - head);
            if (free < len || len == 0) return 0;
            memcpy(buf_ + pos, data, len);
            tail_.store(tail + len, std::memory_order_release);
            return len;
        }

        // 跨回绕: 空洞方案。尾部剩余空间标为空洞, tail 跨越到物理开头写整条。
        // 约定(与消费者共享):
        //   - 尾部剩余 >= 4: 写空洞头 [seq=0][len=0], 消费者读 len==0 跳过
        //   - 尾部剩余 <  4: 不写(生产者不用 <4 的尾部), 消费者读到尾部<4 直接跳过
        // 消息不跨回绕 → 消费者读连续数据, 无拼段竞态(根治 flaky)。
        size_t aligned = (tail / capacity_ + 1) * capacity_;   // 下一圈开头
        if (aligned + len > head + capacity_) return 0;         // 物理开头空间不足
        size_t tail_room = capacity_ - pos;
        if (tail_room >= 4) {
            // 空洞头: [seq 0][len 0], 只填 len=0(消费者据此识别), seq 填 0
            buf_[pos + 0] = 0;
            buf_[pos + 1] = 0;
            buf_[pos + 2] = 0;   // len 高字节
            buf_[pos + 3] = 0;   // len 低字节 = 0 (空洞)
        }
        // tail_room < 4: 不写, 消费者读到尾部<4 即跳过(生产者不用 <4 尾部)
        memcpy(buf_, data, len);            // 消息写物理开头
        tail_.store(aligned + len, std::memory_order_release);
        return len;
    }

    // ── consumer: 零拷贝读 ──

    // 返回 ring 内部连续数据指针，调用方读完后必须 call read_release
    size_t read_acquire(const void*& ptr, size_t request)
    {
        size_t tail = tail_.load(std::memory_order_acquire);
        size_t head = head_.load(std::memory_order_relaxed);
        size_t used = tail - head;
        if (used == 0 || request == 0) { ptr = nullptr; return 0; }

        size_t actual = (request < used) ? request : used;
        size_t mask   = capacity_ - 1;
        size_t pos    = head & mask;
        size_t contig = capacity_ - pos;
        if (actual > contig) actual = contig;
        ptr = buf_ + pos;
        return actual;
    }

    void read_release(size_t bytes)
    {
        head_.store(head_.load(std::memory_order_relaxed) + bytes,
                    std::memory_order_release);
    }

    // ── consumer: 带拷贝读取 ──
    //
    // 原子读：只有拿到完整 len 字节才释放，不足时什么都不释放（返回实际可读，
    // 但字节保留在 ring，调用方应等更多数据再试）。这避免了"部分消费导致丢字节"。

    size_t pop(void* buf, size_t len)
    {
        const void* ptr;
        size_t n1 = read_acquire(ptr, len);
        if (n1 == 0) return 0;
        memcpy(buf, ptr, n1);
        if (n1 == len) {            // 一次读全（不跨回绕）
            read_release(n1);
            return n1;
        }

        size_t remaining = len - n1;
        size_t n2 = read_acquire(ptr, remaining);
        if (n2 < remaining) {
            // 第二段不足：不释放任何字节（n1 也不释放），等更多数据
            return 0;
        }
        memcpy(static_cast<uint8_t*>(buf) + n1, ptr, n2);
        read_release(len);          // 完整拿到，一次释放全部
        return len;
    }

    // ── queries & buffer 暴露 ──

    size_t free_space() const
    {
        size_t h = head_.load(std::memory_order_acquire);
        size_t t = tail_.load(std::memory_order_relaxed);
        return capacity_ - (t - h);
    }

    uint8_t* raw_buffer() { return buf_; }
    size_t   raw_size() const { return capacity_; }
    struct iovec raw_iovec() { return { buf_, capacity_ }; }
    // 外部只读：暴露读写位置（供监控端 mmap 后直接读）
    size_t tail() const { return tail_.load(std::memory_order_relaxed); }
    size_t head() const { return head_.load(std::memory_order_relaxed); }
    bool empty() const { return head() == tail(); }

private:
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) std::atomic<size_t> head_{0};
    uint8_t* buf_ = nullptr;        // 用户传入的存储空间（队列不拥有）
    size_t   capacity_ = 0;         // 容量（运行时）
    bool     valid_ = false;        // 空间是否合法（2 的幂 + 已绑定）
};
