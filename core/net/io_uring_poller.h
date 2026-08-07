#pragma once

#include <liburing.h>
#include <cstdint>
#include <functional>

// ── io_uring 事件轮询器（迁移自 NebulaX，UDP 化）──
// 封装 io_uring 的 SQE/CQE 管理 + 固定缓冲区池 + 可打断等待。
// 只负责接收路径（recv）；发送路径见 io_uring_sender。
//
// 相对 NebulaX 的裁剪：
//   - 去掉 TCP 部分（accept / per-connection / keepalive / close 流程）
//   - 保留：固定 buffer 池、POLL_FIRST、submit_and_wait_timeout、CQE 分发

// IORING_RECVSEND_POLL_FIRST: 让 io_uring recv 先 poll 而非立即回 -EAGAIN
// 内核 5.19+，系统头文件可能未导出，手动保证
#ifndef IORING_RECVSEND_POLL_FIRST
#define IORING_RECVSEND_POLL_FIRST (1U << 0)
#endif

class IoUringPoller
{
public:
    static constexpr unsigned DEFAULT_ENTRIES = 256;
    static constexpr unsigned MAX_BUFFERS = 64;
    static constexpr size_t   BUF_SIZE   = 8192;   // > 最大 UDP 包(benchmark 最大 4168B)

    IoUringPoller() = default;

    // 初始化 ring + 预注册固定缓冲区池。
    // 固定缓冲区的价值：recv 免去每次 get_user_pages（内核直写）。
    bool init(unsigned entries = DEFAULT_ENTRIES)
    {
        if (io_uring_queue_init(entries, &ring_, 0) < 0)
            return false;

        for (unsigned i = 0; i < MAX_BUFFERS; ++i) {
            iovs_[i].iov_base = bufs_[i];
            iovs_[i].iov_len  = BUF_SIZE;
            buf_free_stack_[i] = MAX_BUFFERS - 1 - i;  // 倒序入栈
        }
        buf_free_top_ = MAX_BUFFERS;

        if (io_uring_register_buffers(&ring_, iovs_, MAX_BUFFERS) < 0) {
            io_uring_queue_exit(&ring_);
            return false;
        }
        ok_ = true;
        return true;
    }

    ~IoUringPoller()
    {
        if (ok_) {
            io_uring_unregister_buffers(&ring_);
            io_uring_queue_exit(&ring_);
        }
    }

    IoUringPoller(const IoUringPoller&) = delete;
    IoUringPoller& operator=(const IoUringPoller&) = delete;

    bool ok() const { return ok_; }

    // ── 固定缓冲区管理 ──

    uint32_t alloc_buffer()
    {
        if (buf_free_top_ == 0) return UINT32_MAX;
        return buf_free_stack_[--buf_free_top_];
    }

    void free_buffer(uint32_t idx)
    {
        if (buf_free_top_ < MAX_BUFFERS)
            buf_free_stack_[buf_free_top_++] = idx;
    }

    char* buffer_ptr(uint32_t idx) { return bufs_[idx]; }

    // ── SQE 提交 ──

    // recv（带 POLL_FIRST）：无数据时内核 poll 等待，不立即回 -EAGAIN
    bool submit_recv(int fd, uint32_t buf_idx)
    {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return false;
        io_uring_prep_recv(sqe, fd, bufs_[buf_idx], BUF_SIZE, 0);
        sqe->buf_index = buf_idx;
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(fd)));
        return true;
    }

    // 提交 recv 并立即 submit（非阻塞路径需要：submit 之后 CQE 才会产生）
    bool submit_recv_now(int fd, uint32_t buf_idx)
    {
        if (!submit_recv(fd, buf_idx)) return false;
        return io_uring_submit(&ring_) >= 0;
    }

    int submit_and_wait()
    {
        return io_uring_submit_and_wait(&ring_, 1);
    }

    // 带超时的 submit_and_wait（用于优雅关闭时不被永久阻塞）。
    // timeout_ms 后即使无 CQE 也会返回。
    // 超时 SQE 的 user_data 用哨兵值，避免 CQE 残留被误判为 recv 结果。
    int submit_and_wait_timeout(uint64_t timeout_ms)
    {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return submit_and_wait();
        struct __kernel_timespec ts;
        ts.tv_sec  = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000;
        io_uring_prep_timeout(sqe, &ts, 1, 0);
        io_uring_sqe_set_data(sqe, k_timeout_token);
        return io_uring_submit_and_wait(&ring_, 1);
    }

    // 非阻塞查看：有无已完成 CQE 立即知道，不等待。
    bool has_cqe()
    {
        struct io_uring_cqe* cqe;
        return io_uring_peek_cqe(&ring_, &cqe) == 0;
    }

    // ── 多在途 recv（V2.4: 单线程预提交多 SQE, 内核并行收包）──
    // 与单包路径(submit_recv/process_cqes)并存: 主行情 recv_th 用此路径,
    // fill_rcv 低频回报仍走单包 recv()。两条路径不混用同一实例。
    //
    // buffer 状态机(总量守恒 = MAX_BUFFERS):
    //   自由池 →(submit_inflight)→ 在途 →(reap_more)→ ready队列 →(take_ready)→
    //   处理中 →(resubmit_recv)→ 在途      (循环复用, 在途 SQE 数恒定)
    //   错误 CQE(如 stop close(fd)) → 自由池
    //
    // user_data = buf_idx(单包路径是 fd, 互不混淆)。CQE 回来能定位数据落哪个固定 buffer。

    // 预提交 count 个 recv SQE(自由池耗尽则少提)。返回实际提交数。
    unsigned submit_inflight(int fd, unsigned count)
    {
        unsigned submitted = 0;
        for (unsigned i = 0; i < count; ++i) {
            uint32_t bidx = alloc_buffer();
            if (bidx == UINT32_MAX) break;
            if (!fill_recv_sqe(fd, bidx)) { free_buffer(bidx); break; }
            ++submitted;
        }
        if (submitted) io_uring_submit(&ring_);
        return submitted;
    }

    // 阻塞等 >=1 个 CQE, 成功 recv 的 buffer 加入 ready 队列(数据保留, 未处理)。
    // 带超时(50ms): stop() 靠 close(fd) 异步取消在途 recv, 无超时可能永久阻塞
    //   (io_uring 取消延迟)。超时后返回 0, 调用方检查 running_ 决定退出。
    // 返回新增 ready 数; 出现错误 CQE(如 stop close(fd) 打断在途 recv) 返回 -1。
    int reap_more(int /*fd*/)
    {
        submit_and_wait_timeout(50);
        struct io_uring_cqe* cqe;
        unsigned head;
        int added = 0;
        bool saw_error = false;
        io_uring_for_each_cqe(&ring_, head, cqe) {
            void* data = io_uring_cqe_get_data(cqe);
            io_uring_cqe_seen(&ring_, cqe);
            if (data == k_timeout_token) continue;
            uint32_t b = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(data));
            if (cqe->res >= 0) {
                if (ready_len_ < MAX_BUFFERS) {
                    ready_[ (ready_head_ + ready_len_) & (MAX_BUFFERS - 1) ] = b;
                    ready_res_[ (ready_head_ + ready_len_) & (MAX_BUFFERS - 1) ] = cqe->res;
                    ++ready_len_; ++added;
                } else {
                    free_buffer(b);   // ready 满: 归还自由池(不应发生, take_ready 持续消费)
                }
            } else {
                saw_error = true;
                free_buffer(b);       // 错误(如 -EBADF): buffer 不再在途, 归还自由池
            }
        }
        return saw_error ? -1 : added;
    }

    // 取一个 ready buffer(非阻塞)。无则 UINT32_MAX。out_bytes 输出包长。
    uint32_t take_ready(int& out_bytes)
    {
        if (ready_len_ == 0) return UINT32_MAX;
        uint32_t slot = ready_head_ & (MAX_BUFFERS - 1);
        uint32_t b = ready_[slot];
        out_bytes = ready_res_[slot];
        ++ready_head_; --ready_len_;
        return b;
    }
    bool ready_empty() const { return ready_len_ == 0; }

    // 调用方处理完该 buffer 后重新提交 recv(循环复用, 在途 SQE 数保持)。
    // **只填 SQE 不 submit**(攒批): 由下一次 reap_more 的 submit_and_wait_timeout
    //   一次性提交所有攒的 SQE。避免每包单独 io_uring_submit syscall(批量 rate 15000
    //   实测每包 2 次 enter 反而比单包 1 次慢)。
    void resubmit_recv(int fd, uint32_t bidx)
    {
        if (!fill_recv_sqe(fd, bidx))
            free_buffer(bidx);   // ring 满: 归还自由池, 下次 submit_inflight 再提
    }

    // 处理当前批次所有 CQE。on_recv(fd, bytes_read)：bytes_read 即 recv 结果
    void process_cqes(const std::function<void(int, int)>& on_recv)
    {
        struct io_uring_cqe* cqe;
        unsigned head;

        io_uring_for_each_cqe(&ring_, head, cqe) {
            void* data = io_uring_cqe_get_data(cqe);
            if (data == k_timeout_token) {  // 超时 CQE，跳过
                io_uring_cqe_seen(&ring_, cqe);
                continue;
            }
            int fd = static_cast<int>(reinterpret_cast<uintptr_t>(data));
            on_recv(fd, cqe->res);
            io_uring_cqe_seen(&ring_, cqe);
        }
    }

private:
    // timeout SQE 的 user_data 哨兵，与 recv 的 fd 区分
    inline static void* const k_timeout_token = reinterpret_cast<void*>(~static_cast<uintptr_t>(0));

    // 填一个 recv SQE, user_data=buf_idx(批量路径 CQE 定位 buffer)。不 submit。
    bool fill_recv_sqe(int fd, uint32_t bidx)
    {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return false;
        io_uring_prep_recv(sqe, fd, bufs_[bidx], BUF_SIZE, 0);
        sqe->buf_index = bidx;
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(bidx)));
        return true;
    }

    struct io_uring ring_{};
    bool ok_ = false;

    // 固定缓冲区池
    char bufs_[MAX_BUFFERS][BUF_SIZE]{};
    struct iovec iovs_[MAX_BUFFERS]{};
    uint32_t buf_free_stack_[MAX_BUFFERS]{};
    uint32_t buf_free_top_ = 0;

    // 多在途 recv 的 ready 队列(recv 完成、数据待处理的 buffer)
    uint32_t ready_[MAX_BUFFERS]{};
    int ready_res_[MAX_BUFFERS]{};
    uint32_t ready_head_ = 0;
    uint32_t ready_len_ = 0;
};
