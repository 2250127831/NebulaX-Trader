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

    struct io_uring ring_{};
    bool ok_ = false;

    // 固定缓冲区池
    char bufs_[MAX_BUFFERS][BUF_SIZE]{};
    struct iovec iovs_[MAX_BUFFERS]{};
    uint32_t buf_free_stack_[MAX_BUFFERS]{};
    uint32_t buf_free_top_ = 0;
};
