#include "io_uring_sender.h"

#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

IoUringSender::IoUringSender(const std::string& host, uint16_t port,
                             uint8_t* ring_buf, size_t ring_capacity)
    : host_(host), port_(port), ring_(ring_buf, ring_capacity) {}

bool IoUringSender::start()
{
    if (running_) return true;

    // V5: TCP(SOCK_STREAM), 同一连接全双工(订单发 + 回报收)。
    // connect 做 3 次握手。调用方须先确保对端(模拟交易所/撮合引擎)已 listen,
    // 否则 connect 立即失败返回 false(测试/生产都在对端就绪后才 start)。
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
        close(fd_);
        fd_ = -1;
        return false;
    }
    if (connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd_);
        fd_ = -1;
        return false;
    }

    // 迁移自 init_send_uring：把 ring 原始 buffer 注册为固定缓冲区
    if (io_uring_queue_init(256, &uring_, 0) < 0) {
        close(fd_);
        fd_ = -1;
        return false;
    }
    struct iovec iov = ring_.raw_iovec();
    if (io_uring_register_buffers(&uring_, &iov, 1) < 0) {
        io_uring_queue_exit(&uring_);
        close(fd_);
        fd_ = -1;
        return false;
    }
    uring_ok_ = true;

    // 接收侧 ring(回报读, 与发送 ring 分离, 同一 fd 共存)
    if (!recv_poller_.init()) {
        io_uring_unregister_buffers(&uring_);
        io_uring_queue_exit(&uring_);
        uring_ok_ = false;
        close(fd_);
        fd_ = -1;
        return false;
    }
    recv_buf_idx_ = recv_poller_.alloc_buffer();
    if (recv_buf_idx_ == UINT32_MAX) {
        io_uring_unregister_buffers(&uring_);
        io_uring_queue_exit(&uring_);
        uring_ok_ = false;
        close(fd_);
        fd_ = -1;
        return false;
    }

    running_ = true;
    return true;
}

void IoUringSender::stop()
{
    if (!running_) return;
    running_ = false;

    // 在途 recv 由 50ms timeout 唤醒后检查 running_ 返回 0
    if (recv_buf_idx_ != UINT32_MAX) {
        recv_poller_.free_buffer(recv_buf_idx_);
        recv_buf_idx_ = UINT32_MAX;
    }
    if (fd_ >= 0) {
        close(fd_);   // 打断阻塞 recv + 未决 send
        fd_ = -1;
    }
    if (uring_ok_) {
        io_uring_unregister_buffers(&uring_);
        io_uring_queue_exit(&uring_);
        uring_ok_ = false;
    }
    // recv_poller_ 析构时自动清理其 ring
}

void IoUringSender::set_blocking(bool blocking)
{
    blocking_ = blocking;
}

int IoUringSender::fd() const { return fd_; }

ssize_t IoUringSender::send(const uint8_t* buf, size_t len)
{
    if (!running_ || len == 0) return 0;
    // 注意：假设 len 远小于 ring 容量（8MB），信号消息仅几十字节。
    // 若 len 接近 RING_SIZE，push 填满 ring 而 send_zc_all 未被调用会死锁。

    // 1. 拷入内部 ring（阻塞等待空间，可被 stop 打断）
    size_t pushed = 0;
    while (pushed < len) {
        size_t n = ring_.push(buf + pushed, len - pushed);
        if (n > 0) { pushed += n; continue; }
        if (!running_) return 0;  // stop() 打断
        __builtin_ia32_pause();
    }

    // 2. 从 ring 零拷贝发送
    return send_zc_all(len);
}

ssize_t IoUringSender::send_zc_all(size_t len)
{
    size_t remaining = len;
    while (remaining > 0) {
        const void* ptr;
        size_t chunk = ring_.read_acquire(ptr, remaining);
        if (chunk == 0) {
            if (!running_) return 0;
            __builtin_ia32_pause();
            continue;
        }

        struct io_uring_sqe* sqe = io_uring_get_sqe(&uring_);
        if (!sqe) { errno = EAGAIN; return -1; }
        io_uring_prep_send_zc_fixed(sqe, fd_, ptr, chunk, MSG_NOSIGNAL, 0, 0);

        ssize_t sent = 0;
        bool got = false;
        while (!got) {
            // 先提交 SQE，再带超时等 CQE（超时后可检查 stop 标志）
            int ret = io_uring_submit(&uring_);
            if (ret < 0) {
                errno = -ret;
                return -1;
            }
            struct io_uring_cqe* cqe;
            struct __kernel_timespec ts;
            ts.tv_sec  = 0;
            ts.tv_nsec = 10 * 1000000;  // 10ms 超时，可被 stop 打断
            ret = io_uring_wait_cqe_timeout(&uring_, &cqe, &ts);
            if (ret < 0) {
                if (ret == -EINTR || ret == -ETIME) {
                    if (!running_) return 0;  // stop() 打断
                    continue;
                }
                errno = -ret;
                return -1;
            }
            sent = cqe->res;
            io_uring_cqe_seen(&uring_, cqe);
            got = true;
        }

        if (sent > 0) {
            ring_.read_release(static_cast<size_t>(sent));
            remaining -= static_cast<size_t>(sent);
        } else {
            // 发送失败：丢弃剩余数据，避免与 ring 状态不一致
            ring_.read_release(remaining);
            errno = (sent == 0) ? EIO : static_cast<int>(-sent);
            return -1;
        }
    }
    return static_cast<ssize_t>(len);
}

// V5 TCP 全双工: 从同一连接读回报(复制 IoUringReceiver::recv 阻塞模式)。
ssize_t IoUringSender::recv(uint8_t* buf, size_t len)
{
    if (!running_ || len == 0) return 0;

    // ── 非阻塞模式：提交后 peek，无完成立即返回 0 ──
    if (!blocking_.load()) {
        if (!recv_pending_) {
            if (!recv_poller_.submit_recv_now(fd_, recv_buf_idx_)) return -1;
            recv_pending_ = true;
        }
        if (!recv_poller_.has_cqe()) return 0;
        int result = 0;
        recv_poller_.process_cqes([&result](int, int res) { result = res; });
        recv_pending_ = false;
        if (result >= 0) {
            size_t n = std::min<size_t>(static_cast<size_t>(result), len);
            std::memcpy(buf, recv_poller_.buffer_ptr(recv_buf_idx_), n);
            return static_cast<ssize_t>(n);
        }
        if (result == -EAGAIN) return 0;
        errno = -result;
        return -1;
    }

    // ── 阻塞模式：提交后等待，stop() 可打断 ──
    if (!recv_pending_) {
        if (!recv_poller_.submit_recv(fd_, recv_buf_idx_)) return -1;
        recv_pending_ = true;
    }
    for (;;) {
        int ret = recv_poller_.submit_and_wait_timeout(50);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        int result = 0;
        bool got = false;
        recv_poller_.process_cqes([&got, &result](int, int res) {
            got = true;
            result = res;
        });
        if (!got) {             // 等到的只有 timeout CQE
            if (!running_) return 0;   // stop() 打断
            continue;
        }
        recv_pending_ = false;
        if (result >= 0) {
            size_t n = std::min<size_t>(static_cast<size_t>(result), len);
            std::memcpy(buf, recv_poller_.buffer_ptr(recv_buf_idx_), n);
            return static_cast<ssize_t>(n);
        }
        if (!running_) return 0;        // stop() 已调用
        if (result == -EAGAIN) continue;
        errno = -result;
        return -1;
    }
}
