#include "io_uring_sender.h"

#include <cerrno>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

IoUringSender::IoUringSender(const std::string& host, uint16_t port)
    : host_(host), port_(port) {}

bool IoUringSender::start()
{
    if (running_) return true;

    fd_ = socket(AF_INET, SOCK_DGRAM, 0);
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

    running_ = true;
    return true;
}

void IoUringSender::stop()
{
    if (!running_) return;
    running_ = false;

    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    if (uring_ok_) {
        io_uring_unregister_buffers(&uring_);
        io_uring_queue_exit(&uring_);
        uring_ok_ = false;
    }
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
