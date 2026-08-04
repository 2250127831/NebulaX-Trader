#include "io_uring_receiver.h"

#include "core/prof/lensx_probe.h"

#include <algorithm>
#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

IoUringReceiver::IoUringReceiver(uint16_t port) : port_(port) {}

bool IoUringReceiver::start()
{
    if (running_) return true;

    fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) return false;

    // 增大内核 UDP 接收缓冲, 吞下突发(默认 ~208KB, 全速/限速窗口突发易溢出丢包)。
    // 内核实际会翻倍到 rmem_max, 这里设 4MB 目标。
    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    if (bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd_);
        fd_ = -1;
        return false;
    }

    if (!poller_.init()) {
        close(fd_);
        fd_ = -1;
        return false;
    }

    buf_idx_ = poller_.alloc_buffer();
    if (buf_idx_ == UINT32_MAX) {
        close(fd_);
        fd_ = -1;
        return false;
    }

    running_ = true;
    return true;
}

void IoUringReceiver::stop()
{
    if (!running_) return;
    running_ = false;

    // 在途 recv 由 50ms timeout 唤醒后检查 running_ 返回 0
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    poller_.free_buffer(buf_idx_);
    buf_idx_ = UINT32_MAX;
}

void IoUringReceiver::set_blocking(bool blocking)
{
    blocking_ = blocking;
}

int IoUringReceiver::fd() const { return fd_; }

ssize_t IoUringReceiver::recv(uint8_t* buf, size_t len)
{
    if (!running_ || len == 0) return 0;

    // ── 非阻塞模式：提交后 peek，无完成立即返回 0 ──
    if (!blocking_.load()) {
        if (!recv_pending_) {
            // 必须显式 submit，否则 SQE 不进内核，CQE 永不产生
            if (!poller_.submit_recv_now(fd_, buf_idx_)) return -1;
            recv_pending_ = true;
        }
        if (!poller_.has_cqe()) return 0;  // 数据未到，recv 保持在途

        int result = 0;
        poller_.process_cqes([&result](int, int res) { result = res; });
        recv_pending_ = false;
        if (result >= 0) {
            size_t n = std::min<size_t>(static_cast<size_t>(result), len);
            std::memcpy(buf, poller_.buffer_ptr(buf_idx_), n);
            lensx::mark_recv_pkt();   // [LensX 包级] recv 返回, 数据已进用户 buf
            return static_cast<ssize_t>(n);
        }
        if (result == -EAGAIN) return 0;  // 未就绪，下轮重提交
        errno = -result;
        return -1;
    }

    // ── 阻塞模式：提交后等待，stop() 可打断 ──
    if (!recv_pending_) {
        if (!poller_.submit_recv(fd_, buf_idx_)) return -1;
        recv_pending_ = true;
    }

    for (;;) {
        int ret = poller_.submit_and_wait_timeout(50);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        }

        int result = 0;
        bool got = false;
        poller_.process_cqes([&got, &result](int, int res) {
            got = true;
            result = res;
        });

        if (!got) {             // 等到的只有 timeout CQE
            if (!running_) return 0;  // stop() 打断
            continue;                 // recv SQE 仍在 ring，继续等
        }

        recv_pending_ = false;
        if (result >= 0) {
            size_t n = std::min<size_t>(static_cast<size_t>(result), len);
            std::memcpy(buf, poller_.buffer_ptr(buf_idx_), n);
            lensx::mark_recv_pkt();   // [LensX 包级] recv 返回, 数据已进用户 buf
            return static_cast<ssize_t>(n);
        }
        if (!running_) return 0;        // stop() 已调用，通道关闭优先返回 0
        if (result == -EAGAIN) continue;  // poll 未就绪，重新提交并继续
        errno = -result;
        return -1;
    }
}
