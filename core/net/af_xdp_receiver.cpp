#include "af_xdp_receiver.h"

#include "core/net/frame_util.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <linux/if_link.h>   // XDP_FLAGS_SKB_MODE
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

AF_XDPReceiver::AF_XDPReceiver(const std::string& ifname, uint16_t port, uint32_t queue_id)
    : ifname_(ifname), port_(port), queue_id_(queue_id) {}

bool AF_XDPReceiver::start()
{
    if (running_) return true;

    // ── UMEM：对齐分配，全部帧初始归还 fill ring ──
    const size_t umem_size = kFrameSize * kNumFrames;
    umem_buf_ = aligned_alloc(4096, umem_size);
    if (!umem_buf_) return false;
    std::memset(umem_buf_, 0, umem_size);

    struct xsk_umem_config ucfg{};
    ucfg.frame_size = kFrameSize;
    ucfg.frame_headroom = 0;
    ucfg.fill_size = kRingSize;
    ucfg.comp_size = kRingSize;

    if (xsk_umem__create(&umem_, umem_buf_, umem_size, &fill_, &comp_, &ucfg)) {
        free(umem_buf_); umem_buf_ = nullptr;
        return false;
    }

    // fill ring 预填所有帧地址（内核收帧时取用）
    for (size_t i = 0; i < kNumFrames; ++i) {
        uint32_t idx = 0;
        if (!xsk_ring_prod__reserve(&fill_, 1, &idx)) break;
        *xsk_ring_prod__fill_addr(&fill_, idx) = static_cast<uint64_t>(i * kFrameSize);
        xsk_ring_prod__submit(&fill_, 1);
    }

    // ── xsk socket：SKB 模式（generic XDP，veth/网卡通用），XDP_COPY 零依赖 ──
    struct xsk_socket_config cfg{};
    cfg.rx_size = kRingSize;
    cfg.tx_size = kRingSize;
    cfg.bind_flags = XDP_COPY;
    cfg.xdp_flags = XDP_FLAGS_SKB_MODE | XDP_FLAGS_UPDATE_IF_NOEXIST;

    if (xsk_socket__create(&xsk_, ifname_.c_str(), queue_id_, umem_, &rx_, &tx_, &cfg)) {
        xsk_umem__delete(umem_); umem_ = nullptr;
        free(umem_buf_); umem_buf_ = nullptr;
        return false;
    }

    // ── 唤醒 fd：stop() 写它打断阻塞 poll ──
    wake_fd_ = eventfd(0, EFD_NONBLOCK);

    running_ = true;
    return true;
}

void AF_XDPReceiver::stop()
{
    if (!running_) return;
    running_ = false;
    // 写 wake_fd 打断阻塞 poll（recv 返回 0）
    if (wake_fd_ >= 0) {
        uint64_t one = 1;
        ssize_t r = write(wake_fd_, &one, sizeof(one)); (void)r;
    }
    if (xsk_) {
        xsk_socket__delete(xsk_); xsk_ = nullptr;
    }
    if (umem_) {
        xsk_umem__delete(umem_); umem_ = nullptr;
    }
    if (umem_buf_) {
        free(umem_buf_); umem_buf_ = nullptr;
    }
    if (wake_fd_ >= 0) {
        close(wake_fd_); wake_fd_ = -1;
    }
}

void AF_XDPReceiver::set_blocking(bool blocking) { blocking_ = blocking; }

int AF_XDPReceiver::fd() const { return xsk_ ? xsk_socket__fd(xsk_) : -1; }

// 处理完的帧地址归还 fill ring（内核回收复用）
void AF_XDPReceiver::refill(uint64_t addr)
{
    uint32_t idx = 0;
    if (xsk_ring_prod__reserve(&fill_, 1, &idx)) {
        *xsk_ring_prod__fill_addr(&fill_, idx) = addr;
        xsk_ring_prod__submit(&fill_, 1);
    }
}

ssize_t AF_XDPReceiver::recv(uint8_t* buf, size_t len)
{
    if (!running_ || len == 0) return 0;

    for (;;) {
        // ── 有帧：剥帧头取载荷，非目标帧跳过继续收 ──
        uint32_t idx = 0;
        if (xsk_ring_cons__peek(&rx_, 1, &idx)) {
            const struct xdp_desc* desc = xsk_ring_cons__rx_desc(&rx_, idx);
            const uint8_t* frame = static_cast<uint8_t*>(umem_buf_) + desc->addr;
            size_t frame_len = desc->len;
            const uint8_t* payload = nullptr;
            size_t plen = extract_udp_payload(frame, frame_len, port_, &payload);
            // 归还 fill ring（无论目标与否，帧都释放复用）
            refill(desc->addr & ~(kFrameSize - 1));
            xsk_ring_cons__release(&rx_, 1);
            if (plen > 0) {
                size_t n = plen < len ? plen : len;
                std::memcpy(buf, payload, n);
                return static_cast<ssize_t>(n);   // 纯载荷(MoldUDP64), 与 io_uring 语义一致
            }
            continue;   // 非目标帧：跳过，继续收下一帧
        }

        // ── 阻塞模式：poll(xsk_fd + wake_fd) 等帧 / stop 打断 ──
        if (blocking_.load()) {
            struct pollfd pfds[2];
            pfds[0] = {xsk_socket__fd(xsk_), POLLIN, 0};
            pfds[1] = {wake_fd_, POLLIN, 0};
            int r = poll(pfds, 2, 100);
            if (r < 0 && errno == EINTR) continue;
            if (r < 0) return -1;
            if (pfds[1].revents & POLLIN) {
                if (!running_) return 0;   // stop() 打断
                uint64_t v; ssize_t rr = read(wake_fd_, &v, sizeof(v)); (void)rr;  // 消费
                continue;
            }
            continue;
        }

        return 0;   // 非阻塞模式：无帧立即返回 0
    }
}
