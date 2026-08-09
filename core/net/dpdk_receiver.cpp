#include "dpdk_receiver.h"

#include "core/net/frame_util.h"

#include <rte_config.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_version.h>

#include <algorithm>
#include <cstring>
#include <ctime>
#include <mutex>

namespace {

// EAL 只能初始化一次（进程全局）。多 DPDKReceiver 共享。
std::once_flag g_eal_once;
int g_eal_rc = -1;

// 收集所有 DPDKReceiver 的 EAL 参数，第一次 init 时合并传入。
std::mutex g_args_mu;
std::vector<std::string> g_eal_args;

}  // namespace

DPDKReceiver::DPDKReceiver(const std::string& vdev_spec, uint16_t port,
                           const std::vector<std::string>& eal_args)
    : vdev_spec_(vdev_spec), port_(port), eal_args_(eal_args)
{
    std::lock_guard<std::mutex> lk(g_args_mu);
    // 合并 EAL 参数（去重 vdev：一次 init 建多个 net_tap0 会重复报错）
    for (const auto& a : eal_args_)
        if (std::find(g_eal_args.begin(), g_eal_args.end(), a) == g_eal_args.end())
            g_eal_args.push_back(a);
    std::string vdev = "--vdev=" + vdev_spec_;
    if (std::find(g_eal_args.begin(), g_eal_args.end(), vdev) == g_eal_args.end())
        g_eal_args.push_back(vdev);
}

int DPDKReceiver::eal_init(const std::vector<std::string>& args)
{
    std::call_once(g_eal_once, [&] {
        // rte_eal_init 消费可写 argv
        std::vector<char*> argv;
        argv.push_back(const_cast<char*>("dpdk"));
        for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
        g_eal_rc = rte_eal_init(static_cast<int>(argv.size()), argv.data());
    });
    return g_eal_rc;
}

bool DPDKReceiver::start()
{
    if (running_) return true;

    std::lock_guard<std::mutex> lk(g_args_mu);
    std::vector<std::string> all = g_eal_args;
    eal_ready_ = (eal_init(all) >= 0);
    if (!eal_ready_) return false;

    // 按 vdev 名找端口
    if (rte_eth_dev_get_port_by_name(vdev_spec_.c_str(), &port_id_) < 0)
        return false;

    // 端口配置（单队列，收包）。默认 MTU 1500 内帧由 mbuf 承载，
    // 超 MTU 帧需 jumbo offload + 大 mbuf（正确性测试用 pack_max 控制帧 < MTU）。
    struct rte_eth_conf conf{};
    if (rte_eth_dev_configure(port_id_, 1, 1, &conf) < 0)
        return false;

    // mbuf 池
    mbuf_pool_ = rte_pktmbuf_pool_create("nx_dpdk_mbuf", kMbufPool, 64, 0,
                                         RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    if (!mbuf_pool_) return false;

    if (rte_eth_rx_queue_setup(port_id_, 0, kRxRing,
                               rte_eth_dev_socket_id(port_id_), nullptr, mbuf_pool_) < 0)
        return false;
    if (rte_eth_tx_queue_setup(port_id_, 0, kTxRing,
                               rte_eth_dev_socket_id(port_id_), nullptr) < 0)
        return false;

    if (rte_eth_dev_start(port_id_) < 0)
        return false;
    rte_eth_promiscuous_enable(port_id_);
    port_started_ = true;
    running_ = true;
    return true;
}

void DPDKReceiver::stop()
{
    if (!running_) return;
    running_ = false;
    if (port_started_) {
        rte_eth_dev_stop(port_id_);
        port_started_ = false;
    }
    if (mbuf_pool_) {
        rte_mempool_free(mbuf_pool_);
        mbuf_pool_ = nullptr;
    }
}

void DPDKReceiver::set_blocking(bool blocking) { blocking_ = blocking; }

int DPDKReceiver::fd() const { return -1; }   // DPDK 纯轮询，无 fd

ssize_t DPDKReceiver::recv(uint8_t* buf, size_t len)
{
    if (!running_ || len == 0) return 0;

    for (;;) {
        uint16_t n = rte_eth_rx_burst(port_id_, 0, mbufs_, 1);
        if (n > 0) {
            struct rte_mbuf* m = mbufs_[0];
            const uint8_t* frame = rte_pktmbuf_mtod(m, const uint8_t*);
            size_t frame_len = rte_pktmbuf_pkt_len(m);
            const uint8_t* payload = nullptr;
            size_t plen = extract_udp_payload(frame, frame_len, port_, &payload);
            rte_pktmbuf_free(m);
            if (plen > 0) {
                size_t c = plen < len ? plen : len;
                std::memcpy(buf, payload, c);
                return static_cast<ssize_t>(c);   // 纯载荷(MoldUDP64), 与 io_uring 语义一致
            }
            continue;   // 非目标帧：跳过，继续收下一帧
        }
        if (!blocking_.load()) return 0;   // 非阻塞：无帧立即返回
        if (!running_.load()) return 0;    // stop() 打断
        // 阻塞轮询：让出 CPU，避免空转烧核
        struct timespec ts{0, 100000};     // 100µs
        nanosleep(&ts, nullptr);
    }
}
