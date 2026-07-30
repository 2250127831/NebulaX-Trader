#pragma once

#include "core/utils/align.h"
#include <cstdint>
#include <atomic>

// 压测流量控制 + 看门狗：共享内存收发计数
//
// 生命周期：
//   1. NX-Trader 启动 → shm_open + mmap → ready = true
//   2. 外部看门狗轮询到 ready → 准备就绪
//   3. Replayer 轮询到 ready → 开始发包
//   4. NX-Trader 主循环每轮 → heartbeat++
//   5. 看门狗监测 heartbeat，连续 N 秒不动 → 进程崩溃，自动重启
//   6. Replayer 每发一条 UDP → sent++
//   7. NX-Trader 每收一条 UDP → received++
//   8. Replayer 定期检查 sent - received 积压量，自动调速

struct FlowControl {
    CACHE_ALIGN std::atomic<bool>     ready{false};
    CACHE_ALIGN std::atomic<uint64_t> heartbeat{0};
    CACHE_ALIGN std::atomic<uint64_t> sent{0};
    CACHE_ALIGN std::atomic<uint64_t> received{0};
};

static constexpr const char* FLOW_SHM_PATH = "/nx_trader_flow";
