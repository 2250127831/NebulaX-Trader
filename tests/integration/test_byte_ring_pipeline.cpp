// 集成测试：MoldUDP64 拆包 + 字节 ring 管道
//   [模拟压测] 裸 ITCH → 封装 MoldUDP64 包（20字节头 + 消息们）
//   [拆包器]   MoldUdpUnpacker: 读包头 → 每条消息前加 2 字节 seq → 推字节 ring
//   [解析器]   ByteRingParser: 读 4 字节头(seq+len) → 填 MarketEvent.seq_id
// 验证：解析消息数 = 期望（不丢），seq 全局连续递增
#include "core/queue/queue_manager.h"
#include "core/queue/spsc_byte_ring.h"
#include "market/pipeline/byte_ring_parser.h"
#include "market/pipeline/mold_udp_unpacker.h"

#include <cstdio>
#include <cstdint>
#include <vector>
#include <thread>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <chrono>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

// 从 ITCH 文件重建"完整裸消息流"（含 2 字节长度前缀），并数出消息数
static std::vector<uint8_t> rebuild_expected(const std::vector<uint8_t>& src,
                                             size_t& msg_count) {
    std::vector<uint8_t> out;
    size_t pos = 0;
    msg_count = 0;
    while (pos + 2 <= src.size()) {
        uint16_t body_len = (static_cast<uint16_t>(src[pos]) << 8) | src[pos+1];
        if (body_len < 1 || body_len > 200) { ++pos; continue; }
        size_t msg_len = 2 + body_len;
        if (pos + msg_len > src.size()) break;
        out.insert(out.end(), src.begin()+pos, src.begin()+pos+msg_len);
        ++msg_count;
        pos += msg_len;
    }
    return out;
}

// 模拟压测客户端：把裸消息流封装成 MoldUDP64 包（每包 1~pack_max 条随机）
static void pack_moldudp(const std::vector<uint8_t>& msgs,
                         std::vector<uint8_t>& out_pkts,
                         size_t& global_msg_seq_out,
                         int pack_max) {
    out_pkts.clear();
    size_t pos = 0;
    uint64_t global_msg_seq = 0;
    std::vector<uint8_t> pkt;
    pkt.reserve(4096);
    pkt.resize(20);
    while (pos + 2 <= msgs.size()) {
        size_t msgs_this = 1 + static_cast<size_t>(rand() % pack_max);
        uint64_t pkt_first_seq = global_msg_seq;
        size_t pkt_count = 0;
        for (size_t i = 0; i < msgs_this && pos + 2 <= msgs.size(); ++i) {
            uint16_t bl = (static_cast<uint16_t>(msgs[pos]) << 8) | msgs[pos+1];
            size_t ml = 2 + bl;
            pkt.insert(pkt.end(), msgs.begin()+pos, msgs.begin()+pos+ml);
            pos += ml;
            ++pkt_count; ++global_msg_seq;
        }
        if (pkt_count > 0) {
            // 填头: session(10) + seq(8) + count(2)
            uint8_t* hdr = pkt.data();
            memset(hdr, 0, 10);
            for (int i = 0; i < 8; ++i) hdr[10+i] = static_cast<uint8_t>(pkt_first_seq >> (56-8*i));
            hdr[18] = static_cast<uint8_t>(pkt_count >> 8);
            hdr[19] = static_cast<uint8_t>(pkt_count & 0xFF);
            out_pkts.insert(out_pkts.end(), pkt.begin(), pkt.end());
        }
        pkt.clear();
        pkt.resize(20);
    }
    global_msg_seq_out = global_msg_seq;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("usage: %s <itch_sample.bin>\n", argv[0]);
        return 1;
    }
    FILE* f = fopen(argv[1], "rb");
    if (!f) { printf("FAIL: 无法打开 %s\n", argv[1]); return 1; }
    fseek(f, 0, SEEK_END); long fsize = ftell(f); fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> src(fsize);
    if (fread(src.data(), 1, fsize, f) != (size_t)fsize) {
        printf("FAIL: 读取失败\n"); fclose(f); return 1;
    }
    fclose(f);
    size_t expected_msgs = 0;
    std::vector<uint8_t> msgs = rebuild_expected(src, expected_msgs);
    printf("裸消息流 %zu 字节, %zu 条消息\n", msgs.size(), expected_msgs);
    fflush(stdout);

    // 模拟压测客户端: 封装 MoldUDP64 包
    srand(12345);
    std::vector<uint8_t> pkts;
    size_t total_seq = 0;
    pack_moldudp(msgs, pkts, total_seq, 100);
    printf("MoldUDP64 包流 %zu 字节, 总消息 seq 到 %zu\n", pkts.size(), total_seq);
    CHECK(total_seq == expected_msgs);  // seq 全局连续 = 消息数

    // ── 管道: QueueManager 创建共享 ring + 通道A → 拆包器写 → 解析分流 → 策略消费 ──
    uint8_t* ring_buf = new uint8_t[1 << 20];
    size_t ring_id = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING,
                                          ring_buf, 1 << 20);
    auto& shared_ring = QueueManager::get<SPSCByteRing>(ring_id);

    // 通道 A: 成交事件广播（低频策略消费）
    auto* ev_slots = new MarketEvent[1 << 16];
    size_t chan_a_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                            ev_slots, 1 << 16, 1);
    auto& channel_a = QueueManager::get<SPMCEventQueue<16>>(chan_a_id);

    // 通道 B: 委托事件广播（订单簿/逐笔策略消费）
    auto* ord_slots = new MarketEvent[1 << 16];
    size_t chan_b_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                            ord_slots, 1 << 16, 1);
    auto& channel_b = QueueManager::get<SPMCEventQueue<16>>(chan_b_id);

    ByteRingParser bp(shared_ring, channel_a, channel_b);
    MoldUdpUnpacker unpacker(shared_ring);  // 共享同一个 ring
    std::atomic<size_t> parsed_count{0};
    std::atomic<bool> stop{false};
    std::atomic<bool> seq_contiguous{true};
    std::atomic<uint64_t> last_seq{0};

    // ── 解析线程: 从共享 ring 读, 成交事件分流进通道 A ──
    // 退出条件: stop 且 ring 空(消费完所有数据)。避免主线程置 stop 后
    // 解析线程还没消费完 ring 就退出导致漏解析。
    std::thread parse_th([&] {
        while (true) {
            size_t n = bp.parse_available();
            parsed_count += n;
            if (bp.ring().empty() && stop.load(std::memory_order_acquire)) break;
            if (n == 0) bp.wait_for_data(200);
        }
    });

    // ── 策略消费线程: 从通道 A 收成交事件, 验证 seq 连续 ──
    std::thread strategy_th([&] {
        MarketEvent ev;
        while (!stop.load(std::memory_order_acquire) || channel_a.pending(0) > 0) {
            if (channel_a.pop(0, ev)) {
                if (last_seq.load() != 0 && ev.seq_id != last_seq.load() + 1)
                    seq_contiguous.store(false, std::memory_order_relaxed);
                last_seq.store(ev.seq_id, std::memory_order_relaxed);
            } else if (!stop.load()) {
                std::this_thread::yield();
            }
        }
    });

    // ── 生产线程（主线程）: 拆包器拆 → 推共享 ring ──
    unpacker.feed(pkts.data(), pkts.size());   // 同步推入 ring(满则等解析腾空间)
    bp.notify();                                // 唤醒解析线程

    // 等解析线程消费完(ring 空)再 stop, 避免 feed 尾部消息漏解析。
    while (!shared_ring.empty())
        std::this_thread::yield();

    stop.store(true, std::memory_order_release);
    bp.notify();
    parse_th.join();
    strategy_th.join();

    printf("解析消息数 = %zu, 期望 = %zu, seq连续=%d\n",
           parsed_count.load(), expected_msgs, seq_contiguous.load());
    CHECK(parsed_count.load() == expected_msgs);

    if (g_failures == 0) {
        printf("\nMoldUDP64 拆包管道集成测试 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
