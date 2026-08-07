// NebulaX-Trader — Low Latency Trading Infrastructure
// 主程序：配置加载 → 行情回放接收 → 双通道 → 策略 + 主从组合 → 下单 → 模拟交易所回报
//
// 用法:
//   ./trader                        # 用 config/default.yaml
//   ./trader --config config/xxx.yaml
//
// 链路:
//   [接收线程]  IoUringReceiver ← MoldUDP64 行情
//     → unpacker(拆包加seq) → 字节ring
//   [解析线程]  ByteRingParser → 通道A(成交) / 通道B(委托)
//   [策略线程]  通道A → 4成交策略 + K线聚合 → 主策略
//   [订单簿线程] 通道B → OrderBookConsumer 重建盘口 → OBI/OFI 从策略
//   [主策略+从策略] → SignalCombiner(主从分层) → 实时组合决策
//     → ExecutionEngine(风控+OMS) → IoUringSender → 模拟交易所 → 成交回报
//   [回报线程]  IoUringReceiver ← FILL → OMS/Risk 更新

#include "core/config.h"
#include "core/ipc/flow_control.h"
#include "core/net/io_uring_receiver.h"
#include "core/net/io_uring_sender.h"
#include "core/prof/lensx_probe.h"

#include <immintrin.h>   // _mm_pause（高频消费者忙轮询暂停）
#include "core/queue/queue_manager.h"
#include "core/queue/spmc_event_queue.h"
#include "core/queue/spsc_byte_ring.h"
#include "execution/execution_engine.h"
#include "market/book/order_book_consumer.h"
#include "market/pipeline/byte_ring_parser.h"
#include "market/pipeline/mold_udp_unpacker.h"
#include "oms/order_manager.h"
#include "oms/order_protocol.h"
#include "risk/risk_manager.h"
#include "strategy/base/strategy.h"
#include "strategy/tick/order_book_imbalance_strategy.h"
#include "strategy/tick/order_flow_imbalance_strategy.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

// ── 线程绑核 ──
// V2.1 分簿并行的前提: N 个 worker + recv/parse 各占独立 P 核(避免内核调度器把
// 并行线程随机撒在 24 核上互相抢核/和 parse 抢核, 污染延迟)。benchmark 绑 CPU3(低
// 中断核), trader 线程避开 CPU0/1(P0 噪声)与 CPU3。
// i9-12900HX: P 核 0-15(SMT 成对: 0/1, 2/3, 4/5...), E 核 16-23。热线程用奇数
// P 核(5/7/9/11/13/15 = 6 个不同物理核的独立 SMT), 避免两热线程共享物理核。
static void pin_cpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}
// i9-12900HX: 8 大核(P, SMT成对 0-15) + 8 小核(E, 独立 16-23)。
// 绑核原则(V2 最终版, 实测驱动):
//   recv=5         大核高吞吐收包(不抢)
//   解析器         E 16 起(小核, 协作流水线大核优势被拉平, 全小核独占)
//   worker         P 11/10 + 13/12(2 worker 共享一个物理 P 核的 SMT 兄弟;
//                  订单簿操作轻重不均, E 核算力不足致排队, P 核够)
//   fill/主线程    共享大核 9(低频, 两线程不互争, 不占热核)
static const int kPinRecv    = 5;   // io_uring 收包(高吞吐, 大核)
static const int kPinParseE0 = 16;  // 解析器小核起点(E 16-19, nparsers 个)
static const int kPinIdle    = 9;   // 低频共享核(fill + 主线程, 大核空闲)

static void usage(const char* prog) {
    printf("Usage: %s [--config <path>] [--no-shm]\n"
           "  --no-shm   不挂共享内存(压测脚本模式): 无握手, 运行 idle_timeout_sec 后退出\n",
           prog);
}

// ══ V2.1 分簿并行：单 book_th → N 个 book_worker（广播 + skip）══
// 每 worker 一套订单簿/策略/信号槽 + 独立仲裁(决策5)，共享无锁 OrderPool/OrderMap
// 与 ExecutionEngine(锁)。归属用全局注册表(locate → owner)，registry 即关心判定。

// ── 信号槽: 各策略线程写, arbitrate() 读(跨线程原子) ──
// 每个信号拆成 side/locate/strength/seq 原子, 避免整 Signal 非原子的竞争。
// seq: 触发信号更新的成交 seq(unpacker 分配的唯一 seq), arbitrate 用它做 key 配对。
struct SignalSlot {
    alignas(64) std::atomic<OrderSide> side{OrderSide::NONE};
    alignas(64) std::atomic<uint64_t> locate{0};
    alignas(64) std::atomic<int64_t> strength{0};
    alignas(64) std::atomic<uint64_t> seq{0};   // 成交 seq(贯穿线索)
};

// ── 全局注册表: locate → owner worker ──
// locate 是 ITCH 16-bit(0-65535) → 固定数组索引(比哈希还快)。registry 即关心判定,
// 不需要 care_list: 每 worker pop 到事件查 owner, 不是自己就 skip。
// 动态均衡: 新 locate 首次出现时由"先遇到它的 worker"代为注册到当前 cared_count 最小者
// (最清闲), 之后固定 → 保序(一个 locate 只归一个 worker)。
struct BookRegistry {
    static constexpr uint32_t kNone = UINT32_MAX;
    std::atomic<uint32_t> owner_[65536];   // locate → owner worker id

    BookRegistry() {
        for (auto& a : owner_) a.store(kNone, std::memory_order_relaxed);
    }

    uint32_t lookup_or_register(uint32_t locate, uint32_t n,
                                const std::atomic<uint64_t>* cared,
                                std::atomic<uint64_t>* registered) {
        uint32_t cur = owner_[locate].load(std::memory_order_acquire);
        if (cur != kNone) return cur;
        // 未注册: 选 (cared_count, registered_count) 字典序最小的 worker。
        //   cared_count    = 处理事件数(主键, 运行期让新 locate 去最清闲者)
        //   registered_count = 已注册给它的 locate 数(次键, 破启动期全 0 平局)
        // 双键从 0 开始遍历: 启动期 cared 全 0, registered 先注册的涨 → 严格轮流
        // 分散(即使同一 worker 先遇到所有新 locate 也均匀), 避免"平局归自己"的
        // 竞速正反馈(先调度者连续抢注册 → 单一 straggler 拖垮 SPMC)。
        uint32_t target = 0;
        uint64_t tc = cared[0].load(std::memory_order_relaxed);
        uint64_t tr = registered[0].load(std::memory_order_relaxed);
        for (uint32_t i = 1; i < n; ++i) {
            uint64_t c = cared[i].load(std::memory_order_relaxed);
            uint64_t r = registered[i].load(std::memory_order_relaxed);
            if (c < tc || (c == tc && r < tr)) { target = i; tc = c; tr = r; }
        }
        uint32_t expected = kNone;
        if (owner_[locate].compare_exchange_strong(expected, target,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
            registered[target].fetch_add(1, std::memory_order_relaxed);
            return target;
        }
        return expected;   // 别人注册了(先到先得)
    }
};

// 下单节奏(V1.5 定稿): 方向翻转 → 必下; 方向不变 → 强度相对上次下单跳变 ≥ 阈值才再下。
static constexpr int64_t kStrengthStep = 500;   // 千分比定点(500 = 5% 满强度)

// ── 单 book_worker: 自己的簿/策略/信号槽 + 独立仲裁 ──
struct BookWorker {
    OrderBookConsumer obc;                    // 自己关心的标的簿(共享池/索引)
    OrderBookImbalanceStrategy obi;
    OrderFlowImbalanceStrategy ofi;
    SignalSlot sig_ofi, sig_obi;              // 自己标的的信号槽
    std::atomic<OrderSide> last_order_side{OrderSide::NONE};
    std::atomic<int64_t> last_order_str{-1};
    size_t arb_sample_cnt = 0;                // 本线程私有, 非原子安全

    // 运行上下文(init 绑定)
    ExecutionEngine* ex = nullptr;
    RiskManager* rm = nullptr;
    const Config* cfg = nullptr;
    std::atomic<size_t>* trade_count = nullptr;
    std::atomic<size_t>* book_events = nullptr;

    explicit BookWorker(OrderPool& pool, OrderMap& idx) : obc(pool, idx) {}

    void init(ExecutionEngine* e, RiskManager* r, const Config* c,
              std::atomic<size_t>* tc, std::atomic<size_t>* be) {
        ex = e; rm = r; cfg = c; trade_count = tc; book_events = be;
    }

    // 统一仲裁: 读本 worker 两信号槽, 同向才下。独立仲裁 → 时序天然正确(决策5)。
    // 下单节奏: 方向翻转必下; 方向不变仅强度跳变 ≥ 阈值才下(last_order_str 是
    // 上次下单时强度, 保证"强→弱"不回补)。
    void arbitrate() {
        // [LensX 级别3] 仲裁函数起点/终点: 抽中才打(同一次调用内局部变量保证成对)。
        bool arb_sample = (arb_sample_cnt++ % lensx::kSample == 0);
        if (arb_sample) lensx::mark_arb_start();
        OrderSide so = sig_ofi.side.load(std::memory_order_acquire);
        OrderSide sb = sig_obi.side.load(std::memory_order_acquire);
        // 仲裁: 两信号同向才下(OFI/OBI)。V1.5 删 TD 后仲裁即两信号。
        bool ok = (so != OrderSide::NONE && so == sb);
        if (ok) {
            // 用 OFI 方向(方向一致时任一方向都成立, 取 OFI)
            uint64_t locate = sig_ofi.locate.load(std::memory_order_acquire);
            int64_t strength = sig_ofi.strength.load(std::memory_order_acquire);
            uint64_t pos = rm->position(locate);
            bool blocked = (so == OrderSide::BUY && pos >= cfg->risk.max_position) ||
                           (so == OrderSide::SELL && pos == 0);
            // 下单触发(强度阈值触发): 方向翻转必下; 方向不变仅强度跳变 ≥ 阈值才下。
            int64_t last_str = last_order_str.load();
            bool fresh_dir  = (so != last_order_side.load());
            bool strength_ge = (strength >= last_str + kStrengthStep);
            if (!blocked && (fresh_dir || strength_ge)) {
                // [LensX 级别4] 下单决策→执行完毕(key=sig_ofi.seq 信号触发seq, 抽样)。
                // 抽样判断在调用点(抽中才调), 避免 uprobe 每次命中拖垮吞吐。
                uint64_t order_seq = sig_ofi.seq.load(std::memory_order_acquire);
                if (order_seq % lensx::kSample == 0) lensx::mark_order_start(order_seq);
                Signal decision{.side = so, .locate = locate,
                                .price = 0, .timestamp = 0, .strength = strength};
                uint64_t oid = ex->submit_signal(decision, 1);
                if (oid != 0) {
                    last_order_side.store(so);
                    last_order_str.store(strength);
                }
                if (order_seq % lensx::kSample == 0) lensx::mark_order_end(order_seq);
            }
        }
        if (arb_sample) lensx::mark_arb_end();
    }

    // 处理一个归属本 worker 的事件: 重建簿 → OFI/OBI 信号 → 仲裁下单。
    void process(const MarketEvent& ev) {
        // [LensX 消息级] 处理起点。每条消息仅 owner worker 打一次
        // (抽样配对 alloc→process 1:1, 勿在每个 worker 的 pop 处都打)。
        if (ev.seq_id % lensx::kSample == 0) lensx::mark_process(ev.seq_id);
        bool is_trade = (ev.type == MarketEvent::Type::TRADE ||
                         ev.type == MarketEvent::Type::EXECUTE);
        if (is_trade) trade_count->fetch_add(1, std::memory_order_relaxed);
        obc.on_event(ev);
        book_events->fetch_add(1, std::memory_order_relaxed);
        const OrderBook* book = obc.book(ev.locate);
        // 方向：A/U 自带 side；D/X/E 查簿
        OrderSide side = OrderSide::NONE;
        if (ev.type == MarketEvent::Type::ADD ||
            ev.type == MarketEvent::Type::REPLACE) {
            side = ev.order.side;
        } else if (book) {
            if (ev.type == MarketEvent::Type::TRADE ||
                ev.type == MarketEvent::Type::EXECUTE)
                side = book->side_of(ev.trade.order_ref);
            else
                side = book->side_of(ev.order.order_ref);
        }
        if (side != OrderSide::NONE && cfg->strategy.use_ofi) {
            ofi.on_event(ev, side);
            if (book && book->best_bid() >= 0 && book->best_ask() >= 0)
                ofi.set_last_price((book->best_bid() + book->best_ask()) / 2);
        }
        if (book && cfg->strategy.use_obi) {
            obi.on_book(ev.locate, book->best_bid(), book->best_bid_volume(),
                        book->best_ask(), book->best_ask_volume(), ev.timestamp);
        }

        // 更新信号槽(arbitrate 读): OFI/OBI 信号 → 原子槽, 带触发 seq
        if (cfg->strategy.use_ofi) {
            Signal s = ofi.signal();
            sig_ofi.side.store(s.side, std::memory_order_release);
            sig_ofi.locate.store(s.locate, std::memory_order_release);
            sig_ofi.strength.store(s.strength, std::memory_order_release);
            sig_ofi.seq.store(ev.seq_id, std::memory_order_release);
        }
        if (cfg->strategy.use_obi) {
            Signal s = obi.signal();
            sig_obi.side.store(s.side, std::memory_order_release);
            sig_obi.locate.store(s.locate, std::memory_order_release);
            sig_obi.strength.store(s.strength, std::memory_order_release);
            sig_obi.seq.store(ev.seq_id, std::memory_order_release);
        }
        arbitrate();   // 写完 OFI/OBI 信号, 检查是否齐 → 仲裁下单
    }
};

int main(int argc, char* argv[]) {
    pin_cpu(kPinIdle);   // 主线程低频(配置/汇总), 与 fill 共享大核 9
    // ── 解析参数 + 加载配置 ──
    std::string config_path = "config/default.yaml";
    bool no_shm = false;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc) config_path = argv[++i];
        else if (strcmp(argv[i], "--no-shm") == 0) no_shm = true;
        else { usage(argv[0]); return 1; }
    }
    Config cfg;
    try {
        cfg = ConfigLoader::load(config_path);
    } catch (const std::exception& e) {
        printf("配置加载失败: %s\n", e.what());
        return 1;
    }

    printf("NebulaX-Trader v0.1.0\n");
    printf("  行情端口: %u  模拟交易所收单: %u  回报: %u  主策略: %s\n",
           cfg.market.port, cfg.execution.order_port, cfg.execution.order_ret_port,
           cfg.strategy.primary.c_str());

    // ── 清理共享内存残留（避免复用旧计数器）──
    if (!no_shm) shm_unlink(FLOW_SHM_PATH);

    // ── 行情接收端 ──
    auto receiver = std::make_unique<IoUringReceiver>(cfg.market.port);
    if (!receiver->start()) { printf("接收端启动失败\n"); return 1; }

    // ── 共享内存 FlowControl（--no-shm 时不挂，压测脚本模式无握手）──
    FlowControl* fc = nullptr;
    if (!no_shm) {
        int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
        if (shm_fd < 0) { perror("shm_open"); return 1; }
        if (ftruncate(shm_fd, sizeof(FlowControl)) < 0) { perror("ftruncate"); return 1; }
        fc = static_cast<FlowControl*>(mmap(nullptr, sizeof(FlowControl),
            PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
        close(shm_fd);
        if (fc == MAP_FAILED) { perror("mmap"); return 1; }
        fc->ready.store(false, std::memory_order_release);
        fc->sent.store(0, std::memory_order_release);
        fc->received.store(0, std::memory_order_release);
        fc->order_ret_port.store(cfg.execution.order_ret_port, std::memory_order_release);
    }

    // ── 双通道 + 共享 ring ──
    // V2.3: 字节 ring 从 SPSC 换 SPMC(单生产 recv_th + 多消费者 N 个解析器)。
    auto ring_buf = std::make_unique<uint8_t[]>(cfg.market.ring_bytes);
    size_t ring_id = QueueManager::create(QueueManager::Type::SPMC_BYTE_RING,
                                          ring_buf.get(), cfg.market.ring_bytes);
    auto& shared_ring = QueueManager::get<SPMCByteRing>(ring_id);

    // 单通道: 全部事件(成交+委托)进同一队列, SPMC 广播(N 个 book_worker 分簿并行)。
    // 同一序列 + 一个 locate 只归一个 worker → 订单簿时序正确(成交不先于对应委托)。
    const size_t nworkers = std::max<size_t>(1, cfg.order_book.workers);
    auto* chan_slots = new MarketEvent[cfg.market.chan_slots];
    size_t chan_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                          chan_slots, cfg.market.chan_slots, nworkers);
    auto& channel = QueueManager::get<SPMCEventQueue<16>>(chan_id);

    MoldUdpUnpacker unpacker(shared_ring);
    // V2.3: N 个解析器, 每个一个 ByteRingParser(共享 ring + channel)。
    // 含引用成员不可移动/拷贝 → unique_ptr 规避 vector 重分配。
    const size_t nparsers = std::max<size_t>(1, cfg.market.parse_workers);
    std::vector<std::unique_ptr<ByteRingParser>> parsers;
    parsers.reserve(nparsers);
    for (size_t i = 0; i < nparsers; ++i)
        parsers.emplace_back(std::make_unique<ByteRingParser>(shared_ring, channel));
    // 解析总数 = 各解析器 message_count 之和(每解析器独立 ItchParser)
    auto total_parsed_count = [&]() -> uint64_t {
        uint64_t total = 0;
        for (auto& p : parsers) total += p->message_count();
        return total;
    };

    // ── 分簿并行: 共享无锁挂单池/索引 + N 个 book_worker ──
    // 共享 OrderPool/OrderMap 无锁(V2.1 前置), 跨 worker 同桶安全; 同一 locate 只归
    // 一个 worker → 同 key 串行。每 worker 一套簿/策略/信号槽, 独立仲裁(决策5)。
    OrderPool shared_pool(cfg.order_book.pool_slots);
    OrderMap  shared_index(cfg.order_book.pool_slots);
    // BookWorker 含引用成员(OrderBookConsumer)不可移动 → unique_ptr 规避 vector 重分配。
    std::vector<std::unique_ptr<BookWorker>> bws;
    bws.reserve(nworkers);
    for (size_t i = 0; i < nworkers; ++i)
        bws.emplace_back(std::make_unique<BookWorker>(shared_pool, shared_index));
    std::vector<std::atomic<uint64_t>> cared_counts(nworkers);     // 每 worker 处理事件数(均衡主键)
    std::vector<std::atomic<uint64_t>> registered_counts(nworkers); // 每 worker 注册 locate 数(均衡次键)
    BookRegistry registry;

    // ── 交易侧：风控 + OMS + 执行引擎 ──
    OrderManager om;
    RiskManager rm;
    rm.set_max_position(cfg.risk.max_position);
    rm.set_max_daily_loss(cfg.risk.max_daily_loss);
    ExecutionEngine ex(om, rm);
    ex.set_base_qty(cfg.execution.base_qty);

    // 订单发送端（→ 模拟交易所）
    auto order_send_ring = std::make_unique<uint8_t[]>(1 << 20);
    auto order_sender = std::make_unique<IoUringSender>(
        "127.0.0.1", cfg.execution.order_port, order_send_ring.get(), 1 << 20);
    if (!order_sender->start()) { printf("订单发送端启动失败\n"); return 1; }
    ex.set_sender(order_sender.get());

    // 成交回报接收端（← 模拟交易所）
    auto fill_rcv = std::make_unique<IoUringReceiver>(cfg.execution.order_ret_port);
    if (!fill_rcv->start()) { printf("回报接收端启动失败\n"); return 1; }

    // ── 线程同步标志 ──
    std::atomic<bool> stop{false};
    std::atomic<bool> parse_done{false};
    std::atomic<size_t> trade_count{0};
    std::atomic<size_t> book_events{0};

    // ── 接收线程(V2.4 多在途 recv): 预提交多 SQE 内核并行收包, recv_batch 逐包 reap ──
    std::thread recv_th([&]() {
        pin_cpu(kPinRecv);   // 绑独立 P 核, 避免被调度器挪走
        receiver->begin_batch();   // 预提交在途 recv SQE(内核并行收包)
        uint8_t buf[65536];
        while (!stop.load(std::memory_order_acquire)) {
            ssize_t n = receiver->recv_batch(buf, sizeof(buf));
            if (n > 0) {
                if (fc) fc->received.fetch_add(1, std::memory_order_release);
                unpacker.feed(buf, (size_t)n);
                for (auto& p : parsers) p->notify();   // 唤醒所有可能阻塞的解析线程
            } else break;
        }
    });

    // 消费者混合退避参数: 短暂空自旋顶住唤醒延迟, 持续空才 eventfd 阻塞。
    // parse_th/book_th 共用(kSpinMax ~ 几十µs)。
    constexpr int kSpinMax = 2000;

    // ── N 个解析线程(V2.3): 从 SPMCByteRing 并行抢消息、并行解析、保序提交 ──
    // 回放结束 = shm 模式(done) / no-shm 模式(stop，由主线程定时置)。
    // 退出: stop/done 且 ring 全部 drain(commit_==tail_, 所有消息已提交)。
    std::vector<std::thread> parse_th;
    parse_th.reserve(nparsers);
    for (size_t i = 0; i < nparsers; ++i) {
        parse_th.emplace_back([&, i]() {
            // 绑核: 全部解析器占小核 16 起(各占一个)。
            // 解析器是协作流水线(抢头串行/提交保序), 快的大核解析器会被小核
            // 解析器拉到同速——大核优势被拉平, 不如全部小核独占。
            pin_cpu(kPinParseE0 + i);
            auto& p = *parsers[i];
            int spin_left = 0;   // 混合退避: 短暂空自旋顶住唤醒延迟, 持续空才阻塞
            while ((fc && !fc->done.load(std::memory_order_acquire)) || (!fc && !stop.load())) {
                p.parse_available();
                if (!p.ring().drained()) { spin_left = kSpinMax; continue; }
                if (spin_left > 0) { --spin_left; _mm_pause(); continue; }   // 短自旋顶唤醒延迟
                spin_left = kSpinMax;
                p.wait_for_data(200);   // 持续空: 阻塞等 recv_th notify
            }
            p.parse_available();   // 排空剩余
        });
    }

    // ── 分簿 book_worker 线程: 广播 + skip, 每 worker 只处理归属自己的 locate ──
    for (size_t i = 0; i < nworkers; ++i)
        bws[i]->init(&ex, &rm, &cfg, &trade_count, &book_events);
    std::vector<std::thread> worker_th;
    worker_th.reserve(nworkers);
    for (size_t i = 0; i < nworkers; ++i) {
        worker_th.emplace_back([&, i]() {
            // 绑核: worker 共享 P 核(订单簿操作轻重不均, P 核算力够; 2 worker 一组
            // 共享一个物理 P 核的 SMT 兄弟)。worker0/1 → P11+P10, worker2/3 → P13+P12。
            // (V2.4 实验修正: worker 绑 E 核算力不足 → 重订单簿操作慢 → SPMC 排队;
            //  改 P 核后全链路 P99 35→26µs, push_spmc→pop P999 9 倍改善。)
            static const int kWorkerP[4] = {11, 10, 13, 12};   // 2组 SMT 兄弟
            if (i < 4) pin_cpu(kWorkerP[i]);
            MarketEvent ev;
            // 排空 + 混合退避: 有数据连续 pop 到空(保吞吐); 短暂空自旋(_mm_pause)顶住
            // 唤醒延迟; 持续空(自旋耗尽)才 wait_for_data 阻塞(省CPU)。
            int spin_left = 0;   // 剩余自旋次数(每空一轮减一, 耗完才阻塞)
            while (!parse_done.load(std::memory_order_acquire) || channel.pending(i) > 0) {
                if (channel.pending(i) == 0) {
                    if (!parse_done.load(std::memory_order_acquire)) {
                        if (spin_left > 0) { --spin_left; _mm_pause(); continue; }   // 短自旋顶唤醒延迟
                        spin_left = kSpinMax;
                        channel.wait_for_data(i);   // 持续空: 广播唤醒, 自己的 fd 无限阻塞
                        continue;
                    }
                    break;
                }
                spin_left = kSpinMax;   // 有数据: 重置自旋预算
                while (channel.pop(i, ev)) {
                    // 关心判定: 查注册表(首次出现注册给最清闲/平局归自己), 非本簿 → skip。
                    // skip 也推进 pop 进度(广播 + skip 模型), 不处理不打探针。
                    uint32_t owner = registry.lookup_or_register(
                        static_cast<uint32_t>(ev.locate),
                        static_cast<uint32_t>(nworkers), cared_counts.data(),
                        registered_counts.data());
                    if (owner != i) continue;   // 非本簿: skip
                    // [LensX 消息级] 排队终点: 消息被归属 worker 从 SPMC 取走(process 前)。
                    // push_spmc→pop 段 = 生产者到 owner 取走(排队等待, 长尾主段)。
                    if (ev.seq_id % lensx::kSample == 0) lensx::mark_pop(ev.seq_id);
                    cared_counts[i].fetch_add(1, std::memory_order_relaxed);
                    bws[i]->process(ev);        // 重建簿 → OFI/OBI 信号 → 独立仲裁
                }
            }
        });
    }

    // ── 回报线程：收 FILL → OMS/Risk ──
    std::atomic<bool> fill_stop{false};
    std::thread fill_th([&]() {
        pin_cpu(kPinIdle);   // 低频回报, 与主线程共享大核 9(不占热核)
        fill_rcv->set_blocking(false);
        uint8_t pre[2048];
        fill_rcv->recv(pre, sizeof(pre));
        fill_rcv->set_blocking(true);
        uint8_t buf[2048];
        while (!fill_stop.load(std::memory_order_acquire)) {
            ssize_t n = fill_rcv->recv(buf, sizeof(buf));
            if (n > 0) {
                uint64_t oid = 0, qty = 0;
                int64_t price = 0;
                if (decode_fill(buf, (size_t)n, oid, qty, price))
                    ex.on_order_fill(oid, qty, price);
            } else break;
        }
    });

    // ── 就绪 → 等回放客户端开始发数据 ──
    printf("接收端就绪，等待回放客户端...\n");
    if (nworkers > 4)
        printf("注意: workers=%zu>4, 超出预留 P 核(recv=5 parse=7 worker=9/11/13/15 fill=16), 额外 worker 未绑核\n",
               nworkers);
    if (fc) fc->ready.store(true, std::memory_order_release);

    // 运行直到回放结束：
    //   shm 模式:  回放客户端发完(置 done)即退出。不用 received/sent 超时推断，
    //              (benchmark 限速等 received，超时推断会因 received 停滞误判退出
    //               而 benchmark 永远等 received 死锁。done 是显式握手。)
    //   no-shm 模式: 运行 idle_timeout_sec 秒(压测脚本: 起 trader → 等 → 发数据 → 汇总)。
    if (fc) {
        auto t_shm_start = std::chrono::steady_clock::now();
        while (!fc->done.load(std::memory_order_acquire))
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        auto t_shm_end = std::chrono::steady_clock::now();
        double wall = std::chrono::duration<double>(t_shm_end - t_shm_start).count();
        uint64_t parsed = total_parsed_count();
        printf("解析 QPS: 均值=%llu msg/s (解析 %llu 条 / %.1f 秒)\n",
               wall > 0 ? (unsigned long long)(parsed / wall) : 0ull,
               (unsigned long long)parsed, wall);
    } else {
        // no-shm 压测模式: 收到第一条消息后, idle_timeout_sec(默认10) 无消息
        // (解析数不再增长) → 写解析总数到文件 → 自动关闭。
        // 从"收到第一条消息"才开始计时, 避免 benchmark 还没发(trader 先启动)时误超时。
        // 同时统计解析 QPS(每秒解析数), 用于确定合理的压测速率。
        uint64_t last_parsed = 0;
        uint64_t idle_sec = 0;
        uint64_t max_qps = 0;       // 峰值解析 QPS(每秒)
        uint64_t total_parsed = 0;  // 总解析数
        uint64_t active_sec = 0;    // 有数据的时间秒数
        bool started = false;
        auto t_start_mon = std::chrono::steady_clock::now();
        while (!started || idle_sec < cfg.execution.idle_timeout_sec) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t p = total_parsed_count();
            if (p != last_parsed) {
                uint64_t inc = p - last_parsed;
                last_parsed = p;
                if (started) {  // 非首条: 算该秒增量
                    total_parsed += inc;
                    ++active_sec;
                    if (inc > max_qps) max_qps = inc;
                }
                idle_sec = 0; started = true;
            }
            else if (started) ++idle_sec;
        }
        auto t_end_mon = std::chrono::steady_clock::now();
        double wall = std::chrono::duration<double>(t_end_mon - t_start_mon).count();
        printf("%d 秒无消息, 停止。解析总数=%llu\n",
               cfg.execution.idle_timeout_sec, (unsigned long long)last_parsed);
        printf("解析 QPS: 峰值=%llu msg/s, 均值=%llu msg/s (有数据 %llu 秒, 墙钟 %.1f 秒)\n",
               (unsigned long long)max_qps,
               active_sec ? (unsigned long long)(total_parsed / active_sec) : 0ull,
               (unsigned long long)active_sec, wall);
        // 把解析总数写入文件(压测脚本读取评估)
        FILE* pf = fopen("trader_parsed.txt", "w");
        if (pf) {
            fprintf(pf, "%llu\n", (unsigned long long)last_parsed);
            fclose(pf);
        }
    }

    // ── 停止 ──
    printf("\n停止中...\n");
    stop.store(true, std::memory_order_release);
    receiver->stop();
    recv_th.join();
    for (auto& p : parsers) p->notify();   // 唤醒所有解析线程
    for (auto& t : parse_th) t.join();     // 解析线程全部排空
    parse_done.store(true, std::memory_order_release);   // 解析完成, 通知 book_worker 可退出
    channel.notify_all();   // 唤醒阻塞在 wait_for_data 的所有 book_worker
    for (auto& t : worker_th) t.join();
    fill_stop.store(true, std::memory_order_release);
    fill_rcv->stop();
    fill_th.join();
    order_sender->stop();

    if (fc) fc->ready.store(false, std::memory_order_release);

    // ── 汇总 ──
    printf("\n=== 运行汇总 ===\n");
    printf("成交事件:   %zu\n", trade_count.load());
    printf("事件处理:   %zu  (book_workers=%zu)\n", book_events.load(), nworkers);
    for (size_t i = 0; i < nworkers; ++i)
        printf("  worker%zu: 处理=%llu  OFI=%lld信号=%d  OBI信号=%d\n",
               i, (unsigned long long)cared_counts[i].load(),
               (long long)bws[i]->ofi.ofi(), (int)bws[i]->ofi.signal().side,
               (int)bws[i]->obi.signal().side);
    printf("订单:       %zu  成交=%zu 风控拒=%zu\n",
           om.order_count(),
           om.count_by_status(OrderStatus::FILLED),
           om.count_by_status(OrderStatus::REJECTED));
    printf("持仓(各worker信号标的):");
    for (size_t i = 0; i < nworkers; ++i)
        printf(" w%zu=%llu", i, (unsigned long long)rm.position(bws[i]->ofi.signal().locate));
    printf("  已实现盈亏=%lld 分\n", (long long)rm.realized_pnl());

    delete[] chan_slots;
    if (fc) munmap(fc, sizeof(FlowControl));
    return 0;
}
