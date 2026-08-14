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
#include "core/net/i_market_data_receiver.h"
#include "core/net/io_uring_receiver.h"
#include "core/net/io_uring_sender.h"
#include "core/net/af_xdp_receiver.h"
#ifdef HAVE_DPDK
#include "core/net/dpdk_receiver.h"
#endif
#include "core/prof/lensx_probe.h"

#include <immintrin.h>   // _mm_pause（高频消费者忙轮询暂停）
#include "core/queue/queue_manager.h"
#include "core/queue/spmc_event_queue.h"
#include "core/dispatch/dispatcher.h"
#include "core/queue/spsc_byte_ring.h"
#include "core/queue/spsc_event_ring.h"
#include "execution/execution_engine.h"
#include "market/book/order_book_consumer.h"
#include "market/pipeline/byte_ring_parser.h"
#include "market/pipeline/mold_udp_unpacker.h"
#include "oms/order_manager.h"
#include "oms/i_order_codec.h"
#include "oms/ouch_order_codec.h"
#include "risk/risk_manager.h"
#include "strategy/base/arbitrate.h"
#include "strategy/base/strategy.h"
#include "strategy/tick/order_book_imbalance_strategy.h"
#include "strategy/tick/order_flow_imbalance_strategy.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
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
//   worker         E 20-23(小核无 SMT 真独占; P 核虽算力强但 SMT 兄弟被系统
//                  进程抢占, 实际只一半算力, 真实速率下 E 核够用且独占更优)
//   fill/主线程    共享 P 核 9(低频, 不占 E 核, P 核空闲)
static const int kPinRecv    = 5;   // io_uring 收包(高吞吐, 大核)
static const int kPinParseE0 = 7;   // 解析器 P 核 7(V1 同款; E16 实测 push_ring→parse 50ms, P7 6.4µs)
static const int kPinRetryE  = 17;  // retry 线程小核 E17(处理下游满, 常驻)
static const int kPinIdle    = 9;   // 低频共享核(fill + 主线程, P 核空闲, 不占 E 核)

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


// 下单节奏(V1.5 定稿): 方向翻转 → 必下; 方向不变 → 强度相对上次下单跳变 ≥ 阈值才再下。
static constexpr int64_t kStrengthStep = 500;   // 千分比定点(500 = 5% 满强度)

// ── 单 book_worker: 自己的簿/策略列表/信号槽 + 独立仲裁 ──
// 模板化: Strategies 编译期绑定(CRTP 策略, 无虚调用), 每策略一个信号槽。
// 仲裁契约: 全部策略信号同向(非 NONE)才下单, 以 slot0 = primary 的 locate/strength/seq 为准。
template <class... Strategies>
struct BookWorker {
    OrderBookConsumer obc;                    // 自己关心的标的簿(共享池/索引)
    std::tuple<Strategies...> strategies_;    // 策略列表(编译期绑定)
    std::array<SignalSlot, sizeof...(Strategies)> slots_;   // 每策略一个信号槽
    // 仲裁运行时配置(init 从 cfg.strategy 解析): 各策略权重(万分比) + 主策略索引 + 净投票阈值
    std::array<int64_t, sizeof...(Strategies)> weight_bp_{};
    size_t primary_idx_ = 0;
    int64_t threshold_bp_ = 0;
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
        // 仲裁运行时配置: 权重按策略名对应槽(未列缺省 1.0 = 10000), primary 索引, 净投票阈值
        const auto& st = c->strategy.strategies;
        for (size_t i = 0; i < sizeof...(Strategies) && i < st.size(); ++i) {
            auto it = c->strategy.weights_bp.find(st[i]);
            weight_bp_[i] = (it != c->strategy.weights_bp.end()) ? it->second : 10000;
            if (st[i] == c->strategy.primary) primary_idx_ = i;
        }
        threshold_bp_ = c->strategy.vote_threshold_bp;
    }

    // 统一仲裁: 加权净投票定方向(可配置权重/主策略/阈值), 独立仲裁 → 时序天然正确(决策5)。
    // 下单节奏: 方向翻转必下; 方向不变仅强度跳变 ≥ 阈值才下(last_order_str 是
    // 上次下单时强度, 保证"强→弱"不回补)。
    void arbitrate() {
        // [LensX 级别3] 仲裁函数起点/终点: 抽中才打(同一次调用内局部变量保证成对)。
        bool arb_sample = (arb_sample_cnt++ % lensx::kSample == 0);
        if (arb_sample) lensx::mark_arb_start();
        // 构造净投票输入(槽 → ArbSignal), 调纯函数决策
        ArbSignal sigs[sizeof...(Strategies) > 0 ? sizeof...(Strategies) : 1];
        for (size_t i = 0; i < slots_.size(); ++i) {
            sigs[i].side     = slots_[i].side.load(std::memory_order_acquire);
            sigs[i].locate   = slots_[i].locate.load(std::memory_order_acquire);
            sigs[i].strength = slots_[i].strength.load(std::memory_order_acquire);
            sigs[i].seq      = slots_[i].seq.load(std::memory_order_acquire);
        }
        ArbDecision d = arbitrate_decide(sigs, slots_.size(),
                                         weight_bp_.data(), primary_idx_, threshold_bp_);
        if (d.act) {
            OrderSide dir = d.dir;
            uint64_t locate = d.locate;
            int64_t strength = d.strength;
            uint64_t pos = rm->position(locate);
            bool blocked = (dir == OrderSide::BUY && pos >= cfg->risk.max_position) ||
                           (dir == OrderSide::SELL && pos == 0);
            // 下单触发(强度阈值触发): 方向翻转必下; 方向不变仅强度跳变 ≥ 阈值才下。
            int64_t last_str = last_order_str.load();
            bool fresh_dir  = (dir != last_order_side.load());
            bool strength_ge = (strength >= last_str + kStrengthStep);
            if (!blocked && (fresh_dir || strength_ge)) {
                // [LensX 级别4] 下单决策→执行完毕(key=primary.seq 信号触发seq, 抽样)。
                // 抽样判断在调用点(抽中才调), 避免 uprobe 每次命中拖垮吞吐。
                uint64_t order_seq = d.seq;
                if (order_seq % lensx::kSample == 0) lensx::mark_order_start(order_seq);
                Signal decision{.side = dir, .locate = locate,
                                .price = 0, .timestamp = 0, .strength = strength};
                uint64_t oid = ex->submit_signal(decision, 1);
                if (oid != 0) {
                    last_order_side.store(dir);
                    last_order_str.store(strength);
                }
                if (order_seq % lensx::kSample == 0) lensx::mark_order_end(order_seq);
            }
        }
        if (arb_sample) lensx::mark_arb_end();
    }

    // 处理一个归属本 worker 的事件: 重建簿 → BookContext → 逐策略信号 → 仲裁下单。
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
        // 框架算一次 BookContext(方向 + BBO + 现价), 各策略共享, 避免每策略查簿。
        BookContext ctx;
        ctx.book = book;
        ctx.seq = ev.seq_id;
        if (book && book->best_bid() >= 0 && book->best_ask() >= 0) {
            ctx.bid = book->best_bid(); ctx.bid_vol = book->best_bid_volume();
            ctx.ask = book->best_ask(); ctx.ask_vol = book->best_ask_volume();
            ctx.mid = (ctx.bid + ctx.ask) / 2;
        }
        // 方向：A/U 自带 side；D/X/E 查簿
        if (ev.type == MarketEvent::Type::ADD ||
            ev.type == MarketEvent::Type::REPLACE) {
            ctx.side = ev.order.side;
        } else if (book) {
            if (ev.type == MarketEvent::Type::TRADE ||
                ev.type == MarketEvent::Type::EXECUTE)
                ctx.side = book->side_of(ev.trade.order_ref);
            else
                ctx.side = book->side_of(ev.order.order_ref);
        }
        // V5 盯市: 盘口有效时喂中间价给风控(回撤按盯市净值计算)
        if (ctx.bid >= 0 && ctx.ask >= 0)
            rm->mark(ev.locate, ctx.mid);
        // 逐策略 on_market(方向/盘口门控由策略内部保证) + 信号 → 原子槽
        feed(ev, ctx, std::index_sequence_for<Strategies...>{});
        arbitrate();   // 写完各策略信号, 检查是否齐 → 仲裁下单
    }

    // 编译期展开: 每策略 on_market + signal → 对应槽(无虚调用, CRTP 内联)。
    template <size_t... Is>
    void feed(const MarketEvent& ev, const BookContext& ctx, std::index_sequence<Is...>) {
        (feed_one<Is>(ev, ctx), ...);
    }
    template <size_t I>
    void feed_one(const MarketEvent& ev, const BookContext& ctx) {
        std::get<I>(strategies_).on_market(ev, ctx);
        Signal s = std::get<I>(strategies_).signal();
        slots_[I].side.store(s.side, std::memory_order_release);
        slots_[I].locate.store(s.locate, std::memory_order_release);
        slots_[I].strength.store(s.strength, std::memory_order_release);
        slots_[I].seq.store(ev.seq_id, std::memory_order_release);
    }

    // 读第 i 个槽的当前信号(汇总/查询用; slot0 = primary)。
    Signal slot_signal(size_t i) const {
        return Signal{.side = slots_[i].side.load(std::memory_order_acquire),
                      .locate = slots_[i].locate.load(std::memory_order_acquire),
                      .price = 0, .timestamp = 0,
                      .strength = slots_[i].strength.load(std::memory_order_acquire)};
    }
};

// ── 网络后端工厂：按配置实例化 IMarketDataReceiver ──
// 三后端 recv() 语义统一(返回纯 UDP 载荷, AF_XDP/DPDK 内部已剥帧头)，
// 业务/解析逻辑不感知后端差异。
static std::unique_ptr<IMarketDataReceiver> make_receiver(const MarketConfig& m) {
#ifdef HAVE_AF_XDP
    if (m.backend == "af_xdp") {
        if (m.ifname.empty()) {
            printf("af_xdp 后端需配置 market.ifname(绑定接口)\n");
            return nullptr;
        }
        return std::make_unique<AF_XDPReceiver>(m.ifname, m.port);
    }
#endif
#ifdef HAVE_DPDK
    if (m.backend == "dpdk") {
        if (m.vdev.empty()) {
            printf("dpdk 后端需配置 market.vdev(vdev 规格)\n");
            return nullptr;
        }
        // eal_args 未配时给默认(纯软件 vdev: 免 PCI/大页)
        auto args = m.eal_args;
        if (args.empty())
            args = {"-l", "0", "--no-pci", "--no-huge", "-m", "128"};
        return std::make_unique<DPDKReceiver>(m.vdev, m.port, args);
    }
#endif
    if (m.backend != "io_uring") {
        // 配置了未编译的后端(如无 libbpf 时配 af_xdp): 明确报错而非静默回退
        printf("后端 %s 未编译(需 libbpf 或 DPDK 开发包), 回退 io_uring\n", m.backend.c_str());
        return nullptr;
    }
    return std::make_unique<IoUringReceiver>(m.port);   // 默认 io_uring
}

// ── 策略组合 → 模板实例化 ──
using OFI = OrderFlowImbalanceStrategy;
using OBI = OrderBookImbalanceStrategy;

// 运行主体模板: 按编译期绑定的策略组合实例化 BookWorker<Strategies...>。
// 启动期由 config.strategy.strategies 白名单 dispatch 到具体组合(不运行时插拔)。
template <class... Strategies>
int run_strategies(const Config& cfg, bool no_shm) {
    pin_cpu(kPinIdle);   // 主线程低频(配置/汇总), 与 fill 共享 P 核 9

    // 校验主策略在启用列表中(否则 primary 索引回退 slot0 是静默错误)
    if (!cfg.strategy.strategies.empty() &&
        std::find(cfg.strategy.strategies.begin(), cfg.strategy.strategies.end(),
                  cfg.strategy.primary) == cfg.strategy.strategies.end()) {
        fprintf(stderr, "主策略 %s 不在启用的策略列表(须是 strategies 之一)\n",
                cfg.strategy.primary.c_str());
        return 1;
    }

    // 打印启用的策略名
    std::string strat_names;
    for (const auto& n : cfg.strategy.strategies) {
        if (!strat_names.empty()) strat_names += "+";
        strat_names += n;
    }
    if (strat_names.empty()) strat_names = "(空)";

    printf("NebulaX-Trader v0.1.0\n");
    printf("  网络后端: %s  行情端口: %u  模拟交易所收单: %u  回报: %u  策略: %s\n",
           cfg.market.backend.c_str(), cfg.market.port, cfg.execution.order_port,
           cfg.execution.order_ret_port, strat_names.c_str());

    // ── 清理共享内存残留（避免复用旧计数器）──
    if (!no_shm) shm_unlink(FLOW_SHM_PATH);

    // ── 行情接收端(按 backend 配置实例化 io_uring / af_xdp / dpdk)──
    auto receiver = make_receiver(cfg.market);
    if (!receiver) return 1;
    if (!receiver->start()) { printf("接收端启动失败 (backend=%s)\n", cfg.market.backend.c_str()); return 1; }

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
    // V3: 字节 ring 回退 SPSC(单生产 recv_th + 单消费者 单解析器)。
    // SPMC 多解析器版(V2.3)保留在 git 历史, 面向 AF_XDP 后高吞吐场景。
    auto ring_buf = std::make_unique<uint8_t[]>(cfg.market.ring_bytes);
    size_t ring_id = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING,
                                          ring_buf.get(), cfg.market.ring_bytes);
    auto& shared_ring = QueueManager::get<SPSCByteRing>(ring_id);

    // ── V3 分发器下游: N 条 SPSC(每 worker 一条) + N 个 retry 桶(每 SPSC 独立容量) ──
    // 单解析器按 registry 分发到 spsc[owner], 满进 retry 桶; worker 只 pop 自己的 spsc。
    // 同一序列 + 一个 locate 只归一个 worker → 订单簿时序正确(成交不先于对应委托)。
    const size_t nworkers = std::max<size_t>(1, cfg.order_book.workers);
    const size_t per_chan = cfg.market.chan_slots / nworkers;   // 每条 SPSC 容量
    // 统一唤醒: 所有 worker 共享一个 eventfd, push 无条件写一次唤醒全部阻塞 worker。
    // 消除每队列独立 fd + blocked_ 登记的时序竞态。
    const int kWorkerWakeFd = eventfd(0, EFD_NONBLOCK);
    std::vector<std::unique_ptr<MarketEvent[]>> chan_bufs;      // 持有槽位数组
    std::vector<std::unique_ptr<SPSCEventRing>> spscs;
    spscs.reserve(nworkers);
    for (size_t i = 0; i < nworkers; ++i) {
        auto buf = std::make_unique<MarketEvent[]>(per_chan);
        spscs.emplace_back(std::make_unique<SPSCEventRing>(buf.get(), per_chan, kWorkerWakeFd));
        chan_bufs.push_back(std::move(buf));   // 保证槽位存活(队列不拥有)
    }
    // retry 桶: 每 SPSC 一个, 独立容量(小容量即可, 桶满才阻塞解析器)。
    // retry 桶共享一个唤醒 fd(retry 线程 poll 它, 与 worker 唤醒分开)。
    const int kRetryWakeFd = eventfd(0, EFD_NONBLOCK);
    std::vector<std::unique_ptr<MarketEvent[]>> retry_bufs;
    std::vector<std::unique_ptr<RetryBucket>> retry_buckets;
    retry_buckets.reserve(nworkers);
    for (size_t i = 0; i < nworkers; ++i) {
        auto buf = std::make_unique<MarketEvent[]>(per_chan);
        retry_buckets.emplace_back(std::make_unique<RetryBucket>(buf.get(), per_chan, kRetryWakeFd));
        retry_bufs.push_back(std::move(buf));
    }
    // SPSCEventRing 数组(Dispatcher 需要连续指针): 从 vector 收集 raw 指针。
    std::vector<SPSCEventRing*> spsc_ptrs;
    for (auto& q : spscs) spsc_ptrs.push_back(q.get());
    std::vector<RetryBucket*> retry_ptrs;
    for (auto& b : retry_buckets) retry_ptrs.push_back(b.get());
    Dispatcher dispatcher(spsc_ptrs.data(), retry_ptrs.data(), nworkers);

    // 双键负载均衡计数: cared(处理事件数, 主键) + registered(注册 locate 数, 次键)。
    std::vector<std::atomic<uint64_t>> cared_counts(nworkers);
    std::vector<std::atomic<uint64_t>> registered_counts(nworkers);

    MoldUdpUnpacker unpacker(shared_ring);
    // V3: 单解析器(SPSC 单消费者) + 分发器(解析出事件按 locate 分发)。
    auto parser = std::make_unique<ByteRingParser>(shared_ring, dispatcher,
                                                   cared_counts.data(), registered_counts.data());
    auto total_parsed_count = [&]() -> uint64_t {
        return parser->message_count();
    };

    // ── 分簿并行: 共享无锁挂单池/索引 + N 个 book_worker ──
    // 共享 OrderPool/OrderMap 无锁(V2.1 前置), 跨 worker 同桶安全; 同一 locate 只归
    // 一个 worker → 同 key 串行。每 worker 一套簿/策略/信号槽, 独立仲裁(决策5)。
    OrderPool shared_pool(cfg.order_book.pool_slots);
    OrderMap  shared_index(cfg.order_book.pool_slots);
    // BookWorker 含引用成员(OrderBookConsumer)不可移动 → unique_ptr 规避 vector 重分配。
    std::vector<std::unique_ptr<BookWorker<Strategies...>>> bws;
    bws.reserve(nworkers);
    for (size_t i = 0; i < nworkers; ++i)
        bws.emplace_back(std::make_unique<BookWorker<Strategies...>>(shared_pool, shared_index));

    // ── 交易侧：风控 + OMS + 执行引擎 ──
    OrderManager om;
    RiskManager rm;
    rm.set_max_position(cfg.risk.max_position);
    rm.set_max_daily_loss(cfg.risk.max_daily_loss);
    // V5 盯市回撤风控: 初始资金 + 两档回撤阈值
    rm.set_initial_equity(cfg.risk.initial_equity);
    rm.set_max_drawdown_pause(cfg.risk.max_drawdown_pause);
    rm.set_max_drawdown_flatten(cfg.risk.max_drawdown_flatten);
    ExecutionEngine ex(om, rm);
    ex.set_base_qty(cfg.execution.base_qty);

    // V5 协议解耦: 订单字节经 IOrderCodec 编解码, 业务逻辑不碰协议字节。
    // 默认 OUCH 4.2(实盘协议化); CustomOrderCodec 保留作回退/对照。
    OuchOrderCodec order_codec;
    ex.set_codec(&order_codec);

    // 订单发送端(TCP 全双工: 订单发 + 回报收共用一连接) → 模拟交易所
    auto order_send_ring = std::make_unique<uint8_t[]>(1 << 20);
    auto order_sender = std::make_unique<IoUringSender>(
        "127.0.0.1", cfg.execution.order_port, order_send_ring.get(), 1 << 20);
    if (!order_sender->start()) { printf("订单发送端启动失败\n"); return 1; }
    ex.set_sender(order_sender.get());

    // ── 线程同步标志 ──
    std::atomic<bool> stop{false};
    std::atomic<bool> parse_done{false};
    std::atomic<size_t> trade_count{0};
    std::atomic<size_t> book_events{0};

    // ── 接收线程: 按后端走最优收包路径 ──
    //   io_uring: 多在途 recv_batch(V2.4, 预提交多 SQE 内核并行收包)
    //   AF_XDP/DPDK: 单包 recv()(receiver 内部已剥帧头, 返回纯载荷, 语义统一)
    //   三后端都返回 MoldUDP64 载荷 → 拆包/解析/分发逻辑完全相同
    std::thread recv_th([&]() {
        pin_cpu(kPinRecv);   // 绑独立 P 核, 避免被调度器挪走
        auto* uring = dynamic_cast<IoUringReceiver*>(receiver.get());
        uint8_t buf[65536];
        if (uring) {
            uring->begin_batch();   // 预提交在途 recv SQE(内核并行收包)
            while (!stop.load(std::memory_order_acquire)) {
                ssize_t n = uring->recv_batch(buf, sizeof(buf));
                if (n > 0) {
                    if (fc) fc->received.fetch_add(1, std::memory_order_release);
                    unpacker.feed(buf, (size_t)n);
                    parser->notify();   // 唤醒可能阻塞的解析线程(单解析器)
                } else break;
            }
        } else {
            while (!stop.load(std::memory_order_acquire)) {
                ssize_t n = receiver->recv(buf, sizeof(buf));
                if (n > 0) {
                    if (fc) fc->received.fetch_add(1, std::memory_order_release);
                    unpacker.feed(buf, (size_t)n);
                    parser->notify();
                } else break;
            }
        }
    });

    // 消费者混合退避参数: 短暂空自旋顶住唤醒延迟, 持续空才 eventfd 阻塞。
    // parse_th/book_th 共用(kSpinMax ~ 几十µs)。
    constexpr int kSpinMax = 2000;

    // ── 单解析线程(V3): 从 SPSCByteRing 读(单消费者) → 解析 → push 事件 SPMC ──
    // 退出: stop(主线程置, 已确认所有包收到并喂进 ring)。统一用 stop 而非 done:
    //   shm 模式 done 只表示 benchmark 发完, 最后一包可能仍在途未 feed 进 ring;
    //   若用 done 退出, 排空会漏掉 done 后 recv_th 才 feed 的包(丢数据)。
    //   主线程 done 后等 received==sent 再 stop, 期间解析线程持续消费, 不丢包。
    std::thread parse_th([&]() {
        pin_cpu(kPinParseE0);   // 单解析器占 P 核 7(V1 同款, 算力足)
        auto& p = *parser;
        int spin_left = 0;   // 混合退避: 短暂空自旋顶住唤醒延迟, 持续空才阻塞
        while (!stop.load(std::memory_order_acquire)) {
            p.parse_available();
            if (!p.ring().empty()) { spin_left = kSpinMax; continue; }
            if (spin_left > 0) { --spin_left; _mm_pause(); continue; }   // 短自旋顶唤醒延迟
            spin_left = kSpinMax;
            p.wait_for_data(200);   // 持续空: 阻塞等 recv_th notify
        }
        p.parse_available();   // 排空剩余
    });

    // ── 分簿 book_worker 线程: 每 worker 只 pop 自己的 SPSC(V3 分发器) ──
    // 分发器已按 locate 把事件推到 spsc[owner], worker 无 skip、无 registry 查表。
    for (size_t i = 0; i < nworkers; ++i)
        bws[i]->init(&ex, &rm, &cfg, &trade_count, &book_events);
    std::vector<std::thread> worker_th;
    worker_th.reserve(nworkers);
    for (size_t i = 0; i < nworkers; ++i) {
        worker_th.emplace_back([&, i]() {
            // 绑核: worker 占 P 核 11/13/15/9(奇数避开 SMT 兄弟被系统占用)。
            // 实测 E 核 20-23 被系统进程抢占(gmain/tokio/mihomo 等), dispatch→pop
            // P99 833µs → P 核 22.6µs(37x)。E 核"无 SMT 真独占"在本机不成立。
            static const int kWorkerE[4] = {11, 13, 15, 9};
            if (i < 4) pin_cpu(kWorkerE[i]);
            MarketEvent ev;
            auto& my_ring = *spscs[i];   // 自己的 SPSC, 单消费者
            // 排空 + 混合退避: 有数据连续 pop 到空(保吞吐); 短暂空自旋顶住唤醒延迟;
            // 持续空才 wait_for_data 阻塞(省CPU)。
            int spin_left = 0;
            while (!parse_done.load(std::memory_order_acquire) || my_ring.pending() > 0) {
                if (my_ring.pending() == 0) {
                    if (!parse_done.load(std::memory_order_acquire)) {
                        if (spin_left > 0) { --spin_left; _mm_pause(); continue; }
                        spin_left = kSpinMax;
                        my_ring.set_blocked();         // 登记阻塞(push 会唤醒)
                        my_ring.wait_for_data(1000);   // 阻塞等分发器/retry 推
                        my_ring.set_active();
                        continue;
                    }
                    break;
                }
                spin_left = kSpinMax;
                while (my_ring.pop(ev)) {
                    // [LensX 消息级] 排队终点: 消息被归属 worker 从 SPSC 取走(process 前)。
                    if (ev.seq_id % lensx::kSample == 0) lensx::mark_pop(ev.seq_id);
                    cared_counts[i].fetch_add(1, std::memory_order_relaxed);
                    bws[i]->process(ev);        // 重建簿 → OFI/OBI 信号 → 独立仲裁
                }
            }
        });
    }

    // ── retry 线程(V3): 常驻, 处理下游 SPSC 满(解析器卸载到 retry 桶的事件) ──
    // 阻塞 poll N 个桶的 fd → 唤醒 → 遍历有数据的桶, 推能推的到 spsc[i] →
    // 桶清空才清 active(保序: 期间解析器只能进桶)。全清空再阻塞。
    std::thread retry_th([&]() {
        pin_cpu(kPinRetryE);   // retry 占 E17(小核)
        std::vector<struct pollfd> pfds(nworkers);
        for (size_t i = 0; i < nworkers; ++i) {
            pfds[i].fd = retry_buckets[i]->bucket.wake_fd();
            pfds[i].events = POLLIN;
            pfds[i].revents = 0;
        }
        MarketEvent ev;
        // 全空判定: 所有桶 pending==0。
        auto all_empty = [&]() {
            for (size_t i = 0; i < nworkers; ++i)
                if (retry_buckets[i]->bucket.pending() > 0) return false;
            return true;
        };
        while (!parse_done.load(std::memory_order_acquire) || !all_empty()) {
            // 任一桶非空 → 处理; 全空 → 阻塞等解析器唤醒。
            if (all_empty()) {
                if (parse_done.load(std::memory_order_acquire)) break;
                poll(pfds.data(), nworkers, 1000);   // 阻塞等任一桶唤醒
                continue;
            }
            // 遍历桶: 先 peek 头, 能推 spsc 才 pop(防取出发推不回的乱序)。
            for (size_t i = 0; i < nworkers; ++i) {
                auto& rq = *retry_buckets[i];
                while (rq.bucket.peek(ev)) {
                    if (!spscs[i]->push(ev)) break;   // spsc 满, 推不进, 头留在桶下轮
                    rq.bucket.pop(ev);                // 推成功才取走(peek→pop 原子一致)
                    // [LensX 消息级] retry 推回 spsc(retry_in→retry_out 段终点, 抽样)。
                    if (ev.seq_id % lensx::kSample == 0) lensx::mark_retry_out(ev.seq_id);
                }
                // 桶清空才清 active(保序前提: 解析器期间只能进桶)。
                if (rq.bucket.pending() == 0)
                    rq.active.store(false, std::memory_order_release);
            }
        }
    });

    // ── 回报线程：从订单连接的同一 TCP 读回报(全双工) → OMS/Risk ──
    // TCP 是字节流无消息边界: 累积缓冲, 按 OUCH 消息类型定长分帧, 解析完整消息再处理。
    std::atomic<bool> fill_stop{false};
    std::thread fill_th([&]() {
        pin_cpu(kPinIdle);   // 低频回报, 与主线程共享 P 核 9(不占热核)
        order_sender->set_blocking(false);
        uint8_t pre[2048];
        order_sender->recv(pre, sizeof(pre));   // 排空残留
        order_sender->set_blocking(true);
        uint8_t buf[2048];
        std::vector<uint8_t> stream;   // TCP 字节流累积缓冲
        stream.reserve(4096);
        while (!fill_stop.load(std::memory_order_acquire)) {
            ssize_t n = order_sender->recv(buf, sizeof(buf));
            if (n > 0) {
                stream.insert(stream.end(), buf, buf + n);
                // 按 OUCH 定长分帧: 首字节定消息类型 → 定长
                for (;;) {
                    if (stream.empty()) break;
                    size_t mlen = 0;
                    if (stream[0] == OuchOrderCodec::kMsgAck) mlen = OuchOrderCodec::kAckMsgLen;
                    else if (stream[0] == OuchOrderCodec::kMsgExec) mlen = OuchOrderCodec::kExecMsgLen;
                    else if (stream[0] == OuchOrderCodec::kMsgCancel) mlen = OuchOrderCodec::kCancelMsgLen;
                    else if (stream[0] == OuchOrderCodec::kMsgReject) mlen = OuchOrderCodec::kRejectMsgLen;
                    else if (stream[0] == OuchOrderCodec::kMsgBook) mlen = OuchOrderCodec::kBookMsgLen;
                    else { stream.erase(stream.begin()); continue; }   // 未知字节, 丢弃
                    if (stream.size() < mlen) break;   // 消息未完整, 等更多数据
                    if (stream[0] == OuchOrderCodec::kMsgBook) {
                        // 'B' 盘口响应: 交给盘口查询等待方
                        BookQuote bq;
                        if (order_codec.decode_book(stream.data(), mlen, bq))
                            ex.on_book_quote(bq);
                    } else {
                        Fill f;
                        if (order_codec.decode_fill(stream.data(), mlen, f))
                            ex.on_order_report(f);   // 按 type 分发(A 接受/E 成交/C 撤/J 拒)
                    }
                    stream.erase(stream.begin(), stream.begin() + mlen);   // 消费完整消息
                }
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
        // done = benchmark 发完所有包(限速: 发一包等 received 追上)。但最后一包 sendto 后
        // 立即置 done, 可能还在内核缓冲/在途未收。等 received 追上 sent 再停止, 否则
        // receiver->stop() 打断 recv 会丢在途包(正确性: sent == parsed 的根基)。
        while (fc->received.load(std::memory_order_acquire) <
               fc->sent.load(std::memory_order_acquire))
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
    parser->notify();     // 唤醒解析线程
    parse_th.join();      // 解析线程排空
    parse_done.store(true, std::memory_order_release);   // 解析完成, 通知 worker/retry 可退出
    for (auto& b : retry_buckets) b->bucket.wake();   // 唤醒阻塞在 poll 的 retry 线程
    retry_th.join();   // retry 先把桶里积压推完(否则 worker 消费不完整)
    for (auto& q : spscs) q->wake();          // 唤醒阻塞在 wait_for_data 的 worker
    for (auto& t : worker_th) t.join();
    fill_stop.store(true, std::memory_order_release);
    order_sender->stop();   // 打断 fill_th 的阻塞 recv(TCP 全双工, 同一连接)
    fill_th.join();

    if (fc) fc->ready.store(false, std::memory_order_release);

    // ── 汇总 ──
    printf("\n=== 运行汇总 ===\n");
    printf("成交事件:   %zu\n", trade_count.load());
    printf("事件处理:   %zu  (book_workers=%zu)\n", book_events.load(), nworkers);
    for (size_t i = 0; i < nworkers; ++i) {
        Signal s0 = bws[i]->slot_signal(0);   // slot0 = primary
        int64_t ofi_val = 0;
        bool slot0_is_ofi = false;
        if constexpr (sizeof...(Strategies) >= 1) {
            using S0 = std::decay_t<decltype(std::get<0>(bws[i]->strategies_))>;
            if constexpr (std::is_same_v<S0, OFI>) {   // 窗口值仅 OFI 有, 编译期特判
                ofi_val = std::get<0>(bws[i]->strategies_).ofi();
                slot0_is_ofi = true;
            }
        }
        printf("  worker%zu: 处理=%llu 注册=%llu  primary信号=%d 强度=%lld%s",
               i, (unsigned long long)cared_counts[i].load(),
               (unsigned long long)registered_counts[i].load(),
               (int)s0.side, (long long)s0.strength,
               slot0_is_ofi ? " OFI窗口=" : "");
        if (slot0_is_ofi) printf("%lld", (long long)ofi_val);
        printf("\n");
    }
    printf("订单:       %zu  成交=%zu 风控拒=%zu\n",
           om.order_count(),
           om.count_by_status(OrderStatus::FILLED),
           om.count_by_status(OrderStatus::REJECTED));
    printf("持仓(各worker primary信号标的):");
    for (size_t i = 0; i < nworkers; ++i)
        printf(" w%zu=%llu", i, (unsigned long long)rm.position(bws[i]->slot_signal(0).locate));
    printf("  已实现盈亏=%lld 分\n", (long long)rm.realized_pnl());
    // V5 盯市回撤统计
    printf("  盯市净值=%lld 分 峰值=%lld 分 回撤=%lld 分 暂停=%s 平仓=%s\n",
           (long long)rm.equity(), (long long)rm.equity_peak(),
           (long long)rm.drawdown(),
           rm.drawdown_paused() ? "是" : "否",
           rm.drawdown_flatten() ? "是" : "否");

    if (fc) munmap(fc, sizeof(FlowControl));
    return 0;
}

// ── 入口: 解析参数 + 加载配置 → 按策略组合 dispatch 到编译期实例化 ──
int main(int argc, char* argv[]) {
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

    // 白名单组合(不运行时插拔; 槽序 = 仲裁优先级, 首个为 primary)。
    // 空列表 = 仅收行情不交易(替代旧 use_*:false 的"全停"语义)。
    const auto& st = cfg.strategy.strategies;
    if (st == std::vector<std::string>{"ofi", "obi"})
        return run_strategies<OFI, OBI>(cfg, no_shm);
    if (st == std::vector<std::string>{"ofi"})
        return run_strategies<OFI>(cfg, no_shm);
    if (st == std::vector<std::string>{"obi"})
        return run_strategies<OBI>(cfg, no_shm);
    if (st.empty()) {
        fprintf(stderr, "策略列表为空，仅收行情不交易\n");
        return run_strategies<>(cfg, no_shm);
    }
    fprintf(stderr, "不支持的策略组合(支持 [ofi,obi]/[ofi]/[obi]/[])\n");
    return 1;
}
