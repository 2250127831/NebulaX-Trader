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
#include "strategy/combo/signal_combiner.h"
#include "strategy/kline/kline_aggregator.h"
#include "strategy/momentum/momentum_strategy.h"
#include "strategy/tick/order_book_imbalance_strategy.h"
#include "strategy/tick/order_flow_imbalance_strategy.h"
#include "strategy/tick/price_breakout_strategy.h"
#include "strategy/tick/tick_momentum_strategy.h"
#include "strategy/tick/trade_direction_strategy.h"
#include "strategy/tick/volume_breakout_strategy.h"
#include "strategy/trend/trend_strategy.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <chrono>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static void usage(const char* prog) {
    printf("Usage: %s [--config <path>]\n", prog);
}

int main(int argc, char* argv[]) {
    // ── 解析参数 + 加载配置 ──
    std::string config_path = "config/default.yaml";
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc) config_path = argv[++i];
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
    shm_unlink(FLOW_SHM_PATH);

    // ── 行情接收端 ──
    auto receiver = std::make_unique<IoUringReceiver>(cfg.market.port);
    if (!receiver->start()) { printf("接收端启动失败\n"); return 1; }

    // ── 共享内存 FlowControl（发给回放客户端/benchmark）──
    int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
    if (shm_fd < 0) { perror("shm_open"); return 1; }
    if (ftruncate(shm_fd, sizeof(FlowControl)) < 0) { perror("ftruncate"); return 1; }
    auto* fc = static_cast<FlowControl*>(mmap(nullptr, sizeof(FlowControl),
        PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
    close(shm_fd);
    if (fc == MAP_FAILED) { perror("mmap"); return 1; }
    fc->ready.store(false, std::memory_order_release);
    fc->sent.store(0, std::memory_order_release);
    fc->received.store(0, std::memory_order_release);
    fc->order_ret_port.store(cfg.execution.order_ret_port, std::memory_order_release);

    // ── 双通道 + 共享 ring ──
    auto ring_buf = std::make_unique<uint8_t[]>(cfg.market.ring_bytes);
    size_t ring_id = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING,
                                          ring_buf.get(), cfg.market.ring_bytes);
    auto& shared_ring = QueueManager::get<SPSCByteRing>(ring_id);

    auto* chan_a_slots = new MarketEvent[cfg.market.chan_slots];
    size_t chan_a_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                            chan_a_slots, cfg.market.chan_slots, 1);
    auto& channel_a = QueueManager::get<SPMCEventQueue<16>>(chan_a_id);

    auto* chan_b_slots = new MarketEvent[cfg.market.chan_slots];
    size_t chan_b_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                            chan_b_slots, cfg.market.chan_slots, 1);
    auto& channel_b = QueueManager::get<SPMCEventQueue<16>>(chan_b_id);

    MoldUdpUnpacker unpacker(shared_ring);
    ByteRingParser parser(shared_ring, channel_a, channel_b);

    // ── 策略 ──
    // 主策略：成交量突破（默认），也可配置趋势/动量
    VolumeBreakoutStrategy vbs(cfg.strategy.vol_window, cfg.strategy.vol_threshold);
    PriceBreakoutStrategy pbs;
    TradeDirectionStrategy tds;
    TickMomentumStrategy tms;
    TrendStrategy trend;
    MomentumStrategy mom;
    KLineAggregator kagg(0, cfg.strategy.kline_ticks);   // 按数量分窗
    kagg.set_sink([&](const KLine& bar) { trend.on_bar(bar); mom.on_bar(bar); });
    SignalCombiner combiner;

    // 主策略信号选择
    auto primary_signal = [&]() -> Signal {
        if (cfg.strategy.primary == "trend")     return trend.signal();
        if (cfg.strategy.primary == "momentum")  return mom.signal();
        return vbs.signal();                      // 默认 volume_breakout
    };

    // 从策略（订单簿侧）
    OrderBookConsumer obc(cfg.order_book.pool_slots);
    OrderBookImbalanceStrategy obi;
    OrderFlowImbalanceStrategy ofi;

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

    // ── 接收线程：recv → unpacker → received++ ──
    std::thread recv_th([&]() {
        receiver->set_blocking(false);
        uint8_t pre[65536];
        receiver->recv(pre, sizeof(pre));
        receiver->set_blocking(true);
        uint8_t buf[65536];
        while (!stop.load(std::memory_order_acquire)) {
            ssize_t n = receiver->recv(buf, sizeof(buf));
            if (n > 0) {
                fc->received.fetch_add(1, std::memory_order_release);
                unpacker.feed(buf, (size_t)n);
            } else break;
        }
    });

    // ── 解析线程：通道A(成交) / 通道B(委托) ──
    std::thread parse_th([&]() {
        while (!stop.load(std::memory_order_acquire)) {
            parser.parse_available();
            if (!parser.ring().empty()) continue;
            if (!stop.load()) parser.wait_for_data(200);
        }
        parser.parse_available();
        parse_done.store(true, std::memory_order_release);
    });

    // ── 策略线程：通道A → 成交策略 + K线 → 主从组合评估 → 下单 ──
    std::atomic<OrderSide> last_order_side{OrderSide::NONE};
    std::atomic<int64_t> last_order_str{-1};
    std::thread strategy_th([&]() {
        MarketEvent ev;
        while (!parse_done.load(std::memory_order_acquire) || channel_a.pending(0) > 0) {
            if (channel_a.pop(0, ev)) {
                vbs.on_event(ev); pbs.on_event(ev); tds.on_event(ev); tms.on_event(ev);
                kagg.on_trade(ev);
                ++trade_count;

                // 组合评估：主策略(通道A)信号更新时，合成从策略(通道B)当前信号
                // 仅成交 tick 时评估，避免独立线程高频空轮询导致的重复下单
                combiner.set_primary(primary_signal());
                if (cfg.strategy.use_ofi) combiner.add_slave(ofi.signal());
                if (cfg.strategy.use_obi) combiner.add_slave(obi.signal());
                Signal decision = combiner.combine();
                combiner.clear_slaves();

                if (decision.side != OrderSide::NONE && decision.strength > 0) {
                    // 持仓封顶检查
                    uint64_t pos = rm.position(decision.locate);
                    if (decision.side == OrderSide::BUY && pos >= cfg.risk.max_position)
                        { last_order_side.store(decision.side); continue; }
                    if (decision.side == OrderSide::SELL && pos == 0)
                        { last_order_side.store(decision.side); continue; }
                    // 方向或强度显著变化才下
                    int64_t str_delta = decision.strength - last_order_str.load();
                    if (str_delta < 0) str_delta = -str_delta;
                    bool fresh = (decision.side != last_order_side.load()) ||
                                 (str_delta >= Signal::kStrengthScale / 20);
                    if (fresh) {
                        uint64_t oid = ex.submit_signal(decision, 1);
                        if (oid != 0) {
                            last_order_side.store(decision.side);
                            last_order_str.store(decision.strength);
                        }
                    }
                } else {
                    last_order_side.store(OrderSide::NONE);
                    last_order_str.store(-1);
                }
            } else if (!parse_done.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            } else {
                break;
            }
        }
        kagg.flush();
    });

    // ── 订单簿线程：通道B → 订单簿重建 → OBI/OFI ──
    std::thread book_th([&]() {
        MarketEvent ev;
        while (!parse_done.load(std::memory_order_acquire) || channel_b.pending(0) > 0) {
            if (channel_b.pop(0, ev)) {
                obc.on_event(ev);
                ++book_events;
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
                if (side != OrderSide::NONE && cfg.strategy.use_ofi) {
                    ofi.on_event(ev, side);
                    if (book && book->best_bid() >= 0 && book->best_ask() >= 0)
                        ofi.set_last_price((book->best_bid() + book->best_ask()) / 2);
                }
                if (book && cfg.strategy.use_obi) {
                    obi.on_book(ev.locate, book->best_bid(), book->best_bid_volume(),
                                book->best_ask(), book->best_ask_volume(), ev.timestamp);
                }
            } else if (!parse_done.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            } else {
                break;
            }
        }
    });

    // 组合评估已并入 strategy_th（成交 tick 时评估，避免独立线程高频空轮询）

    // ── 回报线程：收 FILL → OMS/Risk ──
    std::atomic<bool> fill_stop{false};
    std::thread fill_th([&]() {
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
    fc->ready.store(true, std::memory_order_release);

    // 运行直到回放结束：received 计数停止增长 idle_timeout_sec 秒视为数据发完。
    // （回放客户端发完行情即停，received 不再增长；Ctrl-C 也可退出）
    uint64_t last_recv = 0;
    uint64_t idle_sec = 0;
    while (idle_sec < cfg.execution.idle_timeout_sec) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        uint64_t r = fc->received.load(std::memory_order_acquire);
        if (r != last_recv) { last_recv = r; idle_sec = 0; }
        else ++idle_sec;
    }

    // ── 停止 ──
    printf("\n停止中...\n");
    stop.store(true, std::memory_order_release);
    receiver->stop();
    recv_th.join();
    parser.notify();
    parse_th.join();
    strategy_th.join();
    book_th.join();
    fill_stop.store(true, std::memory_order_release);
    fill_rcv->stop();
    fill_th.join();
    order_sender->stop();

    fc->ready.store(false, std::memory_order_release);

    // ── 汇总 ──
    printf("\n=== 运行汇总 ===\n");
    printf("成交事件:   %zu\n", trade_count.load());
    printf("委托事件:   %zu\n", book_events.load());
    printf("主策略信号: %d  (0=BUY 1=SELL 2=NONE)\n", (int)primary_signal().side);
    if (cfg.strategy.use_ofi)
        printf("OFI 累计:   %lld  信号=%d\n",
               (long long)ofi.ofi(), (int)ofi.signal().side);
    if (cfg.strategy.use_obi)
        printf("OBI 信号:   %d\n", (int)obi.signal().side);
    printf("订单:       %zu  成交=%zu 风控拒=%zu\n",
           om.order_count(),
           om.count_by_status(OrderStatus::FILLED),
           om.count_by_status(OrderStatus::REJECTED));
    printf("持仓:       %llu  已实现盈亏=%lld 分\n",
           (unsigned long long)rm.position(vbs.signal().locate),
           (long long)rm.realized_pnl());

    delete[] chan_a_slots;
    delete[] chan_b_slots;
    munmap(fc, sizeof(FlowControl));
    return 0;
}
