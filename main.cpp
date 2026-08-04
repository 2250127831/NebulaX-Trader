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
#include "strategy/tick/trade_direction_strategy.h"

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
    printf("Usage: %s [--config <path>] [--no-shm]\n"
           "  --no-shm   不挂共享内存(压测脚本模式): 无握手, 运行 idle_timeout_sec 后退出\n",
           prog);
}

int main(int argc, char* argv[]) {
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
    auto ring_buf = std::make_unique<uint8_t[]>(cfg.market.ring_bytes);
    size_t ring_id = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING,
                                          ring_buf.get(), cfg.market.ring_bytes);
    auto& shared_ring = QueueManager::get<SPSCByteRing>(ring_id);

    // 单通道: 全部事件(成交+委托)进同一队列, 多消费者(SPMC 广播)。
    // 消费者0 = book_th(订单簿+OFI/OBI+仲裁), 消费者1 = strategy_th(独立高频策略)。
    // 同一序列 → 订单簿时序正确(成交不会先于对应委托处理)。
    auto* chan_slots = new MarketEvent[cfg.market.chan_slots];
    size_t chan_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                          chan_slots, cfg.market.chan_slots, 2);
    auto& channel = QueueManager::get<SPMCEventQueue<16>>(chan_id);

    MoldUdpUnpacker unpacker(shared_ring);
    ByteRingParser parser(shared_ring, channel);

    // ── 高频策略(订单簿侧): 共享挂单池 + 共享索引(主线程全局, 所有订单簿引用) ──
    // 独立信号 + 统一仲裁: OFI(逐笔委托) + OBI(盘口) + TradeDirection(成交) 各产信号,
    // arbitrate() 读三信号槽, 同向才下单。
    OrderPool shared_pool(cfg.order_book.pool_slots);
    OrderMap  shared_index(cfg.order_book.pool_slots);
    OrderBookConsumer obc(shared_pool, shared_index);
    OrderBookImbalanceStrategy obi;
    OrderFlowImbalanceStrategy ofi;
    TradeDirectionStrategy tds;   // 独立高频策略(消费成交, 不依赖订单簿)

    // ── 信号槽: 各策略线程写, arbitrate() 读(跨线程原子) ──
    // 每个信号拆成 side/locate/strength 原子, 避免整 Signal 非原子的竞争。
    struct SignalSlot {
        alignas(64) std::atomic<OrderSide> side{OrderSide::NONE};
        alignas(64) std::atomic<uint64_t> locate{0};
        alignas(64) std::atomic<int64_t> strength{0};
    } sig_ofi, sig_obi, sig_td;   // OFI/OBI(book_th 写) + TD(strategy_th 写)

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

    // ── 统一仲裁(共享函数): 读三信号槽, 同向才下 ──
    // 谁写信号谁调用(写完检查信号是否齐全, 齐则仲裁下单)。
    // 无定时器无独立线程——信号一更新同步仲裁, TD 后写也能触发。
    std::atomic<OrderSide> last_order_side{OrderSide::NONE};
    std::atomic<int64_t> last_order_str{-1};
    auto arbitrate = [&]() {
        OrderSide so = sig_ofi.side.load(std::memory_order_acquire);
        OrderSide sb = sig_obi.side.load(std::memory_order_acquire);
        OrderSide st = sig_td.side.load(std::memory_order_acquire);
        // 仲裁: TD 有方向时要求三同向; TD 无方向(NONE, E 多无主动方)退化为 OFI/OBI 两信号。
        bool ok;
        if (st != OrderSide::NONE) ok = (so != OrderSide::NONE && so == sb && so == st);
        else                      ok = (so != OrderSide::NONE && so == sb);
        if (!ok) return;
        // 用 OFI 方向(方向一致时任一方向都成立, 取 OFI)
        uint64_t locate = sig_ofi.locate.load(std::memory_order_acquire);
        int64_t strength = sig_ofi.strength.load(std::memory_order_acquire);
        uint64_t pos = rm.position(locate);
        if (so == OrderSide::BUY && pos >= cfg.risk.max_position) return;
        if (so == OrderSide::SELL && pos == 0) return;
        bool fresh = (so != last_order_side.load());
        if (fresh) {
            Signal decision{.side = so, .locate = locate,
                            .price = 0, .timestamp = 0, .strength = strength};
            lensx::mark_s5(0);   // [LensX s5] 下单决策
            uint64_t oid = ex.submit_signal(decision, 1);
            lensx::mark_s6(0);   // [LensX s6] 订单发出
            if (oid != 0) {
                last_order_side.store(so);
                last_order_str.store(strength);
            }
        }
    };

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
                if (fc) fc->received.fetch_add(1, std::memory_order_release);
                unpacker.feed(buf, (size_t)n);
                parser.notify();   // 新数据已写入 ring → 立即唤醒可能阻塞在 poll 的解析线程
            } else break;
        }
    });

    // 消费者混合退避参数: 短暂空自旋顶住唤醒延迟, 持续空才 eventfd 阻塞。
    // parse_th/strategy_th/book_th 共用(kSpinMax ~ 几十µs)。
    constexpr int kSpinMax = 2000;

    // ── 解析线程：解析 ITCH → 单通道广播(方案A, 全部事件) ──
    // 回放结束 = shm 模式(done) / no-shm 模式(stop，由主线程定时置)。排空 ring 后置 parse_done。
    std::thread parse_th([&]() {
        int spin_left = 0;   // 混合退避: 短暂空自旋顶住唤醒延迟, 持续空才阻塞
        while ((fc && !fc->done.load(std::memory_order_acquire)) || (!fc && !stop.load())) {
            parser.parse_available();
            if (!parser.ring().empty()) { spin_left = kSpinMax; continue; }
            if (spin_left > 0) { --spin_left; _mm_pause(); continue; }   // 短自旋顶唤醒延迟
            spin_left = kSpinMax;
            parser.wait_for_data(200);   // 持续空: 阻塞等 recv_th notify
        }
        parser.parse_available();   // 排空剩余
        parse_done.store(true, std::memory_order_release);
    });

    // ── 独立高频策略线程(consumer 1): 消费成交跑 TradeDirection, 产信号(不下单) ──
    // 验证 SPMC 多消费者广播: 独立于 book_th 并行消费单通道成交, 不依赖订单簿。
    std::thread strategy_th([&]() {
        MarketEvent ev;
        int spin_left = 0;
        while (!parse_done.load(std::memory_order_acquire) || channel.pending(1) > 0) {
            if (channel.pending(1) == 0) {
                if (!parse_done.load(std::memory_order_acquire)) {
                    if (spin_left > 0) { --spin_left; _mm_pause(); continue; }
                    spin_left = kSpinMax;
                    channel.wait_for_data(200);
                    continue;
                }
                break;
            }
            spin_left = kSpinMax;
            while (channel.pop(1, ev)) {
                if (ev.type != MarketEvent::Type::TRADE &&
                    ev.type != MarketEvent::Type::EXECUTE) continue;   // 只处理成交
                tds.on_event(ev);   // TradeDirection: 独立信号(不依赖订单簿)
                // 写信号槽, 检查是否齐 → 仲裁下单(TD 后写也能触发)
                Signal s = tds.signal();
                sig_td.side.store(s.side, std::memory_order_release);
                sig_td.locate.store(s.locate, std::memory_order_release);
                sig_td.strength.store(s.strength, std::memory_order_release);
                arbitrate();
            }
        }
    });

    // ── 订单簿线程(consumer 0): 单通道全部事件 → 订单簿重建 → OFI/OBI 信号 → 写信号槽 ──
    std::thread book_th([&]() {
        MarketEvent ev;
        // 排空 + 混合退避: 有数据连续 pop 到空(保吞吐); 短暂空自旋(_mm_pause)顶住
        // 唤醒延迟; 持续空(自旋耗尽)才 wait_for_data 阻塞(省CPU)。
        // 平衡高频吞吐(不牺牲)与空闲省CPU(不空转)。
        int spin_left = 0;   // 剩余自旋次数(每空一轮减一, 耗完才阻塞)
        while (!parse_done.load(std::memory_order_acquire) || channel.pending(0) > 0) {
            if (channel.pending(0) == 0) {
                if (!parse_done.load(std::memory_order_acquire)) {
                    if (spin_left > 0) { --spin_left; _mm_pause(); continue; }   // 短自旋顶唤醒延迟
                    spin_left = kSpinMax;
                    channel.wait_for_data(200);   // 持续空: 阻塞等 push 唤醒
                    continue;
                }
                break;
            }
            spin_left = kSpinMax;   // 有数据: 重置自旋预算
            while (channel.pop(0, ev)) {
                // [LensX s2] 只对成交打点(端到端: 成交→仲裁→下单→send 同 seq 配对)。
                bool is_trade = (ev.type == MarketEvent::Type::TRADE ||
                                 ev.type == MarketEvent::Type::EXECUTE);
                if (is_trade) {
                    lensx::mark_s2(ev.seq_id);
                    ++trade_count;
                }
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
                    lensx::mark_book(ev.seq_id);   // [LensX 四类起点] 会更新OFI的委托
                    ofi.on_event(ev, side);
                    lensx::mark_ofi(ev.seq_id);   // [LensX 四类终点] OFI 信号更新完成
                    if (book && book->best_bid() >= 0 && book->best_ask() >= 0)
                        ofi.set_last_price((book->best_bid() + book->best_ask()) / 2);
                }
                if (book && cfg.strategy.use_obi) {
                    lensx::mark_book(ev.seq_id);   // [LensX 三类起点] 会更新OBI的委托
                    obi.on_book(ev.locate, book->best_bid(), book->best_bid_volume(),
                                book->best_ask(), book->best_ask_volume(), ev.timestamp);
                    lensx::mark_obi(ev.seq_id);   // [LensX 三类终点] OBI 信号更新完成
                }

                // 更新信号槽(arbitrate 读): OFI/OBI 信号 → 原子槽
                if (cfg.strategy.use_ofi) {
                    Signal s = ofi.signal();
                    sig_ofi.side.store(s.side, std::memory_order_release);
                    sig_ofi.locate.store(s.locate, std::memory_order_release);
                    sig_ofi.strength.store(s.strength, std::memory_order_release);
                }
                if (cfg.strategy.use_obi) {
                    Signal s = obi.signal();
                    sig_obi.side.store(s.side, std::memory_order_release);
                    sig_obi.locate.store(s.locate, std::memory_order_release);
                    sig_obi.strength.store(s.strength, std::memory_order_release);
                }
                arbitrate();   // 写完 OFI/OBI 信号, 检查是否齐 → 仲裁下单
            }
        }
    });

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
        uint64_t parsed = parser.message_count();
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
            uint64_t p = parser.message_count();
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
    parser.notify();
    parse_th.join();
    channel.notify_all();   // 唤醒阻塞在 wait_for_data 的消费者(strategy_th/book_th)
    strategy_th.join();
    book_th.join();
    fill_stop.store(true, std::memory_order_release);
    fill_rcv->stop();
    fill_th.join();
    order_sender->stop();

    if (fc) fc->ready.store(false, std::memory_order_release);

    // ── 汇总 ──
    printf("\n=== 运行汇总 ===\n");
    printf("成交事件:   %zu  单通道丢=%llu\n", trade_count.load(), (unsigned long long)parser.drops_a());
    printf("事件处理:   %zu  单通道丢=%llu\n", book_events.load(), (unsigned long long)parser.drops_b());
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
           (unsigned long long)rm.position(ofi.signal().locate),
           (long long)rm.realized_pnl());

    delete[] chan_slots;
    if (fc) munmap(fc, sizeof(FlowControl));
    return 0;
}
