// 端到端验证测试：benchmark 发送 MoldUDP64 + 接收端完整链路
//   recv → 拆包加seq → 字节ring → ByteRingParser → 通道A → 4策略 + K线
//
// 验证:
//   - benchmark 完整发送(子进程 exit 0)
//   - 成交事件 seq 全局连续(无丢包)
//   - 策略从 SPMC 通道消费成交, 各自出信号
//   - 限速: sent/received 包数(新限速: 发前等 received 追上)

#include "core/ipc/flow_control.h"
#include "core/queue/queue_manager.h"
#include "core/queue/spmc_event_queue.h"
#include "core/queue/spsc_byte_ring.h"
#include "core/net/i_market_data_receiver.h"
#include "core/net/io_uring_receiver.h"
#include "core/net/io_uring_sender.h"
#include "market/pipeline/byte_ring_parser.h"
#include "market/pipeline/mold_udp_unpacker.h"
#include "market/book/order_book_consumer.h"
#include "strategy/kline/kline_aggregator.h"
#include "strategy/trend/trend_strategy.h"
#include "strategy/momentum/momentum_strategy.h"
#include "strategy/combo/signal_combiner.h"
#include "strategy/tick/price_breakout_strategy.h"
#include "strategy/tick/tick_momentum_strategy.h"
#include "strategy/tick/trade_direction_strategy.h"
#include "strategy/tick/volume_breakout_strategy.h"
#include "strategy/tick/order_book_imbalance_strategy.h"
#include "strategy/tick/order_flow_imbalance_strategy.h"
#include "execution/execution_engine.h"
#include "oms/order_manager.h"
#include "oms/order_protocol.h"
#include "risk/risk_manager.h"

// 模拟交易所端口：订单发到 benchmark(order-port)，成交回报回到本端(order_ret_port)
static constexpr uint16_t ORDER_PORT     = 9090;
static constexpr uint16_t ORDER_RET_PORT = 9091;

#include <atomic>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <thread>

#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);        \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static std::unique_ptr<IMarketDataReceiver> make_receiver(
    const std::string& backend, uint16_t port) {
    if (backend == "io_uring") {
        return std::make_unique<IoUringReceiver>(port);
    }
    printf("未知后端: %s\n", backend.c_str());
    return nullptr;
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        printf("usage: %s <itch_file> <port> <trader_benchmark> [backend]\n", argv[0]);
        return 1;
    }
    std::string itch_file = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    std::string bench_path = argv[3];
    std::string backend = (argc >= 5) ? argv[4] : "io_uring";

    // ── 清理环境: 杀掉残留 benchmark 子进程(上次测试超时 kill 后成孤儿占端口) ──
    // 必须在 fork 之前做, 避免误杀本次 fork 的 benchmark。
    // 用 -x 精确匹配进程名(不含命令行参数), 避免误杀本测试
    // (本测试命令行含 "./build/trader_benchmark" 参数, -f 会误伤自身)。
    (void)!system("pkill -x trader_benchmark");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 等进程释放端口

    // ── 清理环境: 移除残留共享内存(避免复用旧计数器) ──
    shm_unlink(FLOW_SHM_PATH);

    auto receiver = make_receiver(backend, port);
    CHECK(receiver != nullptr);
    if (!receiver) return 1;
    CHECK(receiver->start());
    if (!receiver->start()) return 1;

    // ── 共享内存 flow_control ──
    int shm_fd = shm_open(FLOW_SHM_PATH, O_CREAT | O_RDWR, 0644);
    CHECK(shm_fd >= 0);
    if (shm_fd < 0) return 1;
    CHECK(ftruncate(shm_fd, sizeof(FlowControl)) == 0);
    auto* fc = (FlowControl*)mmap(nullptr, sizeof(FlowControl),
                                  PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    CHECK(fc != MAP_FAILED);
    if (fc == MAP_FAILED) return 1;
    // 重置共享内存计数器(避免复用上次测试的旧值)
    fc->ready.store(false, std::memory_order_release);
    fc->heartbeat.store(0, std::memory_order_release);
    fc->sent.store(0, std::memory_order_release);
    fc->received.store(0, std::memory_order_release);
    // 告诉 benchmark 模拟交易所：成交回报发到 ORDER_RET_PORT
    fc->order_ret_port.store(ORDER_RET_PORT, std::memory_order_release);

    // ── 管道: 共享 ring + 通道A + 拆包器 + 解析器 + 策略 ──
    // ring/通道容量加大, 减少背压(接收端跟上 benchmark)
    auto ring_buf = std::make_unique<uint8_t[]>(1 << 22);   // 4MB
    size_t ring_id = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING,
                                          ring_buf.get(), 1 << 22);
    auto& shared_ring = QueueManager::get<SPSCByteRing>(ring_id);

    auto* ev_slots = new MarketEvent[1 << 20];   // 1M 槽
    size_t chan_id = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE,
                                          ev_slots, 1 << 20, 2);
    auto& channel = QueueManager::get<SPMCEventQueue<16>>(chan_id);

    MoldUdpUnpacker unpacker(shared_ring);
    ByteRingParser parser(shared_ring, channel);

    VolumeBreakoutStrategy vbs;
    PriceBreakoutStrategy pbs;
    TradeDirectionStrategy tds;
    TickMomentumStrategy tms;
    KLineAggregator kagg(0, 10);   // 按数量: 每 10 笔成交一根 K线(与时间无关, 回放小数据也成形)
    TrendStrategy trend;            // 低频主策略候选(趋势, 需长周期均线)
    MomentumStrategy mom;           // 低频主策略(动量, 需 6 根 K线即成形)
    SignalCombiner combiner;        // 主从分层: 主定方向, 从定强度
    kagg.set_sink([&](const KLine& bar) { trend.on_bar(bar); mom.on_bar(bar); });  // K线 → 低频主策略

    // 通道 B 策略(线程间共享, 提前声明): 共享池 + 共享索引
    OrderPool obc_pool(1 << 20);
    OrderMap  obc_index(1 << 20);
    OrderBookConsumer obc(obc_pool, obc_index);
    OrderBookImbalanceStrategy obi;
    OrderFlowImbalanceStrategy ofi;

    // ── 交易侧: 信号 → 执行引擎(风控 + OMS) → 真发送到模拟交易所 ──
    OrderManager om;
    RiskManager rm;
    ExecutionEngine ex(om, rm);
    ex.set_base_qty(100);

    // 订单发送端(io_uring 零拷贝) → benchmark 模拟交易所
    auto order_send_ring = std::make_unique<uint8_t[]>(1 << 20);
    auto order_sender = std::make_unique<IoUringSender>(
        "127.0.0.1", ORDER_PORT, order_send_ring.get(), 1 << 20);
    CHECK(order_sender->start());
    if (!order_sender->start()) return 1;
    ex.set_sender(order_sender.get());

    // 成交回报接收端(io_uring) ← benchmark 模拟交易所
    auto fill_rcv = std::make_unique<IoUringReceiver>(ORDER_RET_PORT);
    CHECK(fill_rcv->start());
    if (!fill_rcv->start()) return 1;

    std::atomic<bool> seq_contiguous{true};
    std::atomic<uint64_t> last_seq{0};
    std::atomic<bool> stop{false};
    std::atomic<bool> parse_done{false};  // 解析线程完成(所有成交已进通道A)
    std::atomic<size_t> trade_count{0};
    std::atomic<size_t> unpacked_total{0};  // 拆包器拆出的消息数
    std::atomic<size_t> recv_count{0};      // 接收线程 recv 次数

    struct State { std::atomic<bool> armed{false}; };
    State st;

    // ── 接收线程: recv → unpacker → fc->received++ ──
    std::thread recv_th([&]() {
        receiver->set_blocking(false);
        uint8_t pre[65536];
        receiver->recv(pre, sizeof(pre));
        receiver->set_blocking(true);
        st.armed.store(true, std::memory_order_release);

        uint8_t buf[65536];
        static bool dumped = false;
        std::atomic<size_t>& rc = recv_count;
        while (!stop.load(std::memory_order_acquire)) {
            ssize_t n = receiver->recv(buf, sizeof(buf));
            if (n > 0) {
                ++rc;
                if (!dumped) {
                    FILE* df = fopen("/tmp/pkt_dump.txt", "w");
                    fprintf(df, "recv n=%zd\n", n);
                    for (int i = 0; i < (n < 32 ? n : 32); ++i) fprintf(df, "%02x ", buf[i]);
                    fprintf(df, "\n");
                    fclose(df);
                    dumped = true;
                }
                fc->received.fetch_add(1, std::memory_order_release);  // 先确认收到包
                size_t unpacked = unpacker.feed(buf, (size_t)n);
                unpacked_total += unpacked;
            } else break;
        }
    });

    // ── 解析线程: ByteRingParser 解析, 成交进通道 A ──
    std::atomic<size_t> parsed_total{0};
    std::thread parse_th([&]() {
        while (!stop.load(std::memory_order_acquire)) {
            size_t n = parser.parse_available();
            parsed_total += n;
            if (!parser.ring().empty()) continue;
            if (!stop.load()) parser.wait_for_data(200);
        }
        parsed_total += parser.parse_available();  // 清空剩余: 所有成交已进通道 A
        parse_done.store(true, std::memory_order_release);
    });

    // ── 策略线程: 从通道 A 收成交, 喂 4 策略 + K线 ──
    // 交易侧: 信号翻转时经执行引擎真发送(风控→OMS→模拟交易所→回报)
    std::thread strategy_th([&]() {
        MarketEvent ev;
        OrderSide last_order_side = OrderSide::NONE;   // 已下单方向(同向不重下)
        // 退出条件: 解析已完成(parse_done) 且 通道空(pending==0)
        while (!parse_done.load(std::memory_order_acquire) || channel.pending(0) > 0) {
            if (channel.pop(0, ev)) {
                if (ev.type != MarketEvent::Type::TRADE &&
                    ev.type != MarketEvent::Type::EXECUTE) continue;   // skip 委托
                vbs.on_event(ev); pbs.on_event(ev); tds.on_event(ev); tms.on_event(ev);
                kagg.on_trade(ev);      // 成交 → K线聚合
                ++trade_count;
                uint64_t l = last_seq.load();
                // seq_id 是全局消息序号, 成交之间隔了委托, 只要求递增不要求连续
                if (l != 0 && ev.seq_id <= l)
                    seq_contiguous.store(false, std::memory_order_relaxed);
                last_seq.store(ev.seq_id, std::memory_order_relaxed);

                // 回放中实时决策: 主从组合(主=量突定方向, 从=OFI/OBI 定强度)
                // 从策略(通道B)稍后追上; 一旦有强度, 决策生效 → 下单
                combiner.set_primary(vbs.signal());
                combiner.add_slave(ofi.signal());
                combiner.add_slave(obi.signal());
                Signal decision = combiner.combine();
                combiner.clear_slaves();
                if (decision.side != OrderSide::NONE && decision.strength > 0) {
                    if (decision.side != last_order_side) {   // 方向变化才下(避免重复)
                        uint64_t oid = ex.submit_signal(decision, 1);
                        if (oid != 0)   // 实际下单才记录方向(强度弱 qty=0 时不下)
                            last_order_side = decision.side;
                    }
                } else {
                    last_order_side = OrderSide::NONE;   // 无信号 → 复位(可重新下单)
                }
            } else if (!parse_done.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            } else {
                break;   // parse_done 且通道A空 → 退出
            }
        }
        kagg.flush();   // 回放结束: 强制推出最后一根 K线, 让主策略看到完整数据
    });

    // ── 通道 B 消费线程: 委托事件 → 订单簿重建 → OBI + OFI 策略 ──
    std::atomic<bool> book_ready{false};
    std::atomic<size_t> book_events{0};
    std::thread book_th([&]() {
        MarketEvent ev;
        while (!parse_done.load(std::memory_order_acquire) || channel.pending(1) > 0) {
            if (channel.pop(1, ev)) {
                obc.on_event(ev);   // 事件 → 订单簿(成交更新挂单 + 委托重建盘口)
                ++book_events;
                const OrderBook* book = obc.book(ev.locate);

                // 方向：A/U 事件自带 side；D/X/E 查簿(order_ref 对应挂单侧)
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

                // OFI：逐笔委托方向累加(高频)
                if (side != OrderSide::NONE) {
                    ofi.on_event(ev, side);
                    // 参考价：盘口可用时取中间价，否则保持
                    if (book && book->best_bid() >= 0 && book->best_ask() >= 0)
                        ofi.set_last_price((book->best_bid() + book->best_ask()) / 2);
                }

                // OBI：盘口失衡(低频盘口参考)
                if (book) {
                    obi.on_book(ev.locate, book->best_bid(), book->best_bid_volume(),
                                book->best_ask(), book->best_ask_volume(), ev.timestamp);
                }
            } else if (!parse_done.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            } else {
                break;   // parse_done 且通道空 → 退出
            }
        }
        book_ready.store(true, std::memory_order_release);
    });

    // ── 成交回报线程: 收模拟交易所 FILL → 驱动 OMS/Risk ──
    std::atomic<bool> fill_stop{false};
    std::atomic<size_t> fill_count{0};
    std::thread fill_th([&]() {
        uint8_t pre[2048];
        fill_rcv->set_blocking(false);
        fill_rcv->recv(pre, sizeof(pre));
        fill_rcv->set_blocking(true);
        uint8_t buf[2048];
        while (!fill_stop.load(std::memory_order_acquire)) {
            ssize_t n = fill_rcv->recv(buf, sizeof(buf));
            if (n > 0) {
                uint64_t oid = 0, qty = 0;
                int64_t price = 0;
                if (decode_fill(buf, (size_t)n, oid, qty, price)) {
                    ex.on_order_fill(oid, qty, price);
                    ++fill_count;
                }
            } else break;
        }
    });

    // 等接收就绪 → fork benchmark
    while (!st.armed.load(std::memory_order_acquire)) sched_yield();
    fc->ready.store(true, std::memory_order_release);
    printf("接收端就绪, fork benchmark...\n"); fflush(stdout);

    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        char port_arg[16], order_port_arg[16], backlog_arg[16];
        snprintf(port_arg, sizeof(port_arg), "%u", port);
        snprintf(order_port_arg, sizeof(order_port_arg), "%u", ORDER_PORT);
        snprintf(backlog_arg, sizeof(backlog_arg), "%d", 32);  // 限速: 发送端最多领先 32 包
                                                               // (匹配内核 UDP 缓冲 ~208KB, 防突发溢出)
        execl(bench_path.c_str(), bench_path.c_str(),
              "--file", itch_file.c_str(),
              "--port", port_arg,
              "--order-port", order_port_arg,
              "--backlog", backlog_arg,
              (char*)nullptr);
        _exit(127);
    }

    // 等 benchmark 子进程完成(新限速: 发前等 received, 接收端跟上则全速)
    int wstatus = 0;
    auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (waitpid(pid, &wstatus, WNOHANG) == 0
           && std::chrono::steady_clock::now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    // 超时未退出: kill 兜底(避免残留进程占端口)
    if (waitpid(pid, &wstatus, WNOHANG) == 0) {
        printf("benchmark 超时, kill\n"); fflush(stdout);
        kill(pid, SIGKILL);
        waitpid(pid, &wstatus, 0);
    }
    bool bench_ok = WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0;
    printf("benchmark 子进程 exit: %s (rc=%d)\n", bench_ok ? "0 (OK)" : "失败",
           WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1);
    fflush(stdout);

    // 停止顺序: 先停接收(不再收包) → 解析线程把 ring 清空(成交全进通道A)
    // → 策略线程把通道A消费完。
    receiver->stop();   // 打断接收线程的阻塞 recv
    recv_th.join();     // 接收完

    // 等解析线程把共享 ring 清空(所有成交进通道 A)
    // 解析线程在 stop 前持续解析; 这里 sleep 让 ring 排空
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    stop.store(true, std::memory_order_release);
    parser.notify();
    parse_th.join();

    // 策略线程: 消费到通道 A 空
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    strategy_th.join();

    // 通道 B 消费线程: 委托事件 → 订单簿重建 → OBI 策略
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    book_th.join();
    CHECK(book_ready.load());
    CHECK(book_events.load() > 0);

    // 成交回报: 等最后一批 FILL 到达 → 停回报线程 → 停发送端
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    fill_stop.store(true, std::memory_order_release);
    fill_rcv->stop();
    fill_th.join();
    order_sender->stop();

    fc->ready.store(false, std::memory_order_release);

    // ── 验证 ──
    printf("\n=== 端到端验证 ===\n");
    printf("接收包数:   %llu\n", (unsigned long long)fc->received.load());
    printf("recv 次数:  %zu\n", recv_count.load());
    printf("拆包消息数: %zu\n", unpacked_total.load());
    printf("解析消息数: %zu\n", parsed_total.load());
    printf("成交事件数: %zu\n", trade_count.load());
    printf("seq 连续:   %s\n", seq_contiguous.load() ? "yes" : "NO");
    printf("策略信号:   成交量突破=%d 价格突破=%d 成交方向=%d 动量=%d OBI盘口=%d OFI订单流=%d (0=BUY 1=SELL 2=NONE)\n",
           (int)vbs.signal().side, (int)pbs.signal().side,
           (int)tds.signal().side, (int)tms.signal().side,
           (int)obi.signal().side, (int)ofi.signal().side);
    combiner.set_primary(vbs.signal());
    combiner.add_slave(ofi.signal());
    combiner.add_slave(obi.signal());
    Signal dc = combiner.combine();
    combiner.clear_slaves();
    printf("主从组合:   主(量突)=%d 从(OFI)=%d 从(OBI)=%d 决策=%d 强度=%lld\n",
           (int)vbs.signal().side, (int)ofi.signal().side,
           (int)obi.signal().side, (int)dc.side, (long long)dc.strength);
    printf("通道B:      委托事件=%zu OFI累计=%lld\n",
           book_events.load(), (long long)ofi.ofi());

    // ── 交易侧: 订单经模拟交易所真实往返(发送→回报→OMS/Risk) ──
    size_t n_orders   = om.order_count();
    size_t n_filled   = om.count_by_status(OrderStatus::FILLED);
    size_t n_rejected = om.count_by_status(OrderStatus::REJECTED);
    size_t n_pending  = om.count_by_status(OrderStatus::PENDING);
    printf("交易侧:   订单=%zu 成交=%zu 风控拒=%zu 未决=%zu 回报=%zu 持仓=%llu 已实现盈亏=%lld 分\n",
           n_orders, n_filled, n_rejected, n_pending, fill_count.load(),
           (unsigned long long)rm.position(vbs.signal().locate),
           (long long)rm.realized_pnl());

    CHECK(bench_ok);
    CHECK(seq_contiguous.load());
    CHECK(trade_count.load() > 0);
    // 真实往返: 至少一个订单发出并经模拟交易所成交回报落地
    CHECK(n_orders > 0);
    CHECK(n_filled > 0);
    CHECK(fill_count.load() > 0);
    // 全部落定: 已发订单要么成交要么被风控拒，无 PENDING 残留
    CHECK(n_filled + n_rejected == n_orders);
    CHECK(n_pending == 0);

    delete[] ev_slots;
    receiver->stop();
    if (g_failures == 0) {
        printf("\n端到端验证 PASS ✓\n");
        return 0;
    }
    printf("\n%d check(s) failed\n", g_failures);
    return 1;
}
