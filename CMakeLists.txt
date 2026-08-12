cmake_minimum_required(VERSION 3.20)
project(NebulaX-Trader VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── 外部依赖 ──
find_package(yaml-cpp REQUIRED)

# io_uring（照搬 NebulaX 配置：pkg-config，静态优先）
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)
if(LIBURING_STATIC_LIBRARIES)
    set(URING_LIBS ${LIBURING_STATIC_LIBRARIES})
else()
    set(URING_LIBS ${LIBURING_LIBRARIES})
endif()

# libbpf（V4 AF_XDP 后端；xsk 绑定 + 默认 XDP 程序重定向）
# 可选: CI/无特权 runner 不装也能构建(io_uring 默认后端), 有 libbpf 才编译 AF_XDP。
pkg_check_modules(LIBBPF QUIET libbpf)

# DPDK（V4 DPDK 后端；net_tap/net_af_packet vdev 软件 PMD，可选）
pkg_check_modules(LIBDPDK QUIET libdpdk)

# ── 编译选项 ──
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -march=native -mtune=native)
endif()

option(SANITIZE_ADDRESS "Enable AddressSanitizer" OFF)
if(SANITIZE_ADDRESS)
    add_compile_options(-O1 -fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
else()
    add_compile_options(-O2)
endif()

# ── 核心基础库 ──
add_library(core
    core/types.h
    core/config.h
    core/config_loader.cpp
    core/dispatch/dispatcher.h
    core/queue/spsc_byte_ring.h
    core/queue/spsc_event_ring.h
    core/queue/spmc_byte_ring.h
    core/queue/spmc_ring.h
    core/queue/spmc_event_queue.h
    core/queue/queue_manager.h
    core/market_event.h
    core/memory/memory_pool.h
    core/memory/order_pool.h
    core/memory/order_map.h
    core/prof/lensx_probe.h
    core/prof/lensx_probe.cpp
    core/utils/align.h
    core/net/i_market_data_receiver.h
    core/net/i_market_data_sender.h
    core/net/io_uring_poller.h
    core/net/io_uring_receiver.h
    core/net/io_uring_receiver.cpp
    core/net/io_uring_sender.h
    core/net/io_uring_sender.cpp
    core/net/frame_util.h
    core/ipc/flow_control.h
)

# V4 网络后端: libbpf(AF_XDP) / DPDK 可选, 找到才编译对应 receiver。
# CI/无特权 runner 只编译 io_uring(默认后端), 不装 libbpf/dpdk 也能过。
if(LIBBPF_FOUND)
    target_sources(core PRIVATE core/net/af_xdp_receiver.h core/net/af_xdp_receiver.cpp)
    target_compile_definitions(core PUBLIC HAVE_AF_XDP=1)
endif()
if(LIBDPDK_FOUND)
    target_sources(core PRIVATE core/net/dpdk_receiver.h core/net/dpdk_receiver.cpp)
    target_compile_definitions(core PUBLIC HAVE_DPDK=1)
endif()

target_include_directories(core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(core PUBLIC yaml-cpp ${URING_LIBS})
target_include_directories(core PUBLIC ${LIBURING_INCLUDE_DIRS})
if(LIBBPF_FOUND)
    target_link_libraries(core PUBLIC ${LIBBPF_LIBRARIES})
    target_include_directories(core PUBLIC ${LIBBPF_INCLUDE_DIRS})
endif()
if(LIBDPDK_FOUND)
    target_link_libraries(core PUBLIC ${LIBDPDK_LIBRARIES})
    target_include_directories(core PUBLIC ${LIBDPDK_INCLUDE_DIRS})
endif()

# ── 行情模块 ──
add_library(market
    market/gateway/market_data_gateway.h
    market/parser/tick_parser.h
    market/parser/itch_parser.h
    market/parser/itch_parser.cpp
    market/book/order_book.h
    market/book/order_book_consumer.h
    market/book/order_book_consumer.cpp
    market/pipeline/byte_ring_parser.h
    market/pipeline/mold_udp_unpacker.h
)

target_link_libraries(market PUBLIC core)
target_include_directories(market PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── 策略模块 ──
add_library(strategy
    strategy/base/strategy.h
    strategy/base/signal.h
    strategy/combo/signal_combiner.h
    strategy/momentum/momentum_strategy.h
    strategy/trend/trend_strategy.h
    strategy/kline/kline_aggregator.h
    strategy/kline/kline_consumer.h
    strategy/tick/volume_breakout_strategy.h
    strategy/tick/price_breakout_strategy.h
    strategy/tick/tick_momentum_strategy.h
    strategy/tick/order_book_imbalance_strategy.h
    strategy/tick/order_flow_imbalance_strategy.h
)

target_link_libraries(strategy PUBLIC core)
target_include_directories(strategy PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── OMS ──
add_library(oms
    oms/order.h
    oms/order_manager.h
    oms/i_order_codec.h
    oms/custom_order_codec.h
    oms/ouch_order_codec.h
)

target_link_libraries(oms PUBLIC core)
target_include_directories(oms PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── 风控 ──
add_library(risk
    risk/risk_manager.h
)

target_link_libraries(risk PUBLIC core)
target_include_directories(risk PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── 执行引擎 ──
add_library(execution
    execution/execution_engine.h
)

target_link_libraries(execution PUBLIC core oms risk)
target_include_directories(execution PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── 主程序入口 ──
add_executable(trader
    main.cpp
)

target_link_libraries(trader PRIVATE core market strategy oms risk execution)

# ── 基准测试 ──
add_executable(trader_benchmark
    benchmark/main.cpp
)

target_link_libraries(trader_benchmark PRIVATE core market strategy oms risk execution)

# ── 单元测试 ──
enable_testing()

add_executable(ut_spsc
    tests/unit/test_spsc.cpp
)
target_link_libraries(ut_spsc PRIVATE core)
add_test(NAME ut_spsc COMMAND ut_spsc)

# SPSCEventRing(V3 分发器下游): 单生产者单消费者定长槽事件环形队列
add_executable(ut_spsc_event_ring
    tests/unit/test_spsc_event_ring.cpp
)
target_link_libraries(ut_spsc_event_ring PRIVATE core)
add_test(NAME ut_spsc_event_ring COMMAND ut_spsc_event_ring)

# Dispatcher(V3 分发器): 按 locate 分发 + retry 桶 + 保序
add_executable(ut_dispatcher
    tests/unit/test_dispatcher.cpp
)
target_link_libraries(ut_dispatcher PRIVATE core)
add_test(NAME ut_dispatcher COMMAND ut_dispatcher)

# 并发 retry 端到端: 小 SPSC 强制满 → retry 桶 → 推回 → 消费, 验证零丢保序
add_executable(ut_retry_flow
    tests/unit/test_retry_flow.cpp
)
target_link_libraries(ut_retry_flow PRIVATE core)
add_test(NAME ut_retry_flow COMMAND ut_retry_flow)

add_executable(ut_spmc
    tests/unit/test_spmc.cpp
)
target_link_libraries(ut_spmc PRIVATE core)
add_test(NAME ut_spmc COMMAND ut_spmc)

add_executable(ut_spmc_event_queue
    tests/unit/test_spmc_event_queue.cpp
)
target_link_libraries(ut_spmc_event_queue PRIVATE core)
add_test(NAME ut_spmc_event_queue COMMAND ut_spmc_event_queue)

add_executable(ut_kline_strategies
    tests/unit/test_kline_strategies.cpp
)
target_link_libraries(ut_kline_strategies PRIVATE core strategy)
add_test(NAME ut_kline_strategies COMMAND ut_kline_strategies)

add_executable(ut_signal_strength
    tests/unit/test_signal_strength.cpp
)
target_link_libraries(ut_signal_strength PRIVATE core strategy)
add_test(NAME ut_signal_strength COMMAND ut_signal_strength)

add_executable(ut_order_book
    tests/unit/test_order_book.cpp
)
target_link_libraries(ut_order_book PRIVATE core market)
add_test(NAME ut_order_book COMMAND ut_order_book)

# OrderMap/OrderPool 并发压力测试(分簿语义: 每线程操作自己的 key 集 + 跨线程同桶冲突)
add_executable(ut_concurrent_order_map
    tests/unit/test_concurrent_order_map.cpp
)
target_link_libraries(ut_concurrent_order_map PRIVATE core)
add_test(NAME ut_concurrent_order_map COMMAND ut_concurrent_order_map)

add_executable(ut_ofi
    tests/unit/test_ofi.cpp
)
target_link_libraries(ut_ofi PRIVATE core strategy)
add_test(NAME ut_ofi COMMAND ut_ofi)

add_executable(ut_signal_combiner
    tests/unit/test_signal_combiner.cpp
)
target_link_libraries(ut_signal_combiner PRIVATE core strategy)
add_test(NAME ut_signal_combiner COMMAND ut_signal_combiner)

add_executable(ut_execution
    tests/unit/test_execution.cpp
)
target_link_libraries(ut_execution PRIVATE core oms risk execution strategy)
add_test(NAME ut_execution COMMAND ut_execution)

# 订单协议 codec(V5 解耦): IOrderCodec 接口 + CustomOrderCodec 编解码往返/校验
add_executable(ut_order_codec
    tests/unit/test_order_codec.cpp
)
target_link_libraries(ut_order_codec PRIVATE core oms)
add_test(NAME ut_order_codec COMMAND ut_order_codec)

# OUCH 4.2 codec(V5 实盘协议化): token↔order_id 映射 + 'O'/'A'/'E' 编解码
add_executable(ut_ouch_codec
    tests/unit/test_ouch_codec.cpp
)
target_link_libraries(ut_ouch_codec PRIVATE core oms)
add_test(NAME ut_ouch_codec COMMAND ut_ouch_codec)

# OUCH 回报语义枚举(V5): A/E/C/J 各类型驱动 OMS 状态机(半成交/撤单/拒单/非法流转)
add_executable(ut_ouch_semantics
    tests/unit/test_ouch_semantics.cpp
)
target_link_libraries(ut_ouch_semantics PRIVATE core oms risk execution)
add_test(NAME ut_ouch_semantics COMMAND ut_ouch_semantics)

# 委托簿(V5): 自有订单完整生命周期状态机 + symbol/strategy 索引查询 + 撤单链路
# (注意: 与市场订单簿 test_order_book.cpp 不同概念, 这是自有订单跟踪)
add_executable(ut_order_book_own
    tests/unit/test_order_book_own.cpp
)
target_link_libraries(ut_order_book_own PRIVATE core oms)
add_test(NAME ut_order_book_own COMMAND ut_order_book_own)

# 盯市回撤风控(V5): 盯市净值/峰值/分档触发(暂停新单/强制平仓)
add_executable(ut_drawdown
    tests/unit/test_drawdown.cpp
)
target_link_libraries(ut_drawdown PRIVATE core oms risk execution strategy)
add_test(NAME ut_drawdown COMMAND ut_drawdown)

# SPMC 多消费者广播: 多个策略从通道 A 消费同一批成交
add_executable(ut_spmc_strategies
    tests/integration/test_spmc_strategies.cpp
)
target_link_libraries(ut_spmc_strategies PRIVATE core strategy)
add_test(NAME ut_spmc_strategies COMMAND ut_spmc_strategies)

add_executable(ut_itch_parser
    tests/unit/test_itch_parser.cpp
)
target_link_libraries(ut_itch_parser PRIVATE core market)
add_test(NAME ut_itch_parser
    COMMAND ut_itch_parser ${CMAKE_CURRENT_SOURCE_DIR}/test_data/itch_parser_sample.bin)

# 字节 ring 管道：连续字节流 → 原地解析 → 事件
add_executable(ut_byte_ring_pipeline
    tests/integration/test_byte_ring_pipeline.cpp
)
target_link_libraries(ut_byte_ring_pipeline PRIVATE core market)
add_test(NAME ut_byte_ring_pipeline
    COMMAND ut_byte_ring_pipeline ${CMAKE_CURRENT_SOURCE_DIR}/test_data/itch_parser_sample.bin)

# ── 集成测试 ──
add_executable(it_tick_pipeline
    tests/integration/test_tick_pipeline.cpp
)
target_link_libraries(it_tick_pipeline PRIVATE core market strategy)
add_test(NAME it_tick_pipeline COMMAND it_tick_pipeline)

add_executable(ut_uring
    tests/integration/test_io_uring.cpp
)
target_link_libraries(ut_uring PRIVATE core)
add_test(NAME ut_uring COMMAND ut_uring)

# 端到端验证：benchmark 发送 + IoUringReceiver 接收逐字节比对
add_executable(ut_benchmark_verify
    tests/integration/test_benchmark_verify.cpp
)
target_link_libraries(ut_benchmark_verify PRIVATE core market strategy)
add_test(NAME ut_benchmark_verify
    COMMAND ut_benchmark_verify
        ${CMAKE_CURRENT_SOURCE_DIR}/test_data/itch_chain_sample.bin
        8080
        $<TARGET_FILE:trader_benchmark>)
# 端到端集成测试: fork benchmark + loopback UDP + io_uring, 对无权限 runner 敏感
# (CI GitHub runner 上卡死无输出)。标 ci-skip → CI 用 -LE ci-skip 排除, 本地全跑。
# 超时保护: 即便 CI 误跑也在 120s 超时失败暴露, 不无限挂。
set_tests_properties(ut_benchmark_verify PROPERTIES LABELS "ci-skip" TIMEOUT 120)

# ── V4 网络后端正确性测试（AF_XDP / DPDK）──
# 端到端: benchmark 真实 UDP 发包 → AF_XDP(veth)/DPDK(net_tap) 收完整 L2 帧
#   → 剥帧头 → 拆包 → 解析 → 零丢失 + seq 连续
# 需要 root(建 veth/绑 xsk/DPDK 运行) + 内核 XDP/veth 支持 → 标 ci-skip。
# 非 root 运行时测试自身 SKIP(返回 0), CI 全排除。
# AF_XDP 测试需 libbpf(HAVE_AF_XDP), DPDK 测试需 libdpdk(HAVE_DPDK) —— 缺依赖不注册。
if(LIBBPF_FOUND)
    add_executable(ut_af_xdp_verify
        tests/integration/test_af_xdp_verify.cpp
    )
    target_link_libraries(ut_af_xdp_verify PRIVATE core market)
    add_test(NAME ut_af_xdp_verify
        COMMAND ut_af_xdp_verify
            ${CMAKE_CURRENT_SOURCE_DIR}/test_data/itch_chain_sample.bin
            $<TARGET_FILE:trader_benchmark>)
    set_tests_properties(ut_af_xdp_verify PROPERTIES LABELS "ci-skip" TIMEOUT 120)
endif()

if(LIBDPDK_FOUND)
    add_executable(ut_dpdk_verify
        tests/integration/test_dpdk_verify.cpp
    )
    target_link_libraries(ut_dpdk_verify PRIVATE core market)
    add_test(NAME ut_dpdk_verify
        COMMAND ut_dpdk_verify
            ${CMAKE_CURRENT_SOURCE_DIR}/test_data/itch_chain_sample.bin
            $<TARGET_FILE:trader_benchmark>)
    set_tests_properties(ut_dpdk_verify PROPERTIES LABELS "ci-skip" TIMEOUT 120)
endif()
