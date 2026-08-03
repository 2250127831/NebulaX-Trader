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
    core/queue/spsc_byte_ring.h
    core/queue/spmc_ring.h
    core/queue/spmc_event_queue.h
    core/queue/queue_manager.h
    core/market_event.h
    core/memory/memory_pool.h
    core/memory/order_pool.h
    core/memory/order_map.h
    core/utils/align.h
    core/net/i_market_data_receiver.h
    core/net/i_market_data_sender.h
    core/net/io_uring_poller.h
    core/net/io_uring_receiver.h
    core/net/io_uring_receiver.cpp
    core/net/io_uring_sender.h
    core/net/io_uring_sender.cpp
    core/ipc/flow_control.h
)

target_include_directories(core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(core PUBLIC yaml-cpp ${URING_LIBS})
target_include_directories(core PUBLIC ${LIBURING_INCLUDE_DIRS})

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
    strategy/momentum/momentum_strategy.h
    strategy/trend/trend_strategy.h
    strategy/kline/kline_aggregator.h
    strategy/kline/kline_consumer.h
    strategy/tick/volume_breakout_strategy.h
    strategy/tick/price_breakout_strategy.h
    strategy/tick/trade_direction_strategy.h
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
    oms/order_protocol.h
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

target_link_libraries(trader PRIVATE core)

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

add_executable(ut_ofi
    tests/unit/test_ofi.cpp
)
target_link_libraries(ut_ofi PRIVATE core strategy)
add_test(NAME ut_ofi COMMAND ut_ofi)

add_executable(ut_execution
    tests/unit/test_execution.cpp
)
target_link_libraries(ut_execution PRIVATE core oms risk execution strategy)
add_test(NAME ut_execution COMMAND ut_execution)

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
