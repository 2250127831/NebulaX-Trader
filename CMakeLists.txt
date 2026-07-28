cmake_minimum_required(VERSION 3.20)
project(NebulaX-Trader VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── 外部依赖 ──
find_package(yaml-cpp REQUIRED)

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
    core/memory/memory_pool.h
    core/utils/align.h
)

target_include_directories(core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(core PUBLIC yaml-cpp)

# ── 行情模块 ──
add_library(market
    market/gateway/market_data_gateway.h
    market/parser/tick_parser.h
    market/simulator/market_simulator.h
)

target_link_libraries(market PUBLIC core)
target_include_directories(market PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── 策略模块 ──
add_library(strategy
    strategy/base/strategy.h
    strategy/momentum/momentum_strategy.h
    strategy/trend/trend_strategy.h
)

target_link_libraries(strategy PUBLIC core)
target_include_directories(strategy PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── OMS ──
add_library(oms
    oms/order.h
    oms/order_manager.h
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

# ── 集成测试 ──
add_executable(it_tick_pipeline
    tests/integration/test_tick_pipeline.cpp
)
target_link_libraries(it_tick_pipeline PRIVATE core market strategy)
add_test(NAME it_tick_pipeline COMMAND it_tick_pipeline)
