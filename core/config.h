#pragma once

#include <string>
#include <vector>
#include <cstdint>

// ── 行情配置 ──
struct MarketConfig {
    std::string type = "simulator";   // simulator | websocket | api
    std::string host = "127.0.0.1";
    uint16_t    port = 8080;
    uint32_t    uring_entries = 256;
};

// ── 策略配置 ──
struct StrategyConfig {
    std::string name = "trend";       // trend | momentum
    uint32_t    workers = 4;
    uint32_t    fast_period = 10;
    uint32_t    slow_period = 30;
};

// ── 风控配置 ──
struct RiskConfig {
    uint64_t max_position = 10000;
    double   max_daily_loss = 100000.0;
};

// ── 执行引擎配置 ──
struct ExecutionConfig {
    std::string algo = "straight";    // straight | iceberg | twap
    uint32_t    timeout_ms = 1000;
    uint32_t    max_retries = 3;
};

// ── Worker 线程配置 ──
struct WorkerConfig {
    std::vector<uint32_t> cpu_affinity;  // 绑核列表，空表示不绑
};

// ── 全局配置 ──
struct Config {
    MarketConfig    market;
    StrategyConfig  strategy;
    RiskConfig      risk;
    ExecutionConfig execution;
    WorkerConfig    worker;
};

// ── 配置加载器 ──
// 依赖 yaml-cpp 库，CMake 中已配置 find_package(yaml-cpp REQUIRED)
class ConfigLoader {
public:
    // 从 YAML 文件加载配置，未指定路径则读取 config/default.yaml
    static Config load(const std::string& path = "config/default.yaml");
};
