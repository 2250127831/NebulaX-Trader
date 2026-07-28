#include "config.h"
#include <yaml-cpp/yaml.h>
#include <cstdio>

Config ConfigLoader::load(const std::string& path) {
    printf("Loading config from %s\n", path.c_str());
    YAML::Node root = YAML::LoadFile(path);

    Config cfg;

    // market
    if (auto n = root["market"]) {
        if (n["type"])           cfg.market.type          = n["type"].as<std::string>();
        if (n["host"])           cfg.market.host          = n["host"].as<std::string>();
        if (n["port"])           cfg.market.port          = n["port"].as<uint16_t>();
        if (n["uring_entries"])  cfg.market.uring_entries = n["uring_entries"].as<uint32_t>();
    }

    // strategy
    if (auto n = root["strategy"]) {
        if (n["name"])        cfg.strategy.name        = n["name"].as<std::string>();
        if (n["workers"])     cfg.strategy.workers     = n["workers"].as<uint32_t>();
        if (n["fast_period"]) cfg.strategy.fast_period = n["fast_period"].as<uint32_t>();
        if (n["slow_period"]) cfg.strategy.slow_period = n["slow_period"].as<uint32_t>();
    }

    // risk
    if (auto n = root["risk"]) {
        if (n["max_position"])   cfg.risk.max_position   = n["max_position"].as<uint64_t>();
        if (n["max_daily_loss"]) cfg.risk.max_daily_loss = n["max_daily_loss"].as<double>();
    }

    // execution
    if (auto n = root["execution"]) {
        if (n["algo"])        cfg.execution.algo        = n["algo"].as<std::string>();
        if (n["timeout_ms"])  cfg.execution.timeout_ms  = n["timeout_ms"].as<uint32_t>();
        if (n["max_retries"]) cfg.execution.max_retries = n["max_retries"].as<uint32_t>();
    }

    // worker
    if (auto n = root["worker"]) {
        if (n["cpu_affinity"]) {
            cfg.worker.cpu_affinity = n["cpu_affinity"].as<std::vector<uint32_t>>();
        }
    }

    return cfg;
}
