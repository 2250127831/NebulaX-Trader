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
        if (n["backend"])        cfg.market.backend       = n["backend"].as<std::string>();
        if (n["ifname"])         cfg.market.ifname        = n["ifname"].as<std::string>();
        if (n["vdev"])           cfg.market.vdev          = n["vdev"].as<std::string>();
        if (n["eal_args"]) {
            cfg.market.eal_args.clear();
            for (const auto& a : n["eal_args"])
                cfg.market.eal_args.push_back(a.as<std::string>());
        }
        if (n["uring_entries"])  cfg.market.uring_entries = n["uring_entries"].as<uint32_t>();
        if (n["ring_bytes"])     cfg.market.ring_bytes    = n["ring_bytes"].as<size_t>();
        if (n["chan_slots"])     cfg.market.chan_slots    = n["chan_slots"].as<size_t>();
    }

    // order_book
    if (auto n = root["order_book"]) {
        if (n["enabled"])     cfg.order_book.enabled    = n["enabled"].as<bool>();
        if (n["pool_slots"])  cfg.order_book.pool_slots = n["pool_slots"].as<size_t>();
        if (n["workers"])     cfg.order_book.workers    = n["workers"].as<size_t>();
    }

    // strategy
    if (auto n = root["strategy"]) {
        if (n["strategies"])      cfg.strategy.strategies     = n["strategies"].as<std::vector<std::string>>();
        if (n["primary"])         cfg.strategy.primary        = n["primary"].as<std::string>();
        if (n["weights"]) {   // yaml double → 万分比定点整数(10000=1.0)
            for (const auto& kv : n["weights"]) {
                std::string name = kv.first.as<std::string>();
                double w = kv.second.as<double>();
                cfg.strategy.weights_bp[name] = (int64_t)(w * 10000.0);
            }
        }
        if (n["vote_threshold"]) cfg.strategy.vote_threshold_bp = (int64_t)(n["vote_threshold"].as<double>() * 10000.0);
        if (n["vol_window"])     cfg.strategy.vol_window       = n["vol_window"].as<size_t>();
        if (n["vol_threshold"])  cfg.strategy.vol_threshold    = n["vol_threshold"].as<uint64_t>();
        if (n["kline_ticks"])    cfg.strategy.kline_ticks      = n["kline_ticks"].as<size_t>();
    }

    // risk
    if (auto n = root["risk"]) {
        if (n["max_position"])   cfg.risk.max_position   = n["max_position"].as<uint64_t>();
        if (n["max_daily_loss"]) cfg.risk.max_daily_loss = n["max_daily_loss"].as<int64_t>();
        if (n["initial_equity"]) cfg.risk.initial_equity = n["initial_equity"].as<int64_t>();
        if (n["max_drawdown_pause"])   cfg.risk.max_drawdown_pause   = n["max_drawdown_pause"].as<int64_t>();
        if (n["max_drawdown_flatten"]) cfg.risk.max_drawdown_flatten = n["max_drawdown_flatten"].as<int64_t>();
    }

    // execution
    if (auto n = root["execution"]) {
        if (n["base_qty"])         cfg.execution.base_qty         = n["base_qty"].as<uint64_t>();
        if (n["order_port"])       cfg.execution.order_port       = n["order_port"].as<uint16_t>();
        if (n["order_ret_port"])   cfg.execution.order_ret_port  = n["order_ret_port"].as<uint16_t>();
        if (n["idle_timeout_sec"]) cfg.execution.idle_timeout_sec = n["idle_timeout_sec"].as<uint32_t>();
    }

    return cfg;
}
