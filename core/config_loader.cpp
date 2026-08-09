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
        if (n["ring_bytes"])     cfg.market.ring_bytes    = n["ring_bytes"].as<size_t>();
        if (n["chan_slots"])     cfg.market.chan_slots    = n["chan_slots"].as<size_t>();
        if (n["parse_workers"])  cfg.market.parse_workers = n["parse_workers"].as<size_t>();
    }

    // order_book
    if (auto n = root["order_book"]) {
        if (n["enabled"])     cfg.order_book.enabled    = n["enabled"].as<bool>();
        if (n["pool_slots"])  cfg.order_book.pool_slots = n["pool_slots"].as<size_t>();
        if (n["workers"])     cfg.order_book.workers    = n["workers"].as<size_t>();
    }

    // strategy
    if (auto n = root["strategy"]) {
        if (n["primary"])         cfg.strategy.primary         = n["primary"].as<std::string>();
        if (n["vol_window"])      cfg.strategy.vol_window      = n["vol_window"].as<size_t>();
        if (n["vol_threshold"])   cfg.strategy.vol_threshold   = n["vol_threshold"].as<uint64_t>();
        if (n["use_obi"])         cfg.strategy.use_obi         = n["use_obi"].as<bool>();
        if (n["use_ofi"])         cfg.strategy.use_ofi         = n["use_ofi"].as<bool>();
        if (n["kline_ticks"])     cfg.strategy.kline_ticks     = n["kline_ticks"].as<size_t>();
    }

    // risk
    if (auto n = root["risk"]) {
        if (n["max_position"])   cfg.risk.max_position   = n["max_position"].as<uint64_t>();
        if (n["max_daily_loss"]) cfg.risk.max_daily_loss = n["max_daily_loss"].as<int64_t>();
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
