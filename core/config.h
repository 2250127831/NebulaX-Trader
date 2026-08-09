#pragma once

#include <string>
#include <vector>
#include <cstdint>

// ── 行情配置 ──
struct MarketConfig {
    std::string type = "itch";          // itch（回放）
    std::string host = "127.0.0.1";
    uint16_t    port = 8080;
    std::string backend = "io_uring";   // 网络接收后端: io_uring / af_xdp / dpdk
                                        //   io_uring: UDP socket(内核剥头), 默认
                                        //   af_xdp  : 绑 ifname 接口, 收完整 L2 帧后内部剥头
                                        //   dpdk    : vdev 软件/物理 PMD, 收完整 L2 帧后内部剥头
    std::string ifname  = "";           // af_xdp: 绑定接口(如 veth0 / enp4s0)
    std::string vdev    = "";           // dpdk: vdev 规格(如 net_tap0 → 内核 tap dtap0)
    std::vector<std::string> eal_args;  // dpdk: EAL 参数(如 -l 0 --no-pci --no-huge -m 128)
    uint32_t    uring_entries = 256;
    size_t      ring_bytes = 1 << 22;   // 字节 ring 容量（接收→解析）
    size_t      chan_slots = 1 << 20;   // 通道 A/B 槽位
};

// ── 订单簿消费者配置（通道B）──
struct OrderBookConfig {
    bool   enabled = true;              // 是否启用订单簿重建 + OBI/OFI
    size_t pool_slots = 1 << 20;        // 共享挂单池槽位
    size_t workers = 4;                 // 分簿并行 book_worker 数(广播+skip)
};

// ── 策略配置 ──
struct StrategyConfig {
    // 主策略：成交量突破 / 趋势 / 动量
    std::string primary = "volume_breakout";
    size_t      vol_window = 100;       // 量突窗口
    uint64_t    vol_threshold = 5000;   // 量突阈值
    // 从策略
    bool        use_obi = true;         // 盘口失衡
    bool        use_ofi = true;         // 订单流失衡
    // 低频主策略候选（K线）
    size_t      kline_ticks = 10;       // 每 N 笔成交一根 K线
};

// ── 风控配置 ──
struct RiskConfig {
    uint64_t max_position = 10000;
    int64_t  max_daily_loss = 100000000;  // 分（默认 100 万元）
};

// ── 执行引擎配置 ──
struct ExecutionConfig {
    uint64_t base_qty = 100;            // 满强度基准下单量
    uint16_t order_port = 9090;         // 模拟交易所收单端口
    uint16_t order_ret_port = 9091;     // 成交回报接收端口
    uint32_t idle_timeout_sec = 3;      // 行情停止增长后退出等待秒数
};

// ── 全局配置 ──
struct Config {
    MarketConfig    market;
    OrderBookConfig order_book;
    StrategyConfig  strategy;
    RiskConfig      risk;
    ExecutionConfig execution;
};

// ── 配置加载器 ──
// 依赖 yaml-cpp 库，CMake 中已配置 find_package(yaml-cpp REQUIRED)
class ConfigLoader {
public:
    // 从 YAML 文件加载配置，未指定路径则读取 config/default.yaml
    static Config load(const std::string& path = "config/default.yaml");
};
