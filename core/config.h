#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

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
    // 启用的策略组合(编译期绑定, 启动期固定; 按序对应槽)。
    // 白名单: ofi / obi; 空列表 = 仅收行情不交易。
    std::vector<std::string> strategies = {"ofi", "obi"};
    std::string primary = "ofi";                 // 主策略(须在 strategies 中, 启动校验);
                                                 // 决定下单标的/seq, 不单独定方向。
    std::map<std::string, int64_t> weights_bp;   // 策略名 → 投票权重(万分比, 10000=1.0);
                                                 // 未列缺省 1.0(公平投票)。
    int64_t vote_threshold_bp = 0;               // 净投票阈值(万分比); 0 = 任一策略有信号即按净信号下。
    size_t      vol_window = 100;       // 量突窗口(volume_breakout 参数, 接入白名单时启用)
    uint64_t    vol_threshold = 5000;   // 量突阈值
    size_t      kline_ticks = 10;       // 每 N 笔成交一根 K线(K线策略参数)
};

// ── 风控配置 ──
struct RiskConfig {
    uint64_t max_position = 10000;
    int64_t  max_daily_loss = 100000000;  // 分（默认 100 万元）
    int64_t  initial_equity = 100000000;  // 初始资金(分, 默认 100 万元), 净值基准
    int64_t  max_drawdown_pause = 5000000;   // 回撤 5% 触发暂停新单(分)
    int64_t  max_drawdown_flatten = 10000000;  // 回撤 10% 触发强制平仓(分)
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
