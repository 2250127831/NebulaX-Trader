#pragma once

#include "core/market_event.h"
#include "core/types.h"

#include <cstdint>
#include <functional>

// ── K线(Bar) ──
// 一段时间的 OHLC 聚合结果。供趋势/动量等低频策略消费。
struct KLine {
    uint64_t symbol_id;   // 股票
    uint64_t timestamp;   // 窗口开始时间
    int64_t  open;        // 开盘价(分)
    int64_t  high;        // 最高价(分)
    int64_t  low;         // 最低价(分)
    int64_t  close;       // 收盘价(分)
    uint64_t volume;      // 成交量
};

// ── K线聚合器 ──
// 消费逐笔成交(MarketEvent.trade)，聚合成 K线。两种分窗口方式：
//   1. 按时间：每 window_ns 纳秒一根 K线(如 1 分钟)，由 tick 自带时间戳触发。
//   2. 按成交数量：每 tick_count 笔成交一根 K线，与时间无关。
//      适合数据量小/时间跨度短的回放——保证 K线一定成形，低频策略有数据。
//      默认按数量(20 笔/根)，因为回放数据时间跨度常不足一个时间窗口。
//
// 用法：
//   KLineAggregator agg;                  // 默认按数量 20 笔/根
//   KLineAggregator agg(60s);             // 按时间 60 秒
//   KLineAggregator agg(kTickCount, 50);  // 按数量 50 笔/根
//   agg.set_sink([](const KLine& bar){ /* 推给策略 */ });
//   agg.on_trade(ev);   // 每笔成交
class KLineAggregator {
public:
    using Sink = std::function<void(const KLine&)>;

    // 按时间窗口(纳秒)，如 60s
    explicit KLineAggregator(uint64_t window_ns)   // 与按数量区分
        : mode_(Mode::TIME), window_ns_(window_ns) {}

    // 按成交数量：每 tick_count 笔一根
    explicit KLineAggregator(int, uint64_t tick_count)
        : mode_(Mode::COUNT), tick_count_(tick_count) {}

    // 默认：按数量 20 笔/根(回放小数据也能成形)
    KLineAggregator() : mode_(Mode::COUNT), tick_count_(20) {}

    void set_sink(Sink sink) { sink_ = std::move(sink); }

    // 每笔成交：并入当前窗口，跨边界则推出完成的 K线
    void on_trade(const MarketEvent& ev) {
        int64_t price = ev.trade.price;
        if (price < 0) return;  // E 消息无价格，跳过

        if (mode_ == Mode::TIME) {
            uint64_t win_start = (ev.timestamp / window_ns_) * window_ns_;
            if (cur_.symbol_id == 0 || win_start != cur_win_start_) {
                if (cur_.symbol_id != 0) push();   // 推出上一根
                start_new(ev, win_start);
                cur_win_start_ = win_start;
            } else {
                extend(ev, price);
            }
        } else {  // COUNT：每 tick_count 笔成交一根
            if (cur_.symbol_id == 0) {
                start_new(ev, ev.timestamp);
                cur_tick_cnt_ = 1;
            } else if (cur_tick_cnt_ >= tick_count_) {
                push();   // 推出上一根
                start_new(ev, ev.timestamp);
                cur_tick_cnt_ = 1;
            } else {
                extend(ev, price);
                ++cur_tick_cnt_;
            }
        }
    }

    // 强制推出当前 K线（窗口结束时由外部调用，或测试用）
    void flush() {
        if (cur_.symbol_id != 0) push();
        cur_ = KLine{};
        cur_win_start_ = 0;
        cur_tick_cnt_ = 0;
    }

    size_t kline_count() const { return emitted_; }

private:
    void push() {
        if (sink_) sink_(cur_);
        ++emitted_;
    }

    void start_new(const MarketEvent& ev, uint64_t ts) {
        cur_.symbol_id = ev.locate;
        cur_.timestamp = ts;
        cur_.open = cur_.high = cur_.low = cur_.close = ev.trade.price;
        cur_.volume = ev.trade.volume;
    }

    void extend(const MarketEvent& ev, int64_t price) {
        cur_.close = price;
        if (price > cur_.high) cur_.high = price;
        if (price < cur_.low)  cur_.low  = price;
        cur_.volume += ev.trade.volume;
    }

    enum class Mode : uint8_t { TIME, COUNT };
    Mode mode_;
    uint64_t window_ns_ = 0;    // TIME 模式窗口(纳秒)
    uint64_t tick_count_ = 20;  // COUNT 模式每根笔数
    KLine cur_{};
    uint64_t cur_win_start_ = 0;
    uint64_t cur_tick_cnt_ = 0;
    size_t emitted_ = 0;        // 已推出 K线数
    Sink sink_;
};
