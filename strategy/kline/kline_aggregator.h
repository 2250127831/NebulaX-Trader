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
// 消费逐笔成交(MarketEvent.trade)，按 timestamp 分窗口(如 1 分钟)聚合 K线。
// 事件驱动：每笔成交并入当前窗口；时间戳跨过窗口边界 → 当前 K线完成，回调推出。
// 不需要定时器：tick 自带 timestamp，窗口边界由它触发。
//
// 用法：
//   KLineAggregator agg(window_ns);
//   agg.set_sink([](const KLine& bar){ /* 推给策略 */ });
//   agg.on_trade(ev);   // 每笔成交
class KLineAggregator {
public:
    using Sink = std::function<void(const KLine&)>;

    explicit KLineAggregator(uint64_t window_ns = 60ull * 1000000000ull)  // 默认 1 分钟
        : window_ns_(window_ns) {}

    void set_sink(Sink sink) { sink_ = std::move(sink); }

    // 每笔成交：并入当前窗口，跨边界则推出完成的 K线
    void on_trade(const MarketEvent& ev) {
        int64_t price = ev.trade.price;
        if (price < 0) return;  // E 消息无价格，跳过

        uint64_t win_start = (ev.timestamp / window_ns_) * window_ns_;
        if (cur_.symbol_id == 0 || win_start != cur_win_start_) {
            // 新窗口：先推出上一个完成的 K线
            if (cur_.symbol_id != 0) {
                cur_.close = cur_.close;  // 已填
                if (sink_) sink_(cur_);
            }
            cur_.symbol_id = ev.locate;
            cur_.timestamp = win_start;
            cur_.open = cur_.high = cur_.low = cur_.close = price;
            cur_.volume = ev.trade.volume;
            cur_win_start_ = win_start;
        } else {
            cur_.close = price;
            if (price > cur_.high) cur_.high = price;
            if (price < cur_.low)  cur_.low  = price;
            cur_.volume += ev.trade.volume;
        }
    }

    // 强制推出当前 K线（窗口结束时由外部调用，或测试用）
    void flush() {
        if (cur_.symbol_id != 0 && sink_) sink_(cur_);
        cur_ = KLine{};
        cur_win_start_ = 0;
    }

private:
    uint64_t window_ns_;
    KLine cur_{};
    uint64_t cur_win_start_ = 0;
    Sink sink_;
};
