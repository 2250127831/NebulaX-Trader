#pragma once

#include "strategy/kline/kline_aggregator.h"

#include <deque>

// ── K线消费者 ──
// 收集 K线价格(供展示/其他模块查看)。K线聚合器产出的 K线直接喂 on_bar()。
// 保留最近 N 根 K线的收盘价，供趋势/动量等低频策略或外部查看。
class KLineConsumer {
public:
    explicit KLineConsumer(size_t keep = 200) : keep_(keep) {}

    void on_bar(const KLine& bar) {
        closes_.push_back(bar.close);
        if (closes_.size() > keep_) closes_.pop_front();
    }

    // 最近 N 根收盘价(倒序,最新在 front)
    const std::deque<int64_t>& closes() const { return closes_; }
    size_t size() const { return closes_.size(); }

private:
    size_t keep_;
    std::deque<int64_t> closes_;
};
