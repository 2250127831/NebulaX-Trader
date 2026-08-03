#include "market/book/order_book_consumer.h"

void OrderBookConsumer::on_event(const MarketEvent& ev) {
    switch (ev.type) {
        case MarketEvent::Type::STOCK_DIR:
            if (!ev.symbol.empty()) symbols_[ev.locate] = ev.symbol;
            break;
        case MarketEvent::Type::ADD:        handle_add(ev); break;
        case MarketEvent::Type::DELETE:     handle_delete(ev); break;
        case MarketEvent::Type::CANCEL:     handle_cancel(ev); break;
        case MarketEvent::Type::REPLACE:    handle_replace(ev); break;
        case MarketEvent::Type::TRADE:      handle_trade(ev, true); break;
        case MarketEvent::Type::EXECUTE:    handle_execute(ev); break;
    }
}

void OrderBookConsumer::handle_add(const MarketEvent& ev) {
    books_[ev.locate].add(ev.order_ref, ev.side, ev.price, ev.shares);
}

void OrderBookConsumer::handle_delete(const MarketEvent& ev) {
    books_[ev.locate].remove(ev.order_ref);
}

void OrderBookConsumer::handle_cancel(const MarketEvent& ev) {
    books_[ev.locate].cancel(ev.order_ref, ev.shares);
}

void OrderBookConsumer::handle_replace(const MarketEvent& ev) {
    // U 无方向：继承旧单方向（旧单不存在则无法重建，跳过）
    OrderSide side = books_[ev.locate].side_of(ev.order_ref);
    if (side == OrderSide::NONE) return;
    books_[ev.locate].replace(ev.order_ref, ev.new_order_ref, side, ev.price, ev.shares);
}

void OrderBookConsumer::handle_trade(const MarketEvent& ev, bool has_price) {
    books_[ev.locate].execute(ev.order_ref, ev.shares);

    // 产出 Tick（喂策略）
    ++tick_seq_;
    last_tick_.timestamp  = ev.timestamp;
    last_tick_.seq_id    = tick_seq_;
    last_tick_.symbol_id = ev.locate;
    if (has_price) last_tick_.last_price = ev.price;
    last_tick_.volume    = ev.shares;
    has_tick_ = true;
}

void OrderBookConsumer::handle_execute(const MarketEvent& ev) {
    // E 不带价格：从簿查该挂单的价格
    int64_t price = books_[ev.locate].price_of(ev.order_ref);
    books_[ev.locate].execute(ev.order_ref, ev.shares);

    ++tick_seq_;
    last_tick_.timestamp  = ev.timestamp;
    last_tick_.seq_id    = tick_seq_;
    last_tick_.symbol_id = ev.locate;
    if (price >= 0) last_tick_.last_price = price;  // 查不到保持上一次价格
    last_tick_.volume    = ev.shares;
    has_tick_ = true;
}
