# 教训：内存池不要让其他类创建

2026/8/3 — 246MB 压测触发 24GB OOM 的根因复盘

## 一句话教训

**共享内存池(OrderPool/OrderMap)由外部(主线程)创建并持有,业务类(OrderBook)只引用,绝不自己创建。**

## 事故经过

- 迁移 NebulaX 撮合簿时,`OrderBook` 自带 `OrderPool` + `OrderMap`(占有模式)
- 撮合引擎只有 **1 个订单簿**,自建没问题
- 本项目是 **6643 个股票** = 6643 个 `OrderBook`
- 每个 `OrderBook` 按共享池容量(1M)预分配 OrderMap → `6643 × 1M × 24B ≈ 160GB`
- 246MB 压测时 RSS 冲到 **24GB**,OOM killer SIGKILL,进程无 core 无栈,极难定位

## 根因

```cpp
// 错误: OrderBook 自己创建了 OrderMap, 容量还用了池容量(1M)
explicit OrderBook(OrderPool& shared_pool)
    : order_index_(shared_pool.capacity())  // 每个簿预分配 1M 节点!
{}

// 每个股票一本簿 → 6643 本 × 1M 节点 = 内存爆炸
books_.try_emplace(locate, shared_pool_);
```

**核心矛盾**:数据共享了(OrderPool),但**索引没共享**(每个 OrderBook 自带 OrderMap),且索引容量错配成池容量。

## 正确做法

```cpp
// 正确: 池和索引都由主线程创建, 全局各一份, OrderBook 只引用
OrderPool shared_pool(cfg.pool_slots);   // 主线程创建
OrderMap  shared_index(cfg.pool_slots);  // 主线程创建

class OrderBook {
    OrderPool& pool_;         // 引用, 不创建
    OrderMap&  order_index_;  // 引用, 不创建
    // 只保留属于该股票的: bids_/asks_(盘口)
};
```

- **挂单数据**:1 份(共享池)
- **挂单索引**:1 份(共享 OrderMap)
- **盘口**:每股票独立(这本来就必须按股票分)
- 内存:24GB → **181MB**(6643 股票 / 587 万委托)

## 通用原则

1. **内存池/索引是资源,由资源所有者(主线程)创建并持有**——业务对象只拿引用
2. **"一个订单一个本体"**:数据、索引各一份,不因对象数量复制
3. **预分配容量按"实际总量",不按"每对象 × 池容量"**
   - 单对象视角: `OrderMap(shared_pool.capacity())` 看似合理
   - 全局视角: N 个对象 × 池容量 = N 倍爆炸
4. **OOM 的特征**:SIGKILL 无栈、日志全丢(缓冲没 flush)、内存随数据量超线性涨
5. **共享后容量可恢复**:全局一份 OrderMap 用 1M 容量没问题(24MB),不爆

## 相关

- [订单簿设计](design/itch5_protocol.md)
- OrderBook 现在只有共享构造(见 `market/book/order_book.h`)
