# ITCH 5.0 协议布局（NASDAQ TotalView）

> 数据源：`test_data/itch_100mb.bin`（NASDAQ 官方样本，245.7MB，2020-01-31 快照）
> 本文档记录解析器用到的消息布局，均为**大端序**。

## 编码约定

- 所有消息以 **2 字节 big-endian 长度前缀**开头，length = 消息体字节数（不含前缀）
- 消息体第一字节是消息类型（ASCII）
- **时间戳**：6 字节，纳秒（自午夜）
- **价格**：4 字节 int，单位 `1/10000 美元`（注意：不是 1/100，是 1/10000）
- **股票引用**：所有后续消息用 **Stock Locate（2 字节 int）** 引用股票，不是 symbol 字符串
  - 必须先从 R（Stock Directory）消息建立 `locate → symbol` 映射

## 消息布局

### R — Stock Directory（39 字节）
```
type(1) locate(2) track(2) ts(6) stock(8) marketcat(1) financial(1) roundlot(4) ...
locate → stock 映射的唯一来源，交易日开始时发。
```

### A — Add Order（36 字节）
```
type(1) locate(2) track(2) ts(6) orderref(8) buysell(1) shares(4) stock(8) price(4)
新挂单进入订单簿。
```

### F — Add Order w/ MPID（40 字节）
```
与 A 相同，末尾多 4 字节 attribution（MPID）。
```

### D — Order Delete（19 字节）
```
type(1) locate(2) track(2) ts(6) orderref(8)
整笔撤单，从订单簿移除该 orderref。
```

### X — Order Cancel（23 字节）
```
type(1) locate(2) track(2) ts(6) orderref(8) cancelledshares(4)
部分撤单，减少该 orderref 的剩余量。
```

### U — Order Replace（35 字节）
```
type(1) locate(2) track(2) ts(6) oldref(8) newref(8) shares(4) price(4)
改单：oldref 作废，newref 以新价/新量挂出。内部需把 oldref 的状态迁移到 newref。
```

### P — Trade Non-Cross（44 字节）
```
type(1) locate(2) track(2) ts(6) orderref(8) buysell(1) shares(4) stock(8) price(4) matchnum(8)
完整成交：带价格、方向、股票。更新 last_price / volume / 订单簿剩余量。
```

### E — Order Executed（31 字节）
```
type(1) locate(2) track(2) ts(6) orderref(8) shares(4) matchnum(8)
成交（不含价格）：某挂单被吃掉 shares 股。价格要靠 orderref 查原始挂单（A/U）。
```

### C — Order Executed with Price（35 字节）(若出现)
```
type(1) locate(2) track(2) ts(6) orderref(8) shares(4) stock(8) price(4) matchnum(8)
```

### 其他
- **S** System Event（12B）：`type(1) locate(2) track(2) ts(6) eventcode(1)`
- **H** Trading Action（25B）、**Y** Price Adjustment（20B）、**L** Market Participant Position（26B）
- **V** Order Executed w/ Price（Price Variant）（36B）

## 订单簿重建逻辑（解析器核心）

```
A/F:  locate → book，orderref → 挂单(价/量/方向)，加入对应价档
X:    orderref 剩余量 -= cancelledshares
D:    orderref 整笔移除
U:    oldref 移除，newref 按新价/新量加入
P:    该成交方向挂单剩余量 -= shares；更新 last_price/volume
E:    按 orderref 查价，该挂单剩余量 -= shares；更新 last_price/volume
```

## 价格换算

ITCH 价格是 `int32 × 1/10000 美元`。本项目 `Tick` 用整数定点（`int64_t` × 100 分，`kTickSize=100`）。
换算发生在**协议边界**：

```cpp
// ITCH price (1/10000 美元) → 内部定点 (分)
int64_t itch_to_points(uint32_t itch_price) {
    return static_cast<int64_t>(itch_price) / 100;  // 1 美元 = 100 分 = 10000 ticks
}
```

> 注意：ITCH 精度（1/10000）比 A 股（1/100）细。若未来接 A 股 L2，精度由协议决定，内部统一用 `int64_t` 定点不冲突。
