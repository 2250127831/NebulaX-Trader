# OrderMap 并发设计：从业务约束推导出无锁快路径

> 日期：2026-08-05 · 分支：V2 · 配套 [V2_PLAN.md](V2_PLAN.md) §3.5（book_th 分簿并行的前置数据结构）
>
> 一句话：**利用"分簿保证同订单单线程访问"的业务约束，用一个 bucket 字段 + 链头 CAS + find 重试，把并发哈希表的正确性做到"无惰性删除、无 hazard pointer、无延迟回收"的极简。**

---

## 0. 这个设计在讲什么

`OrderMap` 是 `order_ref → OrderSlot*` 的哈希表（订单簿挂单索引，替代 `std::unordered_map`）。V2 要把单订单簿线程拆成 N 个 worker 分簿并行，N 个 worker 并发访问共享的 OrderMap，需要并发安全。

通用解法（hazard pointer / 惰性删除 / RCU / 桶锁）都很重。本文的出发点是：**我们的业务有一个关键约束，能不能用它把并发问题简化掉？** 答案是能，而且简化得非常彻底。

---

## 1. 业务约束（一切推导的基石）

**分簿后，一条行情消息只被一个 worker 真实消费，同一个 order_ref 只被它的 owner worker 访问。**

| 操作 | 谁做 | 并发性 |
|---|---|---|
| 同一 order_ref 的 insert/find/erase | 同一个 owner worker | **串行**（单线程） |
| 不同 order_ref 哈希到同一桶 | 不同 worker | **并发**（链头竞争） |
| free_head_（跨桶共享空闲节点池） | 所有 worker | **并发**（alloc/free 竞争） |

### 推论 1：同一 order_ref 不会被并发 find/erase

不存在"两个线程同时读同一个节点"。我们担心的"无锁 find 读到正在被 erase 的节点"——**因为 erase 和 find 是同一个线程，根本不会并发**。这个竞态不存在。

### 推论 2：find 遍历链时会经过别人的节点

不同 locate 订单哈希到同一桶，链上相邻。worker A 找自己的 key 时，会经过 worker B 的节点。**但 A 只读 B 节点的 order_ref（不匹配就跳过），不会对 B 的节点做操作。**

---

## 2. 真正的并发点（只有两处）

### 2.1 链头竞争：不同 locate 订单哈希到同一桶

```
worker A (locate L1, key k1):  insert → 桶 b
worker B (locate L2, key k2):  insert → 桶 b (hash(k1)==hash(k2))
                              ── 都试图改写 buckets_[b] 链头 ──
```
→ **链头 CAS**（无锁）。CAS 失败重试。

### 2.2 free_head_ 竞争：跨桶共享空闲节点池

所有 worker 从同一个空闲节点池 allocNode/freeNode。
→ **Treiber 无锁栈**（CAS）。槽位单一所有权 → ABA 不丢节点。

---

## 3. 最棘手的竞态：节点复用导致的"跳链"（我们的核心洞察）

### 场景

```
链(桶b): H → B → G        (B 是 worker B 的节点, 哈希到桶 b)
worker B: erase(B), 物理摘除, 归还 free_head_ 栈
worker A: find(kA), 遍历到 H → B (读 B.order_ref, 不匹配, 继续)
    同时: B 被 worker C 重新 allocate, 复用为新订单, next 指向"另一个桶"的链
worker A: 读 B.next → 跳到别的链 → 在错链上找不到 kA → 误判"不存在"
```

**本质**：内存池是栈，刚归还的节点可能被立刻复用。复用时 `next_idx` 被改写指向无关链表，**无锁 find 被带偏到别的链**。这不是 use-after-free（池内存还在），是**逻辑错乱（跳链）**。

### 通用解法为什么重

- **hazard pointer**：find 遍历前登记，erase 前扫描——find 路径要加登记/校验，高频路径变复杂
- **惰性删除**：erase 不物理摘除，标记删除延迟回收——erase 路径复杂化
- **RCU / epoch**：回收延迟一个宽限期——实现重

这些都是在"无锁 find 上叠加回收机制"，**都很重**。

---

## 4. 我们的解法：给 Node 加一个 `bucket` 字段，find 校验重试

### 4.1 表示

```cpp
struct Node {
    uint64_t   order_ref;   // 交易所挂单唯一引用号（ITCH order_ref）
    OrderSlot* order;       // 挂单池化数据
    uint32_t   next_idx;    // 链指针 / 空闲链表(共用)
    uint32_t   bucket;      // 本节点所属桶（核心: 防跳链）
};
```

### 4.2 find：无锁读链 + bucket 校验 + 重试

```cpp
OrderSlot* find(uint64_t k) {
    uint32_t b = hash(k);                    // 桶号由 key 内部算, 不外传
retry:
    uint32_t N = buckets_[b].load();         // 重读链头
    while (N != UINT32_MAX) {
        if (nodes_[N].bucket != b) goto retry;   // 读到别的桶的节点 = 链被并发跳走 → 重试
        if (nodes_[N].order_ref == k) return nodes_[N].order;
        N = nodes_[N].next_idx;                  // 前进
    }
    return nullptr;                          // 正常走到链尾 → 真不存在
}
```

**关键语义**：`bucket` 校验**不是**"发现跳链就返回 not found"，而是**重试**。
- 读到 `bucket != 目标桶` 只说明**这条链刚被并发改过**（有节点被摘除/复用，next 跳链）
- 重试从最新链头重新走，能避开已摘除/复用的节点
- 返回 not found 是**错误**的：k 可能明明还在，只是刚才遍历时链被并发改了一下没走到

### 4.3 add：链头 CAS

```cpp
N = allocNode();                 // Treiber 无锁取空闲节点
nodes_[N] = {key, order, buckets_[b].load(), b};   // next = 当前桶头
while (!buckets_[b].compare_exchange_weak(old, N)) // CAS 链头, 失败重读重试
    nodes_[N].next_idx = old;
```

### 4.4 erase：摘除 + 归还

```
b = hash(key)
找到前驱/自身 → CAS 摘除（链头则桶头 CAS, 链中则 CAS 前驱 next 跳过）
freeNode(N)   // Treiber 无锁归还
```

### 4.5 树化桶（桶长≥8）：外部锁

```
桶长≥8 → 锁 overflow_lock_ → 复制式迁移进 std::map → 置 overflowed_[b]=1
之后该桶 insert/erase/find 锁 overflow_lock_ 走 overflow_
```

> STL `std::map` 内部并发不可控，**只能从外部整体锁**。树化概率 6.6e-24（λ=0.005），几乎永不触发，锁的成本可忽略。

---

## 5. 为什么这个设计成立（正确性论证）

| 竞态 | 保护 | 原理 |
|---|---|---|
| 同 key 并发 | 分簿保证串行 | 同 order_ref 只被 owner 访问 |
| 跨 key 同桶链头竞争 | 链头 CAS | CAS 失败重试 |
| 节点复用跳链 | `bucket` 校验 + 重试 | 读到别的桶 → 重试而非误判 |
| free_head_ 竞争 | Treiber 无锁栈 | 槽位单一所有权 → ABA 不丢 |

### 关键：为什么不需要惰性删除 / hazard pointer / 延迟回收

1. **不需要惰性删除**：惰性删除防的是"无锁 find 读到正在被 erase 的节点"。但同一 order_ref 的 find/erase 是同一线程（推论 1），**不会并发**。
2. **不需要 hazard pointer**：它防 use-after-free。但内存池不释放内存（只复用），且复用跳链已被 `bucket` 校验止损 + 重试解决。
3. **不需要延迟回收**：延迟回收防"复用改写 next 跳链"。`bucket` 校验让 find 能识别跳链并重试，**不需要延迟节点归还**。

**一句话**：把并发问题的解从"在无锁 find 上叠加回收机制"（重）换成"给数据加一个字段，让 find 自己能识别并规避并发痕迹"（轻）。

---

## 6. 与 V2_PLAN 的关系

- 前置：V2_PLAN §3.4（OrderPool 无锁 Treiber 栈）+ §3.5（OrderMap 并发）
- 配套：分簿并行（§2.2）的 worker 架构，本设计是它的数据结构底座
- 树化桶外部锁：STL map 不可控，只能锁——见 §4.5
