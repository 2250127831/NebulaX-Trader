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

---

## 7. 实际实施遇到的竞态与解决方案（实战验证）

> 理论设计（§1-5）落成代码后，多线程压力测试（8 线程 × 512 key × 20000 次 insert/find/erase 循环）暴露了 4 个真实的并发问题。每个都是"看着对、跑起来错"的经典陷阱。**这段记录的是"设计时没想到、踩坑才发现"的部分——比理论设计更能体现对并发真实性的理解。**

### 7.1 竞态一：free_head_ 的 Treiber ABA（最致命）

**症状**：`-O1` 压力测试稳定段错误；单线程 50 万次通过，但多线程并发 insert/erase（节点归还复用）必崩。

**根因**：空闲栈 `free_head_` 无 tag 的 Treiber 栈有 ABA——两个线程都读到栈头 `head=X`，T1 的 CAS(X→next) 成功后 T2 的 stale CAS 仍可能成功，导致**两个线程同时"拥有"同一个节点**，链上节点字段互相覆盖。

**解决**：`free_head_` 升级为 `std::atomic<uint64_t>` 存 `(tag<<32)|idx`，每次成功 CAS 都 `tag+1`。**stale CAS 因 tag 不匹配而失败** → 重读重试。

```cpp
std::atomic<uint64_t> free_head_;   // (tag<<32)|idx, tag 消 ABA
// allocNode:
uint64_t fh = free_head_.load(relaxed);
while ((uint32_t)fh != UINT32_MAX) {
    uint32_t next = nodes_[(uint32_t)fh].next_idx.load(relaxed);
    if (free_head_.compare_exchange_weak(fh, (((fh>>32)+1)<<32)|next, release, relaxed))
        return (uint32_t)fh;
    fh = 已更新;   // CAS 失败, 重读重试
}
```

**教训**：**任何"归还后可复用"的无锁空闲栈都必须带 tag**。§2.2 里"槽位单一所有权 → ABA 不丢节点"的注释是错的——节点归还后立即被别的线程复用，"单一所有权"只在"持有期间"成立，不覆盖归还-复用窗口。

### 7.2 竞态二：erase 链中删除的前驱被复用（跳链）

**症状**：修复竞态一后，压力测试结束时有 3 个孤儿 key 残留（map.size()=3 ≠ 0），即 3 个 erase 没删掉。

**根因**：erase 链中删除时遍历找前驱，**前驱可能是别人的节点，会被其 owner 并发 erase → 归还 → 复用**。复用后前驱的 `next` 指向别的桶链，erase 沿错链找 K → 找不到 → 没删掉。

**解决**：**把 find 的 bucket 校验同样应用到 erase 的链中遍历**——每步校验前驱 `bucket == 目标桶`，读到 `bucket≠b` 说明前驱已被复用跳桶 → **重试**（重读链头）。

```cpp
while (idx != UINT32_MAX) {
    if (nodes_[idx].bucket != b) goto retry;   // 前驱被复用跳桶 → 重找
    ...
    // CAS 前驱 next 跳过本节点; CAS 返回值是唯一权威, 不靠 re-read 判相等(那正是 ABA)
    if (nodes_[idx].next_idx.compare_exchange_weak(next, nxt2, release, relaxed)) {
        freeNode(next); ... return;
    }
    goto retry;   // CAS 失败 → 重走链头
}
```

**教训**：bucket 校验不只 find 需要，**所有无锁链遍历（find/erase）都需要**。前驱是"别人的节点"，同样会被并发复用。§4.4 只写了"找到前驱 CAS 摘除"，没写前驱本身的并发。

### 7.3 竞态三：treeify 与链插入的竞态窗口

**症状**：树化桶（桶长≥8）时，find 可能 miss——链上有新节点但 find 走 overflow_ 查不到。

**根因**：§4.5 初始设计只有 `overflowed_` 标志，**没考虑路径切换竞态**。insert 读到 `overflowed_=0`（未树化）走链路径链头 CAS 插入 K，但**同时另一个线程触发 treeify**（复制链 + 置 `overflowed_=1`）。K 落在链上，但 find 已走 overflow_ → miss。**这个竞态是实施压力测试才暴露的，门禁是后来补的，不是初始设计。**（此段与 §7.1/7.2 一样，都属于"设计时没想到"。）

**解决**：引入**树化门禁 `state_`**（0=链 / 1=转化中 / 2=已树化）：
- treeify 用 **CAS 0→1 抢占**转化权（防并发触发），复制链后置 2
- **链操作每一步前检查 `state_`**，读到 ≠0（转化中/已树化）→ **放弃链路径，重试进 map**（锁 overflow_lock_）

```cpp
// treeify:
if (!state_[b].compare_exchange_strong(0, 1, acq_rel, relaxed)) return;  // 已被并发转化
lock(overflow_lock_); 复制链进 overflow_;  state_[b].store(2, release);

// insert 链头 CAS 前:
if (state_[b].load(acquire)) {   // 转化已启动
    freeNode(idx);               // 归还节点, 不落链
    lock(overflow_lock_);  overflow_[k]=order;  return;
}
```

**教训**：无锁数据结构里的"路径切换"（链 ↔ 树）是隐藏竞态源，需要一个**原子门禁**让所有操作一致地看到切换。§4.5 的"外部锁"只解决了"STL map 不可控"，没解决"链/树两条路径的选择一致性"。

### 7.4 竞态四（顺带发现）：RiskManager 线程安全（项目预存 bug）

**症状**：`-O2` 压测间歇性段错误（9 次崩 1 次），但不在 OrderMap 代码里——崩溃在 `fill_th`（回报线程）。

**根因**：与 OrderMap 无关的**项目预存 bug**——`RiskManager` 无锁，下单线程（book_th）调 `check_order/position`，回报线程（fill_th）调 `on_fill`，并发读写 `pos_`（std::unordered_map）与 `realized_pnl_`。unordered_map 并发 rehash 时读到悬空迭代器 → 段错误。实测**改动前的 V1 版本也崩**，非本次引入。

**解决**：RiskManager 所有方法内部加 `std::mutex`，自包含线程安全。注意 `check_order` 原来调 `daily_loss_breached()`（也加锁），改为内联判断避免**重入死锁**。

**教训**：压测暴露的崩溃要先确认"是不是我引入的"——用 git diff 确认改动范围 + 跑改动前版本复现，才能准确归因。这个 bug 是排查中顺带发现的。

### 7.5 验证结果

| 验证 | 结果 |
|---|---|
| 并发压力测试（8 线程×512 key×20000） | **5 次全 PASS**（无 miss、无残留、池归零） |
| TSAN（压力测试 + RiskManager 并发） | **无数据竞争告警** |
| 全量 ctest（16 个，含新压力测试） | **16/16 通过** |
| 压测（--rate 10000） | **10 次零丢包**（修复前 9 次崩 1 次） |

---

## 8. 设计时的疏漏与实战修正（小结）

| § | 初始设计 | 实战暴露的问题 | 最终修正 |
|---|---|---|---|
| 2.2 | free_head_ Treiber 栈"ABA 不丢" | 节点复用导致两个线程同时拥有同一节点 | **带 tag CAS** |
| 4.4 | erase 找前驱 CAS 摘除 | 前驱是别人的节点，被并发复用跳链 | **erase 也做 bucket 校验 + 重试** |
| 4.5 | overflowed_ 标志 + 外部锁 | 链/树路径切换竞态，节点落在链上但 find 查 overflow_ | **state_ 门禁** |
| 7.4 | （无关）RiskManager 无锁 | -O2 间歇段错误（预存 bug） | **先加锁跑通，后改无锁原子数组**（见 §9） |

> **一句话**：理论设计解决了"想得到的竞态"（跳链、链头竞争），实战测试暴露了"没想到的竞态"（Treiber ABA、前驱复用、路径切换）——**并发正确性最终是测试逼出来的，不是设计推出来的。**

---

## 9. RiskManager 无锁化：从"快速跑通"到"考虑性能"（追加）

> 本节是 RiskManager 线程安全修复的演进记录。它独立于 OrderMap，但共享同一个设计哲学：**先搞清楚真实并发点，再决定要多少同步。**

### 9.1 背景：一个被"快速修复"掩盖的过度设计

压测暴露 RiskManager 段错误（§7.4）后，第一反应是**加锁快速跑通**：给每个方法加 `std::mutex`。功能对了，但冷静下来发现它是**过度设计**：

- **check_order/on_fill 已被 ExecutionEngine 的 mtx_ 互斥串行化** → 它们之间根本没有竞争
- 我加的 RiskManager 锁是**把本已由 EE 锁串行的路径又锁了一遍**
- 还给高频的 `position()`（arbitrate 每次信号对齐时调用）加了锁开销

### 9.2 真实并发点分析（为什么能无锁）

| 路径 | 线程 | 已有保护 |
|---|---|---|
| `check_order` / `on_fill` | book_th / fill_th | EE `mtx_` 已互斥串行 |
| `position()`（arbitrate 裸调） | book_th | **唯一真正并发点** |

真正的竞态只有一个：`position()`（book_th）读 map vs `on_fill`（fill_th）写 map → unordered_map rehash 时读悬空迭代器 = 段错误根因。**所以只需要让 `position()` 无锁读安全，而不是给整个 RiskManager 上锁。**

### 9.3 无锁方案：locate 即下标的原子数组

```
locate 是 ITCH 16-bit(0-65535) → 固定数组 65536 格, key 即下标, 无哈希冲突
qty_:     std::atomic<uint64_t>[kMaxLocate]   // position 原子读 / on_fill 原子写
avg_cost_: int64_t[kMaxLocate]                // 仅 on_fill 单写者(EE 锁保证), 无需原子
realized_pnl_: std::atomic<int64_t>           // fetch_add / load
```

- `position()` → **单次原子读**（无锁、无 map、无 rehash）
- `on_fill` → **单写者**（EE 锁保证），`qty_` 原子 store
- 移除了 mutex 和 unordered_map

### 9.4 为什么原子读

**原子读防的不是"脏读/幻读"**（那是数据库事务概念），而是：

1. **消除 C++ data race（UB）**：跨线程无同步读写共享变量在 C++ 标准里是未定义行为。不是"读到的值可能旧"，而是"**读这个动作本身让程序 UB**"，编译器可能基于错误假设优化（原段错误 -O2 崩、-O0 不崩，正是这个 UB 的表现）。
2. **防硬件撕裂读**：8 字节 `uint64_t` 在非对齐/32 位平台可能拆成两次内存访问，读到高低位拼凑的怪值。原子保证单次不可分割读。

**它不保证**：读到的一定是最新值。`book_th` 读 `qty_` 可能拿到 fill_th 写之前的旧快照——但风控判断容忍短暂滞后（下单前持仓读数差几十股不影响是否超限），所以用宽松的 acquire 语义。

> **一句话**：原子读是让"并发访问合法"（消除 UB），不是让"读到的值新鲜"。能把这个区分讲清楚，是对并发本质理解到位。

### 9.5 为什么它算"自定义无锁哈希表"

- **哈希表本体**：`locate` → 持仓数据，固定数组当桶（locate 即下标）——最朴素的哈希表形态
- **"自定义"在三个维度**：
  1. **牺牲通用性换极简**：通用无锁哈希表要考虑 resize/冲突链/erase 复用；这里 key 空间固定（65536）、无冲突，全不用处理
  2. **业务约束驱动结构**：从"locate 16-bit"和"EE 锁已串行化 on_fill"推导出只需 `qty_` 原子 + `avg_cost_` 单写者
  3. **与 OrderMap 形成对照**：OrderMap 是"真·无锁哈希表"（分离链接 + 链头 CAS + bucket 校验，处理了冲突和节点复用）；RiskManager 是"伪哈希表"（key 即下标，原子数组）。**两个都是无锁，但复杂度差一个量级——因为业务约束不同**

### 9.6 验证结果

| 验证 | 结果 |
|---|---|
| 全量 ctest（16 个，含 test_execution 期望值） | **16/16 通过** |
| TSAN 真实模式（1 写者 + 4 读者） | **无数据竞争告警** |
| 压测（--rate 10000） | **8 次零丢包**（无崩溃） |

### 9.7 对比：加锁版 vs 无锁版

| | 加锁版（快速跑通） | 无锁版（最终） |
|---|---|---|
| `position()` 热路径 | 每次抢 mutex (~20ns) | **单次原子读 (~1ns)** |
| 数据结构 | unordered_map | 固定数组 (1MB) |
| 锁冗余 | EE 锁 + RiskManager 锁双重 | **无锁** |
| 段错误根因 | 仍在（map 还在） | 消除 |

> **一句话**：先加锁是为了快速验证正确性（功能跑通），改无锁是为了性能（高频 `position()` 热路径）。**"先正确，再快"——用锁把逻辑跑对，再分析真实并发点，把能去掉的锁去掉。**
