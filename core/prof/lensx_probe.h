#pragma once

#include <cstdint>

// ── LensX eBPF 延迟追踪探针 ──
// noinline 空函数: 只作为 uprobe 挂载点(符号必须存在且不内联, LensX 才能 attach)。
// key 参数: 跨线程配对用。同一条消息在多个阶段传同一 key。
//
// 两级测量(以一条消息/一个包为单位, 从分配 seq 到被消费的完整旅程):
//
// 级别1 — 包级 recv_th 内部延迟(seq 模式, 同线程, 无 key)
//   recv_pkt → unpack       IoUringReceiver::recv 返回 → MoldUdpUnpacker::feed 拆包开始
//   测: 包到达 → 开始拆包(内核→用户态拷贝 + 提交 SQE 的开销)
//   包级粒度: recv 每包 1 次, feed 每包 1 次, 1:1 seq 配对。
//
// 级别2 — 消息级完整链路(key 模式, 跨线程, key = 消息 seq, 抽样)
//   alloc → pop             分配 seq → SPMC 消费 pop 出
//   测: 一条消息从分配到被消费的完整旅程(含跨线程排队)
//   key = unpacker 分配的 64 位消息 seq, 跨线程靠 MarketEvent.seq_id 贯穿。
//   抽样: 每 N 条打 1 条(alloc/pop 独立计数器, 各按序号 mod N 判定, 抽中同一批)。
//     全采样(每消息打)实测吞吐从 5M 跌到 543K(9x), 探针拖垮高频线程 → 延迟被污染。
//     抽样把探针开销压到 ~1/N, 延迟才可信。
//
// 定义在 lensx_probe.cpp(避免头文件多翻译单元重复定义)。
// -O3 下仅一次非内联函数调用开销, 测量完可移除插桩。
namespace lensx {

// 消息级抽样比: 每 kSample 条消息打 1 条(msg_seq % kSample == 0)。
// 调用点也按此判断(跳过调用), 探针开销压到 ~1/kSample。
constexpr uint64_t kSample = 128;

// 级别1 包级(recv_th 内部, seq 模式, 无 key)
void mark_recv_pkt();     // IoUringReceiver::recv 返回后(数据已进用户 buf)
void mark_unpack();       // MoldUdpUnpacker::feed 拆包开始前(每包一次)

// 级别2 消息级完整链路(key 模式, key = 消息 seq, 抽样)
void mark_alloc(uint64_t key);   // 分配 msg_seq 时(unpacker, 每 N 条打 1 条)
void mark_pop(uint64_t key);     // 消息被归属 worker 从 SPSC 取走时(worker 循环, process 前)
void mark_process(uint64_t key); // 消息开始被归属 worker 处理时(BookWorker::process 开头)
// 级别2 细化(V3 分发器版): alloc→process 拆 5 段 + retry 桶路径(key = 消息 seq, 抽样)
//   alloc → push_ring         构造+入字节 ring (recv_th 内)
//   push_ring → parse_done     跨线程①: ring→解析 (recv_th→parse_th, 含唤醒/调度/排队)
//   parse_done → dispatch      解析→分发器 (parse_th 内, 含 registry 查表 + push 决策)
//   dispatch → pop             跨线程②: 分发到 spsc[owner]→worker 取走 (含唤醒/调度/排队)
//   pop → process              取走到处理 (worker 线程内, 应极小——证明长尾在排队不在处理)
//   retry 桶路径(慢消费者隔离触发时):
//   dispatch → retry_in        事件进 retry 桶(满/active, 保序前提)
//   retry_in → retry_out       桶→retry 线程推回 spsc (retry 线程)
//   retry_out → pop            推回 spsc→worker 取走 (跨线程, 测慢消费者隔离代价)
void mark_push_ring(uint64_t key);    // 字节 ring push 后 (unpacker)
void mark_parse_done(uint64_t key);   // 解析完成 (ItchParser::emit)
void mark_dispatch(uint64_t key);     // 分发器 dispatch 入口 (ByteRingParser sink → Dispatcher)
void mark_retry_in(uint64_t key);     // 事件进 retry 桶 (Dispatcher::dispatch, 满/active 时)
void mark_retry_out(uint64_t key);    // retry 线程推回 spsc (retry 线程循环)

// 级别3 仲裁函数完整执行耗时(seq 模式, 无 key)
//   arb_start → arb_end  arbitrate 开头 → 统一出口(done:)
//   单线程顺序执行, 无并发, seq 配对。arbitrate 改单出口(提前 return → 统一落 done:),
//   保证开头打的起点、结尾必打终点, 无残留。
void mark_arb_start();    // arbitrate 开头(每次进仲裁)
void mark_arb_end();      // arbitrate 统一出口(done:)

// 级别4 下单决策→执行完毕(key 模式, key = sig_ofi.seq, 抽样)
//   order_start → order_end  决策点(fresh块内, submit_signal前) → 决策块末尾(无论 send/被拒)
//   测: 下单决策 → 执行完毕(send 或被拒)。key=信号触发 seq(sig_ofi.seq)。
//   起点/终点都在决策块, 成对无残留; send 和被拒都是终点状态。
void mark_order_start(uint64_t key);   // 决策点: fresh 块内, submit_signal 前
void mark_order_end(uint64_t key);     // 决策块末尾: send 或被拒都打

}  // namespace lensx
