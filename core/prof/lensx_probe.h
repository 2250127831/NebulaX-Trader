#pragma once

#include <cstdint>

// ── LensX eBPF 延迟追踪探针 ──
// noinline 空函数: 只作为 uprobe 挂载点(符号必须存在且不内联, LensX 才能 attach)。
// key 参数: 跨线程配对用(消息 seq)。同一条消息在多个阶段传同一 key。
//
// 五类延迟测量(见 docs/PERF_V1_ARCHIVE.md 第 5 节):
//   第一类 收包→入ring    mark_recv (recv_th) → mark_s0 (recv_th)
//   第二类 通道A→信号     mark_s2   (strategy_th) → mark_sig (strategy_th)
//   第三类 通道B→OBI      mark_book (book_th) → mark_obi (book_th)
//   第四类 通道B→OFI      mark_book (book_th) → mark_ofi (book_th)
//   第五类 合成→send      mark_s5   (strategy_th) → mark_s6 (strategy_th)
//
// 定义在 lensx_probe.cpp(避免头文件多翻译单元重复定义)。
// -O3 下仅一次非内联函数调用开销, 测量完可移除插桩。
namespace lensx {

void mark_recv(uint64_t key);   // 第一类起点: recv 收到 UDP 包
void mark_s0(uint64_t key);     // 第一类终点: 消息推入 ring
void mark_s2(uint64_t key);     // 第二类起点: 策略线程 pop 成交
void mark_sig(uint64_t key);    // 第二类终点: 信号合成完成(combine 后)
void mark_book(uint64_t key);   // 第三/四类起点: 订单簿线程 pop 委托
void mark_obi(uint64_t key);    // 第三类终点: OBI 信号更新完成
void mark_ofi(uint64_t key);    // 第四类终点: OFI 信号更新完成
void mark_s5(uint64_t key);     // 第五类起点: 合成成功触发下单(fresh)
void mark_s6(uint64_t key);     // 第五类终点: send 发出

}  // namespace lensx
