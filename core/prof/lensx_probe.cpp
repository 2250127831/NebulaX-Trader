#include "core/prof/lensx_probe.h"

// ── LensX 探针定义 ──
// noinline 保证符号存在 + 参数必须真正传入寄存器(LensX PT_REGS_PARM1 才能读到 key)。
// 函数体强制读 key, 防止 -O3 优化掉参数传递(空函数体时参数可能不进寄存器,
// 导致 LensX key 提取读到垃圾值)。
// 定义在 cpp 而非头文件, 避免多翻译单元重复定义。
#define LENSX_NOINLINE __attribute__((noinline))

namespace lensx {

// 级别1 包级(recv_th 内部, seq 模式, 无 key)
LENSX_NOINLINE void mark_recv_pkt() { asm volatile("" ::: "memory"); }
LENSX_NOINLINE void mark_unpack()   { asm volatile("" ::: "memory"); }

// 级别2 消息级完整链路(key 模式, key = 消息 seq, 抽样)
LENSX_NOINLINE void mark_alloc(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_pop(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_process(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
// 级别2 细化: alloc→pop 拆 4 段(key = 消息 seq, 抽样, 与 alloc/pop 相同 kSample)
LENSX_NOINLINE void mark_push_ring(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_parse_done(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_dispatch(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_retry_in(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_retry_out(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}

// 级别3 仲裁函数完整执行耗时(seq 模式, 无 key)
// 抽样判断在调用点(main.cpp arbitrate 内, 抽中才调本函数), 函数体只需 noinline 防优化。
LENSX_NOINLINE void mark_arb_start() { asm volatile("" ::: "memory"); }
LENSX_NOINLINE void mark_arb_end()   { asm volatile("" ::: "memory"); }

// 级别4 下单决策→执行完毕(key 模式, key = sig_ofi.seq, 抽样)
// 抽样判断在调用点(决策块内, 抽中才调), 函数体只需 noinline + 读 key 防优化。
LENSX_NOINLINE void mark_order_start(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}
LENSX_NOINLINE void mark_order_end(uint64_t key) {
    if (key % kSample == 0) { volatile uint64_t x = key; (void)x; }
}

}  // namespace lensx
