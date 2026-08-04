#include "core/prof/lensx_probe.h"

// ── LensX 探针定义 ──
// noinline 保证符号存在 + 参数必须真正传入寄存器(LensX PT_REGS_PARM1 才能读到 key)。
// 函数体强制读 key, 防止 -O3 优化掉参数传递(空函数体时参数可能不进寄存器,
// 导致 LensX key 提取读到垃圾值)。
// 定义在 cpp 而非头文件, 避免多翻译单元重复定义。
#define LENSX_NOINLINE __attribute__((noinline))

namespace lensx {

LENSX_NOINLINE void mark_recv(uint64_t key) { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_s0(uint64_t key)   { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_s2(uint64_t key)   { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_sig(uint64_t key)  { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_book(uint64_t key) { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_obi(uint64_t key)  { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_ofi(uint64_t key)  { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_s5(uint64_t key)   { volatile uint64_t x = key; (void)x; }
LENSX_NOINLINE void mark_s6(uint64_t key)   { volatile uint64_t x = key; (void)x; }

}  // namespace lensx
