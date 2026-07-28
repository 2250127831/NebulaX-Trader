#pragma once

#include <cstddef>

// ── cache line 对齐 ──
constexpr size_t CACHE_LINE_SIZE = 64;
#define CACHE_ALIGN alignas(CACHE_LINE_SIZE)
