#pragma once

#include "core/types.h"
#include <string_view>

// Tick 解析器：将原始行情消息解析为 Tick 结构体
struct TickParser {
    static Tick parse(const std::string_view& raw);
};
