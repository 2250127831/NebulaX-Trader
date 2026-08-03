#!/usr/bin/env bash
# 压测脚本：起 trader + benchmark，真实压测完整数据
#
# 用法:
#   ./scripts/pressure_test.sh [数据文件] [rate]
#     数据文件  默认 test_data/itch_100mb.bin (246MB 完整日数据)
#     rate      每 0.1 秒发送包数(窗口限速, 0=全速, 默认 0)
#
# 流程:
#   1. 清理残留 trader/benchmark 进程 + 旧解析结果文件
#   2. 起 trader --no-shm (无共享内存, 10秒无消息→写解析总数到文件→自动关闭)
#   3. 等 5 秒让 trader 就绪
#   4. 起 benchmark --no-shm --rate N (窗口限速发数据)
#   5. benchmark 发完退出, trader 10秒无消息后写 trader_parsed.txt 并关闭
#   6. 读 trader_parsed.txt 得解析总数, 与 benchmark 发送数对比(压测评估)
#
# 限速 = 固定窗口限速(每 0.1 秒 N 包), 解析数只作压测结果评估(无实时反馈)。
set -u

cd "$(dirname "$0")/.."

DATA_FILE="${1:-test_data/itch_100mb.bin}"
RATE="${2:-0}"
TRADER=./build/trader
BENCH=./build/trader_benchmark
PORT=8080
ORDER_PORT=9090
RET_PORT=9091
START_WAIT=5        # 起 trader 后等待秒数再发数据
NO_MSG_TIMEOUT=10   # trader 无消息秒数(写解析数并关闭)
PARSED_FILE=trader_parsed.txt
LOG=/tmp/trader_pressure.log

echo "═══ NebulaX-Trader 压测 ═══"
echo "数据文件: $DATA_FILE ($(du -h "$DATA_FILE" 2>/dev/null | cut -f1))"
echo "行情端口: $PORT  模拟交易所: $ORDER_PORT  回报: $RET_PORT"
echo "限速: ${RATE} 包/0.1s (0=全速)  无消息超时: ${NO_MSG_TIMEOUT}s"

# ── 清理残留 ──
echo "清理残留进程 + 旧解析结果..."
pkill -x trader 2>/dev/null || true
pkill -x trader_benchmark 2>/dev/null || true
rm -f "$PARSED_FILE"
sleep 1

# ── 起 trader (--no-shm) ──
echo "启动 trader (--no-shm)..."
$TRADER --config config/default.yaml --no-shm > "$LOG" 2>&1 &
TRADER_PID=$!
sleep 1
if ! kill -0 $TRADER_PID 2>/dev/null; then
    echo "trader 启动失败:"; cat "$LOG"; exit 1
fi

# ── 等 trader 就绪 ──
echo "等待 ${START_WAIT}s 让 trader 就绪..."
sleep $START_WAIT

# ── 起 benchmark (--no-shm --rate 窗口限速) ──
echo "启动 benchmark 压测 (rate=$RATE 包/0.1s)..."
T0=$(date +%s.%N)
$BENCH --file "$DATA_FILE" --port $PORT --order-port $ORDER_PORT \
       --order-ret-port $RET_PORT --no-shm --rate "$RATE"
T1=$(date +%s.%N)
BENCH_SEC=$(echo "$T1 - $T0" | bc 2>/dev/null || echo "?")

# ── 等 trader 收到第一条消息后, 无消息 NO_MSG_TIMEOUT 秒自动关闭(写解析数到文件) ──
# benchmark 发完 → trader 再等 10s 无消息才关。等待上限要覆盖:
#   限速发送时长(rate 越小越久) + 无消息超时。
echo "等待 trader 无消息超时后自动关闭..."
MAX_WAIT=180   # 覆盖 全速(2s)+10s 到 慢速限速(任意时长)+10s
for i in $(seq 1 $MAX_WAIT); do
    if ! kill -0 $TRADER_PID 2>/dev/null; then break; fi
    sleep 1
done
if kill -0 $TRADER_PID 2>/dev/null; then
    echo "警告: trader 超时未退出(>${MAX_WAIT}s), 强制 kill"
    kill -9 $TRADER_PID 2>/dev/null || true
fi
wait $TRADER_PID 2>/dev/null || true

echo ""
echo "═══ 压测汇总 ═══"
echo "benchmark 发送耗时: ${BENCH_SEC}s"
# 解析总数(压测评估): trader 写入的文件
if [ -f "$PARSED_FILE" ]; then
    PARSED=$(cat "$PARSED_FILE")
    echo "trader 解析总数: $PARSED (写入 $PARSED_FILE)"
else
    echo "警告: 未找到 $PARSED_FILE (trader 可能没跑完)"
fi
grep -E "成交事件|委托事件|主策略|OFI|OBI|订单:|持仓|运行汇总|10 秒无消息" "$LOG" | sed 's/^/  /'
