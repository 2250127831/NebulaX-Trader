#!/usr/bin/env bash
# 压测脚本：起 trader + benchmark，真实压测完整数据
#
# 用法:
#   ./scripts/pressure_test.sh [数据文件] [pace_us]
#     数据文件  默认 test_data/itch_100mb.bin (246MB 完整日数据)
#     pace_us   每包间隔微秒(0 = 全速，默认)
#
# 流程:
#   1. 清理残留 trader/benchmark 进程
#   2. 起 trader --no-shm (不挂共享内存，运行 idle_timeout_sec 秒后退出)
#   3. 等 5 秒让 trader 就绪
#   4. 起 benchmark --no-shm (不挂共享内存，全速/pace 限速发数据)
#   5. benchmark 发完退出，trader 定时结束后汇总
#
# 无共享内存握手: 压测脚本模式只要求"起 trader → 等 → 发数据"，
# 不依赖 received/sent 同步(那是测试模式的需求)。彻底避免握手死锁。
set -u

cd "$(dirname "$0")/.."

DATA_FILE="${1:-test_data/itch_100mb.bin}"
PACE_US="${2:-0}"
TRADER=./build/trader
BENCH=./build/trader_benchmark
PORT=8080
ORDER_PORT=9090
RET_PORT=9091
IDLE_SEC=5          # trader 运行秒数(等数据 + 处理)
START_WAIT=5        # 起 trader 后等待秒数再发数据
LOG=/tmp/trader_pressure.log

echo "═══ NebulaX-Trader 压测 ═══"
echo "数据文件: $DATA_FILE ($(du -h "$DATA_FILE" 2>/dev/null | cut -f1))"
echo "行情端口: $PORT  模拟交易所: $ORDER_PORT  回报: $RET_PORT"
echo "每包间隔: ${PACE_US}us (0=全速)  trader运行: ${IDLE_SEC}s"

# ── 清理残留 ──
echo "清理残留进程..."
pkill -x trader 2>/dev/null || true
pkill -x trader_benchmark 2>/dev/null || true
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

# ── 起 benchmark (--no-shm) ──
echo "启动 benchmark 压测..."
T0=$(date +%s.%N)
$BENCH --file "$DATA_FILE" --port $PORT --order-port $ORDER_PORT \
       --order-ret-port $RET_PORT --no-shm --pace-us "$PACE_US"
T1=$(date +%s.%N)
BENCH_SEC=$(echo "$T1 - $T0" | bc 2>/dev/null || echo "?")

# ── 等 trader 自然退出(idle_timeout=30s 覆盖 等5s+发送+处理，汇总后退出) ──
echo "等待 trader 处理完并输出汇总..."
# 最多等 40s(30s idle + 余量)。若超时说明卡住，kill 并提示。
for i in $(seq 1 40); do
    if ! kill -0 $TRADER_PID 2>/dev/null; then break; fi
    sleep 1
done
if kill -0 $TRADER_PID 2>/dev/null; then
    echo "警告: trader 超时未退出, 强制 kill"
    kill -9 $TRADER_PID 2>/dev/null || true
fi
wait $TRADER_PID 2>/dev/null || true

echo ""
echo "═══ 压测汇总 ═══"
echo "benchmark 发送耗时: ${BENCH_SEC}s"
grep -E "成交事件|委托事件|主策略|OFI|OBI|订单:|持仓|运行汇总" "$LOG" | sed 's/^/  /'
