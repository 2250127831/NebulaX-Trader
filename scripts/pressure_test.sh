#!/usr/bin/env bash
# 压测脚本：起 trader + benchmark，共享内存协同压测（不允许积压包）
#
# 用法:
#   ./scripts/pressure_test.sh [数据文件]
#     数据文件  默认 test_data/itch_100mb.bin (246MB 完整日数据)
#
# 流程:
#   1. 清理残留 trader/benchmark 进程 + 旧解析结果文件
#   2. 起 trader (默认共享内存模式, benchmark 发完置 done 自动关闭)
#   3. 起 benchmark --backlog 0 (发一个等一个, 不允许积压包)
#   4. benchmark 发完 → 置 done → trader 自动关闭
#   5. 读 trader 汇总, 与 benchmark 发送数对比(零丢失评估)
#
# 不允许积压包(backlog=0): 发送端永不领先接收端, 零丢失测真实 QPS。
# 这是"协同压测"模式(共享内存握手), 区别于 no-shm 全速压测。
set -u

cd "$(dirname "$0")/.."

DATA_FILE="${1:-test_data/itch_100mb.bin}"
TRADER=./build/trader
BENCH=./build/trader_benchmark
PORT=8080
ORDER_PORT=9090
RET_PORT=9091
LOG=/tmp/trader_pressure.log

echo "═══ NebulaX-Trader 压测(协同, 不允许积压) ═══"
echo "数据文件: $DATA_FILE ($(du -h "$DATA_FILE" 2>/dev/null | cut -f1))"
echo "行情端口: $PORT  模拟交易所: $ORDER_PORT  回报: $RET_PORT"
echo "限速: 发一个等一个(backlog=0, 不允许积压包)"

# ── 清理残留 + 共享内存 ──
echo "清理残留进程 + 共享内存..."
pkill -x trader 2>/dev/null || true
pkill -x trader_benchmark 2>/dev/null || true
rm -f /dev/shm/nx_trader_flow
sleep 1

# ── 起 trader (共享内存模式) ──
echo "启动 trader (共享内存模式)..."
$TRADER --config config/default.yaml > "$LOG" 2>&1 &
TRADER_PID=$!
sleep 1
if ! kill -0 $TRADER_PID 2>/dev/null; then
    echo "trader 启动失败:"; cat "$LOG"; exit 1
fi

# ── 起 benchmark (backlog=0, 不允许积压) ──
echo "启动 benchmark 压测 (发一个等一个)..."
T0=$(date +%s.%N)
$BENCH --file "$DATA_FILE" --port $PORT --order-port $ORDER_PORT \
       --order-ret-port $RET_PORT --backlog 0
T1=$(date +%s.%N)
BENCH_SEC=$(echo "$T1 - $T0" | bc 2>/dev/null || echo "?")

# ── 等 trader 自动关闭(done 握手后) ──
echo "等待 trader 处理完并自动关闭..."
MAX_WAIT=60
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
grep -E "解析 QPS|解析 [0-9]+ 条|成交事件|委托事件|主策略|OFI|OBI|订单:|持仓|运行汇总" "$LOG" | sed 's/^/  /'
