#!/usr/bin/env bash
# NebulaX-Trader: 起 trader → attach LensX → 压测 → 汇总
# LensX eBPF 延迟测量一键脚本。
#
# 用法:
#   SUDO_PASS=<密码> ./scripts/measure_lensx.sh [rate] [yaml]
#     rate  每 0.1s 发包数(默认 10000, 标准零丢档)
#     yaml  LensX 配置(默认 docs/bench/trader_lensx.yaml)
#
# 前提:
#   sudo 需要 root(eBPF 加载)。提供 SUDO_PASS 环境变量, 或用已缓存的 sudo 凭据。
#   LensX 二进制在 /home/qiwang/LensX/build/lensx(或设 LENSX 环境变量覆盖)。
set -u
cd "$(dirname "$0")/.."
LENSX=${LENSX:-/home/qiwang/LensX/build/lensx}
YAML=${2:-docs/bench/trader_lensx.yaml}
RATE=${1:-10000}

S(){ echo "$SUDO_PASS" | sudo -S "$@" 2>/dev/null; }

pkill -x trader 2>/dev/null || true
pkill -x trader_benchmark 2>/dev/null || true
S rm -f /dev/shm/nx_trader_flow trader_parsed.txt /tmp/trader_lensx.csv 2>/dev/null || true
sleep 1

./build/trader --no-shm > /tmp/trader_run.log 2>&1 &
TRADER_PID=$!
sleep 1.5
if ! kill -0 $TRADER_PID 2>/dev/null; then echo "trader 启动失败:"; cat /tmp/trader_run.log; exit 1; fi

S $LENSX run $YAML --pid $TRADER_PID > /tmp/lensx.log 2>&1 &
sleep 1.5

./build/trader_benchmark --file test_data/itch_100mb.bin --no-shm --rate $RATE > /tmp/bench.log 2>&1

# 等 trader 自动退出(no-shm idle 10s)
for i in $(seq 1 40); do
  kill -0 $TRADER_PID 2>/dev/null || break
  sleep 1
done
sleep 3   # 排空 lensx ring buffer
S pkill -x lensx 2>/dev/null || true
wait $TRADER_PID 2>/dev/null || true
echo ""
echo "=== trader 汇总 ==="
grep -E "解析总数|解析 QPS|成交事件|订单:|持仓|运行汇总|=== 运行汇总 ===" /tmp/trader_run.log
echo ""
echo "=== 压测对比 ==="
grep "Messages sent" /tmp/bench.log
echo ""
echo "=== lensx 输出 ==="
cat /tmp/lensx.log
