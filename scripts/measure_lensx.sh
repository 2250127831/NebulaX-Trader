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
# SIGINT 触发 lensx 输出 Results(默认 SIGTERM 不输出)
S pkill -INT -x lensx 2>/dev/null || true
sleep 2
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
echo ""
echo "=== CSV 离线分析(P50/P99/P999, 剔除异常) ==="
# CSV 是完整原始数据, 比终端 Results 可靠(Results 可能被 SIGINT 截断样本)。
# 先 chown 让当前用户能读(CSV 是 root 写的)
echo "$SUDO_PASS" | sudo -S chown "$(whoami)" /tmp/trader_lensx.csv 2>/dev/null || true
python3 - <<'PYEOF'
import csv
SEG = {
    (0,1):'recv->unpack', (2,3):'alloc->push_ring', (3,4):'push_ring->parse',
    (4,5):'parse->dispatch', (5,8):'dispatch->pop', (8,9):'pop->process',
    (2,8):'alloc->pop', (2,9):'alloc->process(total)',
    (5,6):'dispatch->retry_in', (6,7):'retry_in->retry_out', (7,8):'retry_out->pop',
    (6,8):'retry->pop', (10,11):'arb', (12,13):'order',
}
groups = {}
for r in csv.DictReader(open('/tmp/trader_lensx.csv')):
    k = (int(r['from_stage']), int(r['to_stage']))
    if k in SEG:
        try: v = int(r['delta_ns'])
        except: continue
        if v > 10**12: continue  # 剔除异常值(211106s)
        groups.setdefault(k, []).append(v)
def pct(d, p):
    d = sorted(d)
    return d[min(len(d)-1, int(len(d)*p))]
for k, name in SEG.items():
    d = groups.get(k, [])
    if not d: continue
    print(f"{name:16s} n={len(d):8d}  P50={pct(d,0.5)/1e3:8.1f}us  P99={pct(d,0.99)/1e3:9.1f}us  P999={pct(d,0.999)/1e3:9.1f}us")
PYEOF
