#!/usr/bin/env bash
# 压测脚本：纯 UDP 发 + 事后检测丢包 + 调速扫档
#
# 用法:
#   ./scripts/pressure_test.sh [--rate N] [--sweep] [--fast] [数据文件]
#     --rate N    每 0.1s 发 N 包(平滑限速, 每包 sleep 到目标节奏, 瞬时≈平均)。
#                 省略 = 全速。
#     --sweep     自动扫档找临界(book_th 消费能力, ~8.5M msg/s)。
#     --fast      缩短 idle_timeout(10s→4s)加速扫档。
#     数据文件    默认 test_data/itch_100mb.bin
#
# 流程(纯 UDP, 事后检测):
#   1. 起 trader --no-shm (无握手, idle_timeout 无消息后自动退出)
#   2. benchmark --no-shm --rate N 纯 UDP 发(无 ack 反馈)
#   3. trader 自动退出 → 打印解析总数 + 通道丢包(drops_a/drops_b)
#   4. 对比 benchmark 发送数 vs trader 解析总数 + drops → 评估丢包
#
# 零丢判定(纯 UDP 不保证绝对零丢, 靠留余量 + 事后检测):
#   drops_b>0 (内部委托通道丢) → 速率超过订单簿消费能力, 降速。
#   发送==解析 && drops==0         → 零丢, 可提速。
#   标准零丢档: --rate 10000 (~5M msg/s, 方案A单通道下 book_th 处理全部事件, 临界降, 留余量)。
#   这是"不丢包前提下找最快"的扫档流程, 模拟真实行情单向 UDP。
set -u

cd "$(dirname "$0")/.."

DATA_FILE="test_data/itch_100mb.bin"
RATE=""            # 空 = 全速
SWEEP=false
FAST=false         # 快速扫档: 缩短 idle_timeout(10s→2s)加速每档, 但需数据发完后再排空
TRADER=./build/trader
BENCH=./build/trader_benchmark
PORT=8080
ORDER_PORT=9090
RET_PORT=9091
LOG=/tmp/trader_pressure.log
PARSED_FILE=trader_parsed.txt

while [ $# -gt 0 ]; do
    case "$1" in
        --rate) RATE="$2"; shift 2;;
        --sweep) SWEEP=true; shift;;
        --fast) FAST=true; shift;;
        -*) echo "未知参数: $1"; exit 1;;
        *) DATA_FILE="$1"; shift;;
    esac
done

echo "═══ NebulaX-Trader 压测(纯 UDP + 事后检测) ═══"
echo "数据文件: $DATA_FILE ($(du -h "$DATA_FILE" 2>/dev/null | cut -f1))"
echo "行情端口: $PORT  模拟交易所: $ORDER_PORT  回报: $RET_PORT"
[ -n "$RATE" ] && echo "限速: 每0.1s发 $RATE 包(--rate)" || echo "限速: 全速(无 --rate)"

# 跑一轮压测, 输出 benchmark 发送数 + trader 汇总
# 参数: 速率(0=全速, 其他=每0.1s包数)
run_once() {
    local rate="$1"
    pkill -x trader 2>/dev/null || true
    pkill -x trader_benchmark 2>/dev/null || true
    rm -f /dev/shm/nx_trader_flow "$PARSED_FILE"
    sleep 1

    # 起 trader (no-shm, 无握手)。--fast 时用短 idle_timeout 加速扫档。
    local cfg=config/default.yaml
    if [ "$FAST" = true ]; then
        cfg=/tmp/trader_fast.yaml
        sed 's/idle_timeout_sec: 10/idle_timeout_sec: 4/' config/default.yaml > "$cfg"
    fi
    $TRADER --config "$cfg" --no-shm > "$LOG" 2>&1 &
    local TRADER_PID=$!
    sleep 1.5
    if ! kill -0 $TRADER_PID 2>/dev/null; then
        echo "trader 启动失败:"; cat "$LOG"; exit 1
    fi

    # 起 benchmark (纯 UDP)
    local bench_args=(--file "$DATA_FILE" --port $PORT --order-port $ORDER_PORT \
                      --order-ret-port $RET_PORT --no-shm)
    if [ "$rate" -gt 0 ]; then bench_args+=(--rate "$rate"); fi
    "$BENCH" "${bench_args[@]}" > /tmp/bench.log 2>&1

    # 等 trader 自动退出 (idle_timeout 无消息, 默认10s / --fast 2s)
    local max_wait=$([ "$FAST" = true ] && echo 12 || echo 40)
    for i in $(seq 1 $max_wait); do
        if ! kill -0 $TRADER_PID 2>/dev/null; then break; fi
        sleep 1
    done
    kill -0 $TRADER_PID 2>/dev/null && kill -9 $TRADER_PID 2>/dev/null || true
    wait $TRADER_PID 2>/dev/null || true
}

# 从日志读一个数字指标: $1=匹配行关键字, $2=字段分割符(如 "=" 或 "丢")
grab() { grep -E "$1" "$LOG" | head -1 | awk -F"$2" '{print $2}' | tr -dc '0-9'; }

if [ "$SWEEP" = true ]; then
    echo ""
    echo "═══ 自动调速扫档(不丢包前提下找最快) ═══"
    # 二分: lo 一定不丢, hi 一定丢。从速率 200 包/0.1s 起步。
    lo=100 hi=0 best=0
    rate=200
    for iter in $(seq 1 12); do
        echo ""
        echo "── 档位 $iter: --rate $rate (每0.1s包) ──"
        run_once "$rate"
        sent=$(grep "Messages sent" /tmp/bench.log 2>/dev/null)
        parsed=$(grab "解析总数" '=')
        drops_b=$(grab "通道B丢" '丢=')
        drops_a=$(grab "通道A丢" '丢=')
        [ -z "$drops_b" ] && drops_b=0
        [ -z "$parsed" ] && parsed=0
        echo "  发送=${sent:-?} 解析=$parsed 通道A丢=${drops_a:-0} 通道B丢=$drops_b"

        if [ "$drops_b" -eq 0 ] && [ "$parsed" -gt 0 ]; then
            best=$rate
            echo "  ✅ 不丢包. 记录最好速率=$best"
            if [ "$hi" -eq 0 ] || [ "$rate" -ge "$hi" ]; then lo=$rate; rate=$((rate * 2)); else rate=$(((lo + hi) / 2)); fi
        else
            echo "  ❌ 丢包($drops_b). 降速"
            hi=$rate
            rate=$(((lo + hi) / 2))
        fi
        if [ "$rate" -le "$best" ]; then rate=$((best + best / 2)); fi
        [ "$rate" -le 0 ] && break
        [ "$hi" -gt 0 ] && [ "$((hi - lo))" -le 100 ] && break
    done
    echo ""
    echo "═══ 扫档完成: 不丢包前提下最快 = --rate $best ═══"
else
    rate=0
    [ -n "$RATE" ] && rate=$RATE
    echo ""
    echo "── 压测: $([ "$rate" -gt 0 ] && echo "--rate $rate" || echo "全速") ──"
    run_once "$rate"
    echo ""
    echo "═══ 压测汇总 ═══"
    grep -E "解析总数|解析 QPS|成交事件|委托事件|主策略|OFI|OBI|订单:|持仓|运行汇总" "$LOG" | sed 's/^/  /'
    echo ""
    echo "── 事后丢包检测 ──"
    sent=$(grep "Messages sent" /tmp/bench.log 2>/dev/null)
    echo "  benchmark 发送: ${sent:-?}"
    parsed=$(grab "解析总数" '=')
    drops_b=$(grab "通道B丢" '丢=')
    [ -z "$drops_b" ] && drops_b=0
    echo "  trader 解析: ${parsed:-?}  通道B丢=$drops_b"
    if [ "$drops_b" -eq 0 ]; then echo "  零内部丢包 ✅"; else echo "  内部丢包 ❌ ($drops_b)"; fi
fi
