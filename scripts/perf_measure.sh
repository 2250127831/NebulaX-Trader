#!/usr/bin/env bash
# NebulaX-Trader: 压测窗口 perf record + perf stat, 按样本时间戳裁剪空闲期
# 学 NebulaX nebulaX_bench.sh: perf record 全程采, 取 2.5%~97.5% 时间窗口过滤
set -u
cd /home/qiwang/NebulaX-Trader
RATE=${1:-10000}
# 每个 sudo 都带密码(不依赖缓存, -S 从 stdin 读)。管道接 sudo 会吞 stdin, 故用临时文件。
SPW="${SUDO_PASS:-}"
S(){ echo "$SPW" | sudo -S "$@" 2>/dev/null; }
PERF_DATA=/tmp/trader_perf.data
PERF_STAT=/tmp/trader_perf_stat.txt

pkill -x trader 2>/dev/null || true
pkill -x trader_benchmark 2>/dev/null || true
S rm -f /dev/shm/nx_trader_flow trader_parsed.txt "$PERF_DATA" "$PERF_STAT" 2>/dev/null || true
sleep 1

./build/trader --no-shm > /tmp/trader_run.log 2>&1 &
TPID=$!
sleep 1.5

# perf record 全程采(热点/火焰图)
S perf record -F 999 --call-graph dwarf -e cpu-clock -p $TPID -o "$PERF_DATA" 2>/dev/null &
REC_PID=$!
# perf stat 硬件事件 + syscall 计数。用 timeout 限时 8s, 到时自动结束写文件(不被 pkill 杀断)。
# 必须 -s INT: timeout 默认 SIGTERM 会让 perf stat 不 flush 文件, SIGINT 才优雅收尾。
S timeout -s INT 8 perf stat -e context-switches,cycles,instructions,cache-misses,cache-references,branch-misses,L1-dcache-load-misses,L2-load-misses,syscalls:sys_enter_sendto,syscalls:sys_enter_recvfrom,syscalls:sys_enter_read -p $TPID -o "$PERF_STAT" 2>/dev/null &
sleep 0.5

./build/trader_benchmark --file test_data/itch_100mb.bin --no-shm --rate $RATE > /tmp/bench.log 2>&1
sleep 7   # 让 perf record 采到负载段, 同时等 timeout 8s 的 perf stat 自然结束写文件

# 只杀 perf record(stat 已由 timeout 结束)
S pkill -x perf 2>/dev/null || true
sleep 2   # 等 perf record 完全退出, flush 数据文件(否则读到半截)

# 把 perf 输出文件属主改回当前用户, 后续读取无需 sudo(管道里 sudo -S 会丢 stdin)
echo "$SPW" | sudo -S chown "$(whoami)" "$PERF_DATA" "$PERF_STAT" 2>/dev/null || true

# 裁剪空闲期: 取样本时间戳 2.5%~97.5%。perf.data 已 chown, 直接读。
# 先转成可读的 time 列表(避免管道里 sudo 读 stdin 问题: sudo 重定向到文件再读)
perf script -i "$PERF_DATA" -F time > /tmp/trader_perf_times.txt 2>/dev/null
TIME_RANGE=$(awk 'NF{t[++n]=$1} END{if(n>20){p5=t[int(n*0.025)]; p95=t[int(n*0.975)]; printf "%f,%f", p5, p95} else printf "0,%f", t[n]}' /tmp/trader_perf_times.txt 2>/dev/null)
echo "裁剪窗口: $TIME_RANGE"

echo ""
echo "=== 硬件事件(perf stat) ==="
cat "$PERF_STAT" > /tmp/trader_perf_stat_read.txt 2>/dev/null
grep -vE "<not counted>|cpu_atom/|^#|^$|Performance counter" /tmp/trader_perf_stat_read.txt 2>/dev/null | sed 's/  #.*$//'
echo ""
echo "=== IPC / ctx/s ==="
INST=$(awk '/instructions/{gsub(/,/,"",$1); print $1; exit}' /tmp/trader_perf_stat_read.txt)
CYCLES=$(awk '/cycles/{gsub(/,/,"",$1); print $1; exit}' /tmp/trader_perf_stat_read.txt)
CTX=$(awk '/context-switches/{gsub(/,/,"",$1); print $1; exit}' /tmp/trader_perf_stat_read.txt)
SEC=$(awk '/seconds time elapsed/{print $1; exit}' /tmp/trader_perf_stat_read.txt)
[ -n "$INST" ] && [ -n "$CYCLES" ] && [ "$CYCLES" != "0" ] && echo "IPC = $(awk "BEGIN{printf \"%.2f\", $INST/$CYCLES}")"
[ -n "$CTX" ] && [ -n "$SEC" ] && [ "$SEC" != "0" ] && echo "ctx/s = $(awk "BEGIN{printf \"%.0f\", $CTX/$SEC}")"
echo ""
echo "=== CPU 热点 Top 15(裁剪窗口内) ==="
perf report -i "$PERF_DATA" --time "$TIME_RANGE" --stdio --no-header > /tmp/trader_perf_report.txt 2>/dev/null
grep -v '^ *#' /tmp/trader_perf_report.txt 2>/dev/null | head -18 | sed 's/^/  /'
echo ""
echo "=== trader 汇总 ==="
grep -E "解析 QPS|解析总数" /tmp/trader_run.log
echo "=== 压测对比 ==="
grep "Messages sent" /tmp/bench.log
S pkill -x trader 2>/dev/null || true
