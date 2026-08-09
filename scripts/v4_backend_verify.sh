#!/usr/bin/env bash
# V4 三后端主程序正确性验证（直接作用在 trader 主线程上）
#
# 每个后端: 起 trader(--config, shm 握手) → 起 benchmark(发到对应网络载体) →
#           shm done 后 trader 自动退出 → 对比 benchmark "Messages sent" vs trader 解析总数。
# 正确性 = sent == parsed（零丢失）+ 网络载体为完整 L2 帧链路（AF_XDP/DPDK 内部剥帧头）。
#
# 后端 + 网络载体:
#   io_uring: benchmark → 127.0.0.1:port(内核 UDP socket 剥头)
#   af_xdp  : veth 对(veth0 挂 XDP+xsk 收, veth1 发包) → 完整 L2 帧 → recv() 剥头
#   dpdk    : net_tap0 vdev(内核 tap dtap0) → 完整 L2 帧 → recv() 剥头
#
# 用法:
#   SUDO_PASS=<密码> ./scripts/v4_backend_verify.sh [itch_file] [port]
#   itch_file 默认 test_data/itch_chain_sample.bin(12932 非R消息)
#   port      默认 8080
#
# 需要 root(建 veth / 绑 xsk / DPDK 运行)。trader 与 benchmark 都用 root 跑(shm 权限一致)。
set -u
cd "$(dirname "$0")/.."
S(){ echo "$SUDO_PASS" | sudo -S "$@" 2>/dev/null; }

ITCH_FILE=${1:-test_data/itch_chain_sample.bin}
PORT=${2:-8080}
SRC_IP=10.0.0.2
DST_IP=10.0.0.99
FAIL=0

cleanup() {
    pkill -x trader 2>/dev/null || true
    pkill -x trader_benchmark 2>/dev/null || true
    S shm_unlink /nx_trader_flow 2>/dev/null || true
    S ip link del veth0 2>/dev/null || true
    S ip link del dtap0 2>/dev/null || true
    sleep 0.3
}

verify() {
    local backend=$1
    echo ""
    echo "=============== backend = $backend ==============="
    cleanup

    local config=config/default.yaml      # io_uring
    local host=127.0.0.1
    if [ "$backend" = "af_xdp" ]; then
        config=config/backend_af_xdp.yaml
        host=$DST_IP
        # veth 对: veth0 挂 XDP+xsk(收), veth1 发包。静态邻居免 ARP, 关 IPv6 免噪声帧。
        S ip link add veth0 type veth peer name veth1
        S ip link set veth0 up; S ip link set veth1 up
        S ip addr add $SRC_IP/24 dev veth1
        local mac=$(S cat /sys/class/net/veth0/address | tr -d '\n')
        S ip neigh add $DST_IP lladdr $mac dev veth1 nud permanent
        S sysctl -w net.ipv6.conf.veth0.disable_ipv6=1 >/dev/null 2>&1
        S sysctl -w net.ipv6.conf.veth1.disable_ipv6=1 >/dev/null 2>&1
    elif [ "$backend" = "dpdk" ]; then
        config=config/backend_dpdk.yaml
        host=$DST_IP
        # net_tap0 vdev 由 trader DPDK 启动时创建内核 tap dtap0(脚本等它出现再配 IP)
    fi

    # ── 起 trader(shm 握手, root 因 af_xdp/dpdk 需特权) ──
    # stdbuf -oL: 行缓冲, 让 printf 实时落 log(否则全缓冲, 进程退出才 flush, ready 检测失效)
    S stdbuf -oL ./build/trader --config $config > /tmp/v4_trader.log 2>&1 &
    local trader_pid=$!

    # ── 等网络载体就绪 + trader ready ──
    #   dpdk: net_tap0 vdev 由 trader 创建内核 tap dtap0, 出现后配 IP + 静态邻居(免 ARP)。
    #   af_xdp/io_uring: 载体已就绪, 直接等 trader ready。
    local ready=0
    local dpdk_setup=0
    for i in $(seq 1 60); do
        if [ "$backend" = "dpdk" ] && [ "$dpdk_setup" = "0" ] \
            && S test -e /sys/class/net/dtap0; then
            S ip addr add $SRC_IP/24 dev dtap0
            S ip neigh add $DST_IP lladdr 02:00:00:00:00:01 dev dtap0 nud permanent
            S sysctl -w net.ipv6.conf.dtap0.disable_ipv6=1 >/dev/null 2>&1
            dpdk_setup=1
        fi
        if grep -q "等待回放客户端" /tmp/v4_trader.log 2>/dev/null; then ready=1; break; fi
        sleep 0.2
    done
    if [ "$ready" != "1" ]; then
        echo "  FAIL: trader 未就绪(检查 /tmp/v4_trader.log)"
        cat /tmp/v4_trader.log
        FAIL=1
        cleanup
        return
    fi

    # ── 起 benchmark(shm 握手, root 与 trader 共享内存权限一致) ──
    # --pack-max 8: 控制帧 < 4KB(AF_XDP UMEM 单帧上限一页) & < mbuf(~2KB), 与真实 MTU 语义一致。
    S ./build/trader_benchmark --file $ITCH_FILE --host $host --port $PORT --pack-max 8 \
        > /tmp/v4_bench.log 2>&1

    # ── 等 trader 退出(shm done 后) ──
    for i in $(seq 1 100); do
        kill -0 $trader_pid 2>/dev/null || break
        sleep 0.2
    done
    if kill -0 $trader_pid 2>/dev/null; then
        echo "  FAIL: trader 超时未退出"
        S kill -9 $trader_pid 2>/dev/null
        FAIL=1
        cleanup
        return
    fi

    # ── 汇总对比 ──
    local sent=$(grep -oP "Messages sent:\s+\K[0-9]+" /tmp/v4_bench.log | head -1)
    local parsed=$(grep -oP "解析 \K[0-9]+(?= 条)" /tmp/v4_trader.log | head -1)
    echo "  benchmark sent = ${sent:-?}"
    echo "  trader   parsed = ${parsed:-?}"
    if [ -n "$sent" ] && [ "$sent" = "$parsed" ]; then
        echo "  backend=$backend 正确性 PASS ✓ (sent == parsed, 零丢失)"
    else
        echo "  backend=$backend 正确性 FAIL ✗"
        echo "  --- bench log 尾部 ---"; tail -8 /tmp/v4_bench.log
        echo "  --- trader log 尾部 ---"; tail -8 /tmp/v4_trader.log
        FAIL=1
    fi
    cleanup
}

verify io_uring
verify af_xdp
verify dpdk

echo ""
if [ "$FAIL" = "0" ]; then
    echo "三后端主程序正确性验证全部 PASS ✓"
    exit 0
fi
echo "存在失败后端"
exit 1
