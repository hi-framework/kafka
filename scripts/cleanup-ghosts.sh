#!/usr/bin/env bash
# 一键清理遗留的 hi-kafka worker 进程。
#
# 背景：worker 是 fork 出来的 daemon（setsid + 不设 PDEATHSIG），故意活过父进程。
# 跑 e2e 时每个测试用独立 socket → 独立 worker；测试退出不清理就堆积。
# macOS 上 ps 看 worker 仍像 php（kernel commandline 与 user-space argv 隔离）。
#
# 本脚本按 cmdline 中包含的 socket 路径模式来 pgrep，覆盖：
#   /tmp/e2e-*.sock          run-all-e2e.sh 的测试 socket
#   /tmp/hi-kafka-*.sock     smoke/integration 的测试 socket
#   /tmp/control-rcv-*.sock  control-recovery / replay-recovery 测试
#   /tmp/replay-rcv*.sock    （同上）
#   /tmp/swoole-p3*.sock     swoole-phase3 测试
#
# 用法：
#   ./scripts/cleanup-ghosts.sh                # 默认所有模式
#   ./scripts/cleanup-ghosts.sh '/tmp/myown'   # 指定额外模式
#   DRY_RUN=1 ./scripts/cleanup-ghosts.sh      # 只打印不杀

set -uo pipefail

DEFAULT_PATTERNS=(
    "/tmp/e2e-.*\\.sock"
    "/tmp/hi-kafka-.*\\.sock"
    "/tmp/control-rcv.*\\.sock"
    "/tmp/replay-rcv.*\\.sock"
    "/tmp/swoole-p3.*\\.sock"
    "/tmp/binary-.*\\.sock"
    "/tmp/y1-test\\.sock"
)
PATTERNS=("${DEFAULT_PATTERNS[@]}" "$@")

killed_total=0
ghosts_total=0
for pat in "${PATTERNS[@]}"; do
    # pgrep -f：匹配整条命令行（含 PHP -d extension=... 和 socket 路径）
    pids=$(pgrep -f "$pat" 2>/dev/null || true)
    [[ -z "$pids" ]] && continue
    echo "==> pattern: $pat"
    while IFS= read -r pid; do
        [[ -z "$pid" ]] && continue
        # 跳过自己 + 父 shell
        [[ "$pid" == "$$" || "$pid" == "$PPID" ]] && continue
        ghosts_total=$((${ghosts_total:-0} + 1))
        if [[ "${DRY_RUN:-0}" == "1" ]]; then
            echo "  would kill $pid: $(ps -p $pid -o command= 2>/dev/null | head -c 100)"
        else
            kill -TERM "$pid" 2>/dev/null || true
            killed_total=$((${killed_total:-0} + 1))
        fi
    done <<< "$pids"
done

if [[ "${DRY_RUN:-0}" == "1" ]]; then
    echo
    echo "==> DRY RUN: 候选 ${ghosts_total} 个"
    exit 0
fi

# 等 graceful drain
[[ $killed_total -gt 0 ]] && sleep 1

# 再扫一遍，仍存在的硬杀
remaining=0
for pat in "${PATTERNS[@]}"; do
    pids=$(pgrep -f "$pat" 2>/dev/null || true)
    [[ -z "$pids" ]] && continue
    while IFS= read -r pid; do
        [[ -z "$pid" ]] && continue
        [[ "$pid" == "$$" || "$pid" == "$PPID" ]] && continue
        if kill -KILL "$pid" 2>/dev/null; then
            remaining=$((${remaining:-0} + 1))
        fi
    done <<< "$pids"
done

# 清理 socket / pid / lock 文件
rm -f /tmp/e2e-*.sock /tmp/e2e-*.sock.pid /tmp/e2e-*.sock.spawn-lock 2>/dev/null
rm -f /tmp/hi-kafka-*.sock /tmp/hi-kafka-*.sock.pid /tmp/hi-kafka-*.sock.spawn-lock 2>/dev/null
rm -f /tmp/control-rcv*.sock /tmp/control-rcv*.sock.pid /tmp/control-rcv*.sock.spawn-lock 2>/dev/null
rm -f /tmp/replay-rcv*.sock /tmp/replay-rcv*.sock.pid /tmp/replay-rcv*.sock.spawn-lock 2>/dev/null
rm -f /tmp/swoole-p3*.sock /tmp/swoole-p3*.sock.pid /tmp/swoole-p3*.sock.spawn-lock 2>/dev/null

echo
echo "==> 杀掉 ${killed_total:-0} + 硬杀 ${remaining:-0}，清理 socket/pid/lock 文件完成"
