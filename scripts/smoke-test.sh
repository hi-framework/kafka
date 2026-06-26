#!/usr/bin/env bash
# End-to-end smoke test：起 worker，PHP 调用 produce_fnf，验证 worker 日志看到帧。
set -euo pipefail

cd "$(dirname "$0")/.."

SOCKET="/tmp/hi-kafka-smoke.sock"
LOG="/tmp/hi-kafka-smoke.log"
EXT_PATH="$(realpath target/debug/libhi_kafka.dylib 2>/dev/null || realpath target/debug/libhi_kafka.so)"

if [[ ! -f "$EXT_PATH" ]]; then
    echo "ERROR: 扩展产物缺失，先跑 scripts/build-dev.sh" >&2
    exit 1
fi

cleanup() {
    if [[ -n "${WORKER_PID:-}" ]]; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    rm -f "$SOCKET"
}
trap cleanup EXIT

echo "==> 启动 worker (socket=$SOCKET)"
rm -f "$SOCKET"
./target/debug/hi-kafka-worker --socket "$SOCKET" --log-level debug > "$LOG" 2>&1 &
WORKER_PID=$!

# 等 socket 就绪
for i in {1..30}; do
    [[ -S "$SOCKET" ]] && break
    sleep 0.1
done
if [[ ! -S "$SOCKET" ]]; then
    echo "ERROR: worker 未在 3s 内就绪" >&2
    cat "$LOG"
    exit 1
fi
echo "==> worker pid=$WORKER_PID"

echo "==> PHP 调用 hi_kafka_produce_fnf"
php -d extension="$EXT_PATH" -r "
    echo 'ext version: ' . hi_kafka_version() . PHP_EOL;
    hi_kafka_produce_fnf('$SOCKET', 'default', 'smoke-topic', 'k1', 'hello kafka');
    hi_kafka_produce_fnf('$SOCKET', 'default', 'smoke-topic', 'k2', 'second msg');
    echo 'sent 2 messages' . PHP_EOL;
"

sleep 0.3
echo "==> worker 日志:"
cat "$LOG"

# 验收：日志含 2 条 PRODUCE_FNF
COUNT=$(grep -c 'PRODUCE_FNF' "$LOG" || true)
if [[ "$COUNT" -ge 2 ]]; then
    echo "==> SMOKE TEST PASSED ($COUNT PRODUCE_FNF entries in log)"
    exit 0
else
    echo "==> SMOKE TEST FAILED: 期望 ≥2 条 PRODUCE_FNF 日志，实际 $COUNT" >&2
    exit 1
fi
