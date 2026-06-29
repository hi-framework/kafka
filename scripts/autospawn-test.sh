#!/usr/bin/env bash
# 测试 worker 自动启动：不预启动 worker，PHP 调用扩展时它自己拉起。
set -euo pipefail

cd "$(dirname "$0")/.."

SOCKET="/tmp/hi-kafka-autospawn.sock"
LOCK="${SOCKET}.spawn-lock"
LOG="/tmp/hi-kafka-autospawn.log"
TOPIC="hi-kafka-autospawn-$$"
WORKER_BIN="$(realpath target/debug/hi-kafka-worker)"
EXT_PATH="$(realpath target/debug/libhi_kafka.dylib 2>/dev/null || realpath target/debug/libhi_kafka.so)"

if [[ ! -f "$EXT_PATH" || ! -f "$WORKER_BIN" ]]; then
    echo "ERROR: 缺少构建产物，先跑 scripts/build-dev.sh" >&2
    exit 1
fi

cleanup() {
    # 杀掉由扩展拉起的 worker（按 socket 路径找 PID）
    pkill -f "hi-kafka-worker.*$SOCKET" 2>/dev/null || true
    rm -f "$SOCKET" "$LOCK" "$LOG"
}
trap cleanup EXIT
cleanup  # 先清场

KAFKA_CONTAINER="${KAFKA_CONTAINER:-hi-kafka-ext-kafka_kraft-1}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
BROKER="${BROKER:-localhost:9094}"
USE_KAFKA="${USE_KAFKA:-1}"

if [[ "$USE_KAFKA" == "1" ]] && ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo "ERROR: Kafka 容器未运行 (USE_KAFKA=0 可跳过)" >&2
    exit 1
fi

echo "==> PHP 调用，期望扩展自动拉起 worker"
echo "==> WORKER_BIN=$WORKER_BIN"
echo "==> SOCKET=$SOCKET"

CONSUMER_OUT=""
if [[ "$USE_KAFKA" == "1" ]]; then
    CONSUMER_OUT="/tmp/hi-kafka-consumer-$$.out"
    docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN/kafka-console-consumer.sh" \
        --bootstrap-server localhost:9092 \
        --topic "$TOPIC" \
        --from-beginning \
        --timeout-ms 8000 > "$CONSUMER_OUT" 2>/dev/null &
    CONSUMER_PID=$!
    sleep 1
fi

# 验证扩展拉起的进程数：3 个并发 PHP 进程，期望只产生 1 个 worker
TS=$(date +%s%N)
(
HI_KAFKA_WORKER_BIN="$WORKER_BIN" \
HI_KAFKA_BROKERS="$BROKER" \
HI_KAFKA_LOG_FILE="$LOG" \
HI_KAFKA_LOG_LEVEL="info" \
    php -d extension="$EXT_PATH" -r "
        hi_kafka_produce_fnf('$SOCKET', 'default', '$TOPIC', 'concurrent-1', 'value-1');
    " &
HI_KAFKA_WORKER_BIN="$WORKER_BIN" \
HI_KAFKA_BROKERS="$BROKER" \
HI_KAFKA_LOG_FILE="$LOG" \
HI_KAFKA_LOG_LEVEL="info" \
    php -d extension="$EXT_PATH" -r "
        hi_kafka_produce_fnf('$SOCKET', 'default', '$TOPIC', 'concurrent-2', 'value-2');
    " &
HI_KAFKA_WORKER_BIN="$WORKER_BIN" \
HI_KAFKA_BROKERS="$BROKER" \
HI_KAFKA_LOG_FILE="$LOG" \
HI_KAFKA_LOG_LEVEL="info" \
    php -d extension="$EXT_PATH" -r "
        hi_kafka_produce_fnf('$SOCKET', 'default', '$TOPIC', 'concurrent-3', 'value-3');
    " &
wait
)

echo "==> 3 个 PHP 进程已完成"

# 数 worker 进程
sleep 0.5
WORKER_COUNT=$(pgrep -f "hi-kafka-worker.*$SOCKET" | wc -l | tr -d ' ')
echo "==> worker 进程数: $WORKER_COUNT (期望 1)"

if [[ "$WORKER_COUNT" != "1" ]]; then
    echo "==> FAIL: worker 进程数不对" >&2
    pgrep -f "hi-kafka-worker.*$SOCKET" | xargs -I{} ps -p {} 2>&1
    exit 1
fi

if [[ "$USE_KAFKA" == "1" ]]; then
    wait $CONSUMER_PID 2>/dev/null || true
    GOT=$(wc -l < "$CONSUMER_OUT" | tr -d ' ')
    echo "==> Kafka 收到: $GOT/3 消息"
    cat "$CONSUMER_OUT"
    rm -f "$CONSUMER_OUT"
    if [[ "$GOT" != "3" ]]; then
        echo "==> FAIL" >&2
        echo "==> Worker 日志:" >&2
        cat "$LOG" >&2
        exit 1
    fi
fi

echo "==> AUTOSPAWN TEST PASSED"
