#!/usr/bin/env bash
# 真实 Kafka 端到端：worker --features kafka + 本地 broker，PHP 发消息，
# kafka-console-consumer 验证收到。
set -euo pipefail

cd "$(dirname "$0")/.."

SOCKET="/tmp/hi-kafka-real.sock"
LOG="/tmp/hi-kafka-real.log"
TOPIC="hi-kafka-real-test-$$"
BROKER="${BROKER:-localhost:9094}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-hi-kafka-ext-kafka_kraft-1}"
KAFKA_BIN="${KAFKA_BIN:-/opt/bitnami/kafka/bin}"
EXT_PATH="$(realpath target/debug/libhi_kafka.dylib 2>/dev/null || realpath target/debug/libhi_kafka.so)"

if [[ ! -f "$EXT_PATH" ]]; then
    echo "ERROR: 扩展产物缺失，先跑 scripts/build-dev.sh" >&2
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo "ERROR: 容器 $KAFKA_CONTAINER 未运行，先：" >&2
    echo "       docker compose -f docker-compose.kafka.yml up -d" >&2
    exit 1
fi

echo "==> 构建 worker (--features kafka)"
cargo build -p hi-kafka-worker --features kafka 2>&1 | tail -3

cleanup() {
    if [[ -n "${WORKER_PID:-}" ]]; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    rm -f "$SOCKET"
}
trap cleanup EXIT

echo "==> 启动 worker (socket=$SOCKET, brokers=$BROKER)"
rm -f "$SOCKET"
./target/debug/hi-kafka-worker --socket "$SOCKET" --brokers "$BROKER" --log-level info > "$LOG" 2>&1 &
WORKER_PID=$!

for i in {1..30}; do
    [[ -S "$SOCKET" ]] && break
    sleep 0.1
done
[[ -S "$SOCKET" ]] || { echo "worker 未就绪" >&2; cat "$LOG"; exit 1; }

echo "==> 启动后台 consumer"
CONSUMER_OUT="/tmp/hi-kafka-consumer-$$.out"
docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN/kafka-console-consumer.sh" \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms 5000 > "$CONSUMER_OUT" 2>/dev/null &
CONSUMER_PID=$!

sleep 1

echo "==> PHP 发 3 条消息到 topic=$TOPIC"
php -d extension="$EXT_PATH" -r "
    for (\$i = 1; \$i <= 3; \$i++) {
        hi_kafka_produce_fnf('$SOCKET', 'default', '$TOPIC', 'key-' . \$i, 'value-' . \$i);
    }
    echo 'sent 3 messages' . PHP_EOL;
"

wait $CONSUMER_PID 2>/dev/null || true

echo "==> Consumer 收到："
cat "$CONSUMER_OUT"

GOT=$(wc -l < "$CONSUMER_OUT" | tr -d ' ')
if [[ "$GOT" == "3" ]]; then
    echo "==> KAFKA E2E PASSED (收到 3/3 消息)"
    rm -f "$CONSUMER_OUT"
    exit 0
else
    echo "==> KAFKA E2E FAILED (收到 $GOT/3)" >&2
    echo "==> Worker 日志：" >&2
    cat "$LOG" >&2
    rm -f "$CONSUMER_OUT"
    exit 1
fi
