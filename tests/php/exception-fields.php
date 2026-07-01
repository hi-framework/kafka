<?php

declare(strict_types=1);

/**
 * 验证 KafkaException 结构化字段 + produceSync 错误返回数组的字段。
 *
 * 两条错误路径在 ext 里语义不同：
 *
 * 1. **produceSync**：错误通过返回数组承载
 *    `['ok' => false, 'code' => int, 'message' => str, 'retryable' => bool]`
 *    业务侧读 `ok`/`retryable` 决定重试；不抛异常。
 *
 * 2. **subscribe / commit / begin_txn / ...**：错误通过 `KafkaException` 抛出
 *    `getKind() / getKindName() / isRetryable() / getNativeCode()` 四个 getter
 *
 * 场景：
 *   - produceSync 到未注册集群 → 返回 `ok=false`，`code=5 (CLUSTER_NOT_REGISTERED)`, `retryable=false`
 *   - subscribe 到未注册集群   → 抛 KafkaException（kindName=CLUSTER_NOT_REGISTERED）
 *   - beginTransaction 到无事务集群 → 抛 KafkaException（kindName ∈ {TXN_STATE, CLUSTER_NOT_REGISTERED, INVALID_ARGUMENT}）
 *
 * 不依赖真 broker：所有错误在 worker 参数校验层就抛出。
 */
if ($argc < 2) {
    \fwrite(\STDERR, "usage: exception-fields.php SOCKET [TOPIC_PREFIX]\n");
    exit(2);
}

[$_, $socket] = $argv;
$topic = $argv[2] ?? ('exc-' . \posix_getpid());

$client = new Hi\Kafka\Client($socket);

$failed = 0;
$passed = 0;

function log_exc(string $name, Hi\Kafka\KafkaException $e): void
{
    echo "  ─ [{$name}]" . \PHP_EOL;
    echo "      kind       = {$e->getKind()}" . \PHP_EOL;
    echo "      kindName   = {$e->getKindName()}" . \PHP_EOL;
    echo "      retryable  = " . ($e->isRetryable() ? 'true' : 'false') . \PHP_EOL;
    echo "      nativeCode = {$e->getNativeCode()}" . \PHP_EOL;
    echo "      message    = {$e->getMessage()}" . \PHP_EOL;
}

// === 场景 1: produceSync 到未注册集群 → 抛 KafkaException =======================
// 与 broker-side delivery 错误（返回 ok=false）语义不同：**worker-side rejection**
// （cluster 未注册 / 参数非法 / 事务状态错等）直接经 Error 帧转成 KafkaException 抛出。
echo '=== 1. produceSync 到未注册集群 → KafkaException ===' . \PHP_EOL;
try {
    $client->produceSync('never-registered', $topic, 'k', 'v', [], null, null, 3000);
    \fwrite(\STDERR, "  FAIL: produceSync 到未注册集群未抛异常\n");
    $failed++;
} catch (Hi\Kafka\KafkaException $e) {
    log_exc('scene1', $e);
    if ('CLUSTER_NOT_REGISTERED' === $e->getKindName()) {
        echo '  ✓ kindName=CLUSTER_NOT_REGISTERED' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: kindName 期望 CLUSTER_NOT_REGISTERED，实际 {$e->getKindName()}\n");
        $failed++;
    }
    if (false === $e->isRetryable()) {
        echo '  ✓ retryable=false（cluster 未注册是永久错）' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: retryable 应为 false\n");
        $failed++;
    }
}

// === 场景 2: subscribe 到未注册集群 → 抛 KafkaException =========================
echo \PHP_EOL . '=== 2. subscribe 到未注册集群 → KafkaException ===' . \PHP_EOL;
try {
    $sub = $client->subscribe('never-registered', 'g', [$topic], null, 3000);
    \fwrite(\STDERR, "  FAIL: subscribe 到未注册集群竟然成功了（sub={$sub}）\n");
    $client->unsubscribe($sub);
    $failed++;
} catch (Hi\Kafka\KafkaException $e) {
    log_exc('scene2', $e);
    if ('CLUSTER_NOT_REGISTERED' === $e->getKindName()) {
        echo '  ✓ kindName=CLUSTER_NOT_REGISTERED' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: kindName 期望 CLUSTER_NOT_REGISTERED，实际 {$e->getKindName()}\n");
        $failed++;
    }
    if (false === $e->isRetryable()) {
        echo '  ✓ retryable=false' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: retryable 应为 false\n");
        $failed++;
    }
    // kind 应为非负 int
    if (\is_int($e->getKind()) && $e->getKind() >= 0) {
        echo '  ✓ kind 为非负 int' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: kind 应为非负 int\n");
        $failed++;
    }
    // nativeCode 应为 int（不检查具体值）
    if (\is_int($e->getNativeCode())) {
        echo '  ✓ nativeCode 为 int' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: nativeCode 应为 int\n");
        $failed++;
    }
}

// === 场景 3: beginTransaction 到无 transactional.id 的集群 =======================
echo \PHP_EOL . '=== 3. beginTransaction 到无 transactional.id → KafkaException ===' . \PHP_EOL;
try {
    $client->registerCluster('no-txn', [
        'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
    ]);
} catch (\Throwable $e) {
    echo "  registerCluster 提示: " . $e->getMessage() . \PHP_EOL;
}
try {
    $client->beginTransaction('no-txn', 3000);
    \fwrite(\STDERR, "  FAIL: 无 transactional.id 竟然 begin 成功\n");
    $failed++;
} catch (Hi\Kafka\KafkaException $e) {
    log_exc('scene3', $e);
    // 允许 TXN_STATE / CLUSTER_NOT_REGISTERED / INVALID_ARGUMENT / INTERNAL 任一
    $allowed = ['TXN_STATE', 'CLUSTER_NOT_REGISTERED', 'INVALID_ARGUMENT', 'INTERNAL'];
    if (\in_array($e->getKindName(), $allowed, true)) {
        echo "  ✓ kindName ∈ {" . \implode('|', $allowed) . '}' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: kindName 应为 {" . \implode('|', $allowed) . "} 之一，实际 {$e->getKindName()}\n");
        $failed++;
    }
    // 事务状态类错误一般不 retryable
    if (\is_bool($e->isRetryable())) {
        echo '  ✓ retryable 为 bool' . \PHP_EOL;
        $passed++;
    } else {
        \fwrite(\STDERR, "  FAIL: retryable 应为 bool\n");
        $failed++;
    }
}

echo \PHP_EOL;
if ($failed === 0) {
    echo "★ exception-fields PASS （{$passed} 断言通过）" . \PHP_EOL;
    exit(0);
}
\fwrite(\STDERR, "FAIL: {$failed} 断言未通过（{$passed} 通过）\n");
exit(1);
