<?php

declare(strict_types=1);

/**
 * 验证 Kafka transactional producer：
 *
 * - 阶段 1：commit 路径
 *   begin → produce 到 2 个 topic → commit
 *   read_committed consumer 看得到
 *
 * - 阶段 2：abort 路径
 *   begin → produce 到 2 个 topic → abort
 *   read_committed consumer **看不到**（只看到 commit 的部分）
 *
 * 前置：每个 PHP 实例需要唯一 transactional.id（KIP-447 fencing）
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: transaction.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$txnId = 'hi-kafka-txn-' . \posix_getpid() . '-' . \mt_rand();
$topicA = "{$topicPrefix}-a";
$topicB = "{$topicPrefix}-b";

$client = new Hi\Kafka\Client($socket);

// 事务集群必须配 transactional.id + acks=all + 幂等
$client->registerCluster('txn', [
    'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
    'transactional.id' => $txnId,
    'transaction.timeout.ms' => '15000',
    'enable.idempotence' => 'true',
    'acks' => 'all',
]);
echo "transactional.id = {$txnId}" . \PHP_EOL . \PHP_EOL;

// === 阶段 1：commit 路径 ===
echo '=== 1. begin → produce A + B → commit ===' . \PHP_EOL;
$client->beginTransaction('txn');
$rA = $client->produceSync('txn', $topicA, 'k', 'tx-commit-A', [], null, null, 5000);
$rB = $client->produceSync('txn', $topicB, 'k', 'tx-commit-B', [], null, null, 5000);
$client->commitTransaction('txn');
echo "  A offset={$rA['offset']} B offset={$rB['offset']}" . \PHP_EOL;
echo '  COMMITTED' . \PHP_EOL . \PHP_EOL;

// === 阶段 2：abort 路径 ===
echo '=== 2. begin → produce A + B → abort ===' . \PHP_EOL;
$client->beginTransaction('txn');
$client->produceSync('txn', $topicA, 'k', 'tx-abort-A-should-not-see', [], null, null, 5000);
$client->produceSync('txn', $topicB, 'k', 'tx-abort-B-should-not-see', [], null, null, 5000);
$client->abortTransaction('txn');
echo '  ABORTED' . \PHP_EOL . \PHP_EOL;

// === 阶段 3：read_committed 消费验证 ===
echo '=== 3. read_committed consumer 验证 ===' . \PHP_EOL;

// 注册独立的非事务集群给 consumer 用（用同一 broker）
$client->registerCluster('reader', [
    'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

foreach ([$topicA, $topicB] as $topic) {
    $sub = $client->subscribe('reader', 'txn-grp-' . \posix_getpid() . '-' . $topic, [$topic], [
        'auto.offset.reset' => 'earliest',
        'isolation.level' => 'read_committed',   // ← 关键：只看 committed
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '2000',
    ]);

    $messages = [];
    $deadline = \microtime(true) + 8;
    while (\microtime(true) < $deadline && \count($messages) < 2) {
        $batch = $client->poll($sub, 100, 1500);
        foreach ($batch as $m) {
            $messages[] = $m['value'];
        }
        if (\count($messages) >= 1) {
            // 可能有更多消息要等；多等一点点确认 abort 的没漏进来
            $deadline = \min($deadline, \microtime(true) + 1.5);
        }
    }
    $client->commit($sub);
    $client->unsubscribe($sub);

    echo "  topic={$topic} 看到: " . \json_encode($messages) . \PHP_EOL;

    // 期望：只看到 "tx-commit-*"，看不到 "tx-abort-*"
    foreach ($messages as $v) {
        if (\str_starts_with($v, 'tx-abort-')) {
            \fwrite(\STDERR, "FAIL: 消费者看到了被 abort 的消息: {$v}\n");
            exit(1);
        }
    }
    if (1 !== \count($messages)) {
        \fwrite(\STDERR, 'FAIL: 期望 1 条 committed，实际 ' . \count($messages) . " 条\n");
        exit(1);
    }
    if (! \str_starts_with($messages[0], 'tx-commit-')) {
        \fwrite(\STDERR, "FAIL: 不是 commit 的消息: {$messages[0]}\n");
        exit(1);
    }
}

echo \PHP_EOL . '★ Transactional PASS（commit 可见 / abort 隔离 / 跨 topic 原子）' . \PHP_EOL;
