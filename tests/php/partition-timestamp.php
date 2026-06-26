<?php

declare(strict_types=1);

/**
 * 验证 per-message partition + timestamp 显式控制：
 *
 * 1. 显式 partition：每条消息指定不同分区，consumer 端验证 partition 字段
 * 2. 显式 timestamp：消息带历史时间戳（一周前），consumer 端验证 timestamp_ms
 *
 * 前置：topic 至少有 3 个分区（默认 auto-create 1 分区，先创建 3 分区 topic）
 */

if ($argc < 3) {
    fwrite(STDERR, "usage: partition-timestamp.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', [
    'bootstrap.servers' => getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

// === 准备：创建 3 分区 topic ===
$topic = $topicPrefix;
echo "=== 0. 用 docker exec 创建 3 分区 topic '$topic' ===" . PHP_EOL;
shell_exec("docker exec hi-kafka-ext-kafka_kraft-1 /opt/bitnami/kafka/bin/kafka-topics.sh "
    . "--bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 1 2>&1");

// === 阶段 1：显式 partition 验证 ===
echo "=== 1. produce 3 条到指定的 3 个不同分区 ===" . PHP_EOL;
$weekAgo = (int) ((microtime(true) - 7 * 86400) * 1000);  // 一周前的毫秒时间戳

$results = [];
for ($p = 0; $p < 3; $p++) {
    $r = $client->produceSync(
        'default',
        $topic,
        'shared-key',  // 故意用同一个 key
        "msg-for-partition-$p",
        [],            // headers
        $p,            // partition (显式)
        $weekAgo + $p, // timestamp (一周前 + 偏移)
        5000           // timeoutMs
    );
    if (! $r['ok']) {
        fwrite(STDERR, "produce fail: " . json_encode($r) . PHP_EOL);
        exit(1);
    }
    $results[$p] = $r;
    printf("  写到 partition=%d -> ack partition=%d offset=%d\n", $p, $r['partition'], $r['offset']);

    if ($r['partition'] !== $p) {
        fwrite(STDERR, "FAIL: 期望 partition=$p, 实际 {$r['partition']}\n");
        exit(1);
    }
}
echo "  3/3 都按指定 partition 落地（同 key 写到 3 个不同分区）" . PHP_EOL;

// === 阶段 2：消费 + 验证 timestamp ===
echo PHP_EOL . "=== 2. 消费 + 验证 timestamp_ms ===" . PHP_EOL;
$sub = $client->subscribe('default', 'pt-grp-' . posix_getpid(), [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);

$got = [];
$rounds = 0;
while (count($got) < 3 && $rounds < 8) {
    $batch = $client->poll($sub, 100, 2000);
    foreach ($batch as $m) {
        $got[$m['partition']] = $m;
    }
    $rounds++;
}

if (count($got) !== 3) {
    fwrite(STDERR, "FAIL: 只收到 " . count($got) . "/3 分区的消息\n");
    exit(1);
}

ksort($got);
foreach ($got as $p => $m) {
    $expectedTs = $weekAgo + $p;
    $tsDeltaMs = abs($m['timestamp_ms'] - $expectedTs);
    printf("  partition=%d offset=%d timestamp_ms=%d delta=%dms value=%s\n",
        $m['partition'], $m['offset'], $m['timestamp_ms'], $tsDeltaMs, $m['value']);

    // librdkafka timestamp 与我们指定的应当完全一致
    if ($tsDeltaMs > 5) {
        fwrite(STDERR, "FAIL: partition $p timestamp 不匹配（期望 $expectedTs，实际 {$m['timestamp_ms']}）\n");
        exit(1);
    }
}

$client->commit($sub);
$client->unsubscribe($sub);

echo PHP_EOL . "★ partition + timestamp PASS（同 key 显式跨分区 + 历史时间戳精确透传）" . PHP_EOL;
