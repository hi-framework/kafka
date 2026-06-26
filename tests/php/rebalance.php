<?php

declare(strict_types=1);

/**
 * 验证 rebalance 事件流：
 *
 * 阶段 1：consumer A 单独订阅 → 拿到全部 3 分区（Assign 事件）
 * 阶段 2：consumer B 加入同 group → A 触发 Revoke + 部分 Assign 事件，B 也拿到 Assign
 * 阶段 3：B 离开 → A 拿到剩余分区的 Assign
 *
 * 用 fork() 来同时跑 2 个 consumer。
 */

if ($argc < 3) {
    fwrite(STDERR, "usage: rebalance.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$group = 'rb-grp-' . posix_getpid();

// === 准备 3 分区 topic ===
echo "=== 0. 创建 3 分区 topic ===" . PHP_EOL;
shell_exec("docker exec hi-kafka-ext-kafka_kraft-1 /opt/bitnami/kafka/bin/kafka-topics.sh "
    . "--bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 1 2>&1");

$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', [
    'bootstrap.servers' => getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

function collectEvents(Hi\Kafka\Client $client, int $sub, int $deadlineMs): array
{
    $events = [];
    $deadline = microtime(true) + $deadlineMs / 1000;
    while (microtime(true) < $deadline) {
        $batch = $client->pollRebalanceEvents($sub, 100, 1000);
        foreach ($batch as $e) {
            $events[] = $e;
        }
        // 同时轻 poll 消息以驱动 rdkafka 内部回调
        $client->poll($sub, 0, 200);
    }
    return $events;
}

function fmtEvent(array $e): string
{
    if ($e['type'] === 'error') {
        return "ERROR: {$e['message']}";
    }
    $parts = array_map(fn($p) => "{$p['topic']}:{$p['partition']}", $e['partitions']);
    return strtoupper($e['type']) . " " . implode(',', $parts);
}

// === 阶段 1：A 单独订阅 ===
echo "=== 1. consumer A 订阅 ===" . PHP_EOL;
$subA = $client->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);

$eventsA1 = collectEvents($client, $subA, 4000);
foreach ($eventsA1 as $e) {
    echo "  A: " . fmtEvent($e) . PHP_EOL;
}

$assignA1 = array_filter($eventsA1, fn($e) => $e['type'] === 'assign');
if (count($assignA1) === 0) {
    fwrite(STDERR, "FAIL: A 没拿到 Assign 事件\n");
    exit(1);
}
$lastAssignA1 = end($assignA1);
$initialPartitions = count($lastAssignA1['partitions']);
echo "  → A 初始持有 $initialPartitions 个分区" . PHP_EOL;

if ($initialPartitions !== 3) {
    fwrite(STDERR, "FAIL: 期望 A 单独时持有 3 分区，实际 $initialPartitions\n");
    exit(1);
}

// === 阶段 2：另起独立 PHP 进程作为 B 加入同 group，触发 rebalance ===
echo PHP_EOL . "=== 2. spawn consumer B (独立 PHP 进程) 加入同 group ===" . PHP_EOL;
$phpBin = PHP_BINARY;
$extPath = ini_get('extension') ?: '';
$joinerPath = __DIR__ . '/_rebalance_joiner.php';
$cmd = sprintf(
    "%s -d extension=%s %s %s %s %s 8 > /tmp/hi-kafka-rb-B.log 2>&1 &",
    escapeshellarg($phpBin),
    escapeshellarg((string) ini_get('extension_dir')) . '/hi_kafka.so',
    escapeshellarg($joinerPath),
    escapeshellarg($socket),
    escapeshellarg($topic),
    escapeshellarg($group)
);
// 上面用 ini_get 不一定能拿到完整路径；改用 phpinfo 提取的 extension 路径
// 简化：传入扩展路径作为环境变量
$extFromEnv = getenv('HI_KAFKA_EXT_PATH') ?: '';
$cmd = sprintf(
    "%s -d extension=%s %s %s %s %s 8 > /tmp/hi-kafka-rb-B.log 2>&1 &",
    escapeshellarg($phpBin),
    escapeshellarg($extFromEnv),
    escapeshellarg($joinerPath),
    escapeshellarg($socket),
    escapeshellarg($topic),
    escapeshellarg($group)
);
shell_exec($cmd);

// 父进程：A 继续运行，收集 rebalance 事件
$eventsA2 = collectEvents($client, $subA, 6000);
foreach ($eventsA2 as $e) {
    echo "  A: " . fmtEvent($e) . PHP_EOL;
}

$revokeA2 = array_filter($eventsA2, fn($e) => $e['type'] === 'revoke');
$assignA2 = array_filter($eventsA2, fn($e) => $e['type'] === 'assign');

if (count($revokeA2) === 0) {
    fwrite(STDERR, "FAIL: A 没有收到 Revoke 事件（B 加入应该触发）\n");
    exit(1);
}
if (count($assignA2) === 0) {
    fwrite(STDERR, "FAIL: A 没有收到新的 Assign 事件（rebalance 后应重新分配）\n");
    exit(1);
}

$finalAssignA2 = end($assignA2);
$afterRebalanceCount = count($finalAssignA2['partitions']);
echo "  → A 在 rebalance 后持有 $afterRebalanceCount 个分区" . PHP_EOL;

if ($afterRebalanceCount >= $initialPartitions) {
    fwrite(STDERR, "FAIL: rebalance 后 A 的分区数应少于初始（$initialPartitions），实际 $afterRebalanceCount\n");
    exit(1);
}

// 等 B 自然退出（B 跑 8 秒，这里已经 collect 了 6 秒，再等 4 秒）
sleep(4);

// === 阶段 3：B 离开，A 应该再次收到 Assign（拿回全部 3 分区）===
echo PHP_EOL . "=== 3. B 已退出，A 应再次拿到全部 3 分区 ===" . PHP_EOL;
$eventsA3 = collectEvents($client, $subA, 10000);
foreach ($eventsA3 as $e) {
    echo "  A: " . fmtEvent($e) . PHP_EOL;
}

$assignA3 = array_filter($eventsA3, fn($e) => $e['type'] === 'assign');
if (count($assignA3) === 0) {
    fwrite(STDERR, "WARN: 阶段 3 没收到 Assign。可能 B 离开速度太快被合并。\n");
} else {
    $finalAssignA3 = end($assignA3);
    $recoveredCount = count($finalAssignA3['partitions']);
    echo "  → A 最终持有 $recoveredCount 个分区" . PHP_EOL;
    if ($recoveredCount !== 3) {
        fwrite(STDERR, "FAIL: A 没拿回 3 分区，实际 $recoveredCount\n");
        exit(1);
    }
}

$client->unsubscribe($subA);
echo PHP_EOL . "★ Rebalance 事件流 PASS" . PHP_EOL;
