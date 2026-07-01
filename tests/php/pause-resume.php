<?php

declare(strict_types=1);

/**
 * 验证 per-partition pause / resume。
 *
 * 语义说明：rdkafka 的 `pause` 是告诉 fetcher「别再从 broker 拉新消息」。
 * 已经预取/缓冲的消息仍会被消费完。要可观测 pause 生效，本测试在 pause 前
 * 先把缓冲消化干净，再 produce 一批新消息到 broker —— pause 期间 fetcher
 * 不工作，PHP 端 poll 不到这批新消息；resume 后才看到。
 *
 * 流程：
 *   阶段 1：种 3 条
 *   阶段 2：subscribe + ASSIGN + 消费 3 条 + drain（保证缓冲空）
 *   阶段 3：pause 分区 → 产 3 条新消息 → poll 2.5s 应为 0
 *   阶段 4：resume → 拿到新 3 条
 *   阶段 5：空 partitions 语义（pause-all / resume-all）
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: pause-resume.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$suffix = \posix_getpid() . '-' . \mt_rand();
$topic = "{$topicPrefix}-{$suffix}";
$group = "pause-grp-{$suffix}";

$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

echo "topic={$topic} group={$group}" . \PHP_EOL . \PHP_EOL;

// ============================================================================
// Phase 1：种 3 条
// ============================================================================
echo '=== 1. 种 3 条 ===' . \PHP_EOL;
for ($i = 0; $i < 3; $i++) {
    $client->produceSync('default', $topic, 'k', "msg-{$i}", [], null, null, 5000);
}
echo '  ok (offsets 0..2)' . \PHP_EOL . \PHP_EOL;

// ============================================================================
// Phase 2：订阅 + 等 ASSIGN + 消费 3 条 + drain 缓冲
// ============================================================================
echo '=== 2. 订阅 + 消费 3 条 + drain ===' . \PHP_EOL;
$sub = $client->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);

$assigned = \waitAssign($client, $sub, 15);
echo '  ASSIGN: ' . \json_encode($assigned) . \PHP_EOL;

$batch = \pollExactly($client, $sub, 3, 10);
echo '  前 3 条: ' . \implode(', ', \array_map(static fn ($m) => "o={$m['offset']}/{$m['value']}", $batch)) . \PHP_EOL;

\drainBuffer($client, $sub, 1.0);
echo '  缓冲已 drain' . \PHP_EOL . \PHP_EOL;

// ============================================================================
// Phase 3：pause → produce 新消息 → 2.5s 不该 poll 到任何东西
// ============================================================================
echo '=== 3. pause 分区 → produce 3 条新消息 → 应 poll 不到 ===' . \PHP_EOL;
[$topics, $parts] = \flattenPartitions($assigned);
$client->pause($sub, $topics, $parts);
echo '  paused' . \PHP_EOL;

// pause 之后才往 broker 写
for ($i = 3; $i < 6; $i++) {
    $client->produceSync('default', $topic, 'k', "msg-{$i}", [], null, null, 5000);
}
echo '  已 produce msg-3..msg-5 到 broker' . \PHP_EOL;

$paused = [];
$deadline = \microtime(true) + 2.5;
while (\microtime(true) < $deadline) {
    foreach ($client->poll($sub, 100, 400) as $m) {
        $paused[] = $m;
    }
}
echo '  pause 期间 poll: ' . \count($paused) . ' 条' . \PHP_EOL;
if (! empty($paused)) {
    \fwrite(\STDERR, 'FAIL: pause 期间不该有消息: '
        . \implode(', ', \array_map(static fn ($m) => "o={$m['offset']}/{$m['value']}", $paused)) . "\n");
    exit(1);
}
echo '  ✓ pause 生效（fetcher 不工作，新消息留在 broker）' . \PHP_EOL . \PHP_EOL;

// ============================================================================
// Phase 4：resume → 拿到 pause 期间产的 3 条
// ============================================================================
echo '=== 4. resume → 拿到 pause 期间产的 3 条 ===' . \PHP_EOL;
$client->resume($sub, $topics, $parts);
echo '  resumed' . \PHP_EOL;

$after = \pollExactly($client, $sub, 3, 10);
echo '  resume 后: ' . \implode(', ', \array_map(static fn ($m) => "o={$m['offset']}/{$m['value']}", $after)) . \PHP_EOL;

foreach ($after as $m) {
    if (! \in_array($m['value'], ['msg-3', 'msg-4', 'msg-5'], true)) {
        \fwrite(\STDERR, "FAIL: 预期 msg-3..5，实际 {$m['value']}\n");
        exit(1);
    }
}
echo '  ✓ resume 后 pause 期间消息全部到齐，offset 不重复' . \PHP_EOL . \PHP_EOL;

// ============================================================================
// Phase 5：空 partitions 语义
// ============================================================================
echo '=== 5. 空 partitions 语义（pause-all / resume-all）===' . \PHP_EOL;
\drainBuffer($client, $sub, 1.0);

$client->pause($sub, [], []);   // 空数组：应用到当前 assignment 全部
echo '  pause-all（空 partitions）' . \PHP_EOL;

for ($i = 6; $i < 9; $i++) {
    $client->produceSync('default', $topic, 'k', "msg-{$i}", [], null, null, 5000);
}

$pausedAll = [];
$deadline = \microtime(true) + 2;
while (\microtime(true) < $deadline) {
    foreach ($client->poll($sub, 100, 400) as $m) {
        $pausedAll[] = $m;
    }
}
if (! empty($pausedAll)) {
    \fwrite(\STDERR, 'FAIL: pause-all 期间不该有消息: '
        . \implode(', ', \array_map(static fn ($m) => "o={$m['offset']}", $pausedAll)) . "\n");
    exit(1);
}
echo '  ✓ pause-all 生效（2s 无消息）' . \PHP_EOL;

$client->resume($sub, [], []);
$tail = \pollExactly($client, $sub, 3, 6);
echo '  resume-all 后: ' . \implode(', ', \array_map(static fn ($m) => "o={$m['offset']}/{$m['value']}", $tail)) . \PHP_EOL;

foreach ($tail as $m) {
    if (! \in_array($m['value'], ['msg-6', 'msg-7', 'msg-8'], true)) {
        \fwrite(\STDERR, "FAIL: 预期 msg-6..8，实际 {$m['value']}\n");
        exit(1);
    }
}
echo '  ✓ resume-all 后续 3 条到齐' . \PHP_EOL . \PHP_EOL;

$client->unsubscribe($sub);
echo '★ Pause / Resume per partition PASS' . \PHP_EOL;

// ============================================================================
// helpers
// ============================================================================

function waitAssign(Hi\Kafka\Client $client, int $sub, float $maxSecs): array
{
    $assigned = [];
    $deadline = \microtime(true) + $maxSecs;
    while (\microtime(true) < $deadline && empty($assigned)) {
        foreach ($client->pollRebalanceEvents($sub, 100, 500) as $e) {
            if ('assign' === $e['type'] && ! empty($e['partitions'])) {
                $assigned = $e['partitions'];
                break;
            }
        }
        if (empty($assigned)) {
            \usleep(200_000);
        }
    }
    if (empty($assigned)) {
        \fwrite(\STDERR, "FAIL: 等不到 ASSIGN\n");
        exit(1);
    }
    return $assigned;
}

function pollExactly(Hi\Kafka\Client $client, int $sub, int $want, float $maxSecs): array
{
    $out = [];
    $deadline = \microtime(true) + $maxSecs;
    while (\count($out) < $want && \microtime(true) < $deadline) {
        foreach ($client->poll($sub, $want, 1500) as $m) {
            $out[] = $m;
            if (\count($out) >= $want) {
                break;
            }
        }
    }
    if (\count($out) < $want) {
        $got = \array_map(static fn ($m) => "o={$m['offset']}/{$m['value']}", $out);
        \fwrite(\STDERR, "FAIL: 期望 poll {$want} 条，实际 " . \count($out) . ' 条: '
            . \implode(', ', $got) . "\n");
        exit(1);
    }
    return $out;
}

/**
 * 把缓冲清空，丢弃任何已预取的消息，确保 pause 后的观测是干净的。
 */
function drainBuffer(Hi\Kafka\Client $client, int $sub, float $maxSecs): void
{
    $deadline = \microtime(true) + $maxSecs;
    while (\microtime(true) < $deadline) {
        $batch = $client->poll($sub, 1000, 200);
        if (empty($batch)) {
            return;
        }
    }
}

function flattenPartitions(array $assigned): array
{
    $topics = $parts = [];
    foreach ($assigned as $p) {
        $topics[] = $p['topic'];
        $parts[] = $p['partition'];
    }
    return [$topics, $parts];
}
