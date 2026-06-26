<?php

declare(strict_types=1);

/**
 * 验证精确 seek：
 *
 * 阶段 1：生产 10 条 + 等 1 秒 + 再生产 5 条（记录时间戳分界）
 * 阶段 2：订阅 + 消费 5 条
 * 阶段 3：seek by offset 回到 offset=3，验证从 3 开始重读
 * 阶段 4：seek by timestamp 到分界时间，验证只读到后 5 条
 */

if ($argc < 3) {
    fwrite(STDERR, "usage: seek.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;

$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', [
    'bootstrap.servers' => getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

// === 阶段 1：生产 10 条（早），等 1.5s，再 5 条（晚）===
echo "=== 1. 生产 10 早 + 5 晚（时间戳分两段）===" . PHP_EOL;
for ($i = 0; $i < 10; $i++) {
    $client->produceSync('default', $topic, 'k', "early-$i", [], null, null, 5000);
}
$boundaryMs = (int) (microtime(true) * 1000);
echo "  时间戳分界 = $boundaryMs" . PHP_EOL;
usleep(1_500_000);

for ($i = 0; $i < 5; $i++) {
    $client->produceSync('default', $topic, 'k', "late-$i", [], null, null, 5000);
}
echo "  生产完毕（topic 共 15 条）" . PHP_EOL;

// === 阶段 2：订阅 + 等 ASSIGN + 消费 5 条 ===
echo PHP_EOL . "=== 2. 订阅 + 消费前 5 条 ===" . PHP_EOL;
$sub = $client->subscribe('default', 'seek-grp-' . posix_getpid(), [$topic], [
    'auto.offset.reset'    => 'earliest',
    'session.timeout.ms'   => '6000',
    'heartbeat.interval.ms'=> '2000',
]);

// 等 ASSIGN 事件（seek 必须在 assign 后）
$assigned = [];
$deadline = microtime(true) + 8;
while (microtime(true) < $deadline && empty($assigned)) {
    foreach ($client->pollRebalanceEvents($sub, 100, 500) as $e) {
        if ($e['type'] === 'assign') {
            foreach ($e['partitions'] as $p) {
                $assigned[] = $p;
            }
        }
    }
    $client->poll($sub, 0, 100);  // 驱动 rebalance callback
}
echo "  ASSIGN: " . count($assigned) . " 分区" . PHP_EOL;
if (empty($assigned)) {
    fwrite(STDERR, "FAIL: 没拿到 ASSIGN 事件\n");
    exit(1);
}

$consumed = [];
while (count($consumed) < 5) {
    foreach ($client->poll($sub, 100, 1500) as $m) {
        $consumed[] = $m;
        if (count($consumed) >= 5) break;
    }
}
echo "  消费前 5 条：" . implode(', ', array_map(fn($m) => "o={$m['offset']}/{$m['value']}", $consumed)) . PHP_EOL;

// === 阶段 3：seek by offset 回到 offset=3 ===
echo PHP_EOL . "=== 3. seek by offset → 回到 offset=3 ===" . PHP_EOL;
$topics = [];
$parts = [];
$offsets = [];
foreach ($assigned as $p) {
    $topics[] = $p['topic'];
    $parts[] = $p['partition'];
    $offsets[] = 3;
}
$client->seek($sub, $topics, $parts, $offsets);

$replayed = [];
$deadline = microtime(true) + 5;
while (microtime(true) < $deadline && count($replayed) < 5) {
    foreach ($client->poll($sub, 100, 1000) as $m) {
        $replayed[] = $m;
        if (count($replayed) >= 5) break 2;
    }
}
echo "  seek 后重读：" . implode(', ', array_map(fn($m) => "o={$m['offset']}/{$m['value']}", $replayed)) . PHP_EOL;

if (empty($replayed)) {
    fwrite(STDERR, "FAIL: seek 后没拿到消息\n");
    exit(1);
}
if ($replayed[0]['offset'] !== 3) {
    fwrite(STDERR, "FAIL: 期望从 offset=3 开始，实际 {$replayed[0]['offset']}\n");
    exit(1);
}
echo "  ✓ offset=3 起" . PHP_EOL;

// === 阶段 4：seek by timestamp → 跳到时间分界（应只看到 "late-*"）===
echo PHP_EOL . "=== 4. seek by timestamp → 跳到分界 $boundaryMs ===" . PHP_EOL;
$client->seekToTimestamp($sub, $boundaryMs, $topics, $parts);

$lateOnly = [];
$deadline = microtime(true) + 5;
while (microtime(true) < $deadline && count($lateOnly) < 5) {
    foreach ($client->poll($sub, 100, 1000) as $m) {
        $lateOnly[] = $m;
        if (count($lateOnly) >= 5) break 2;
    }
}
echo "  timestamp seek 后：" . implode(', ', array_map(fn($m) => "o={$m['offset']}/{$m['value']}", $lateOnly)) . PHP_EOL;

if (empty($lateOnly)) {
    fwrite(STDERR, "FAIL: timestamp seek 后没拿到消息\n");
    exit(1);
}

foreach ($lateOnly as $m) {
    if (! str_starts_with($m['value'], 'late-')) {
        fwrite(STDERR, "FAIL: 时间戳之后应只有 'late-*'，实际拿到 '{$m['value']}' (offset {$m['offset']})\n");
        exit(1);
    }
}
echo "  ✓ 全部是 'late-*'（时间戳精确定位 5 条晚消息）" . PHP_EOL;

$client->unsubscribe($sub);
echo PHP_EOL . "★ Seek by offset + by timestamp PASS" . PHP_EOL;
