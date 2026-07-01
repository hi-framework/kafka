<?php

declare(strict_types=1);

/**
 * 验证 per-message headers 的端到端：
 *   produceSync(... + headers) → broker → poll → 收到的 message['headers'] 完整还原
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: headers.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$group = 'headers-grp-' . \posix_getpid();

$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', [
    'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

// === 阶段 1: 生产带 headers 的 5 条消息 ===
echo '=== 1. produce with headers ===' . \PHP_EOL;
$tracesSent = [];
for ($i = 0; $i < 5; $i++) {
    $traceparent = \sprintf('00-%032x-%016x-01', \mt_rand(), $i + 1);
    $headers = [
        'traceparent' => $traceparent,
        'source' => 'headers-test',
        'index' => (string) $i,
    ];
    // 注：当前 header value 限定 UTF-8（PHP HashMap<String,String> 限制）
    // 真二进制 header 支持留待 Phase 3.x（需 BinarySlice 类型适配）
    $tracesSent[$i] = $headers;

    // 注意：headers 现在是必传位置参数（传 [] 表示无 headers）
    $r = $client->produceSync('default', $topic, "k-{$i}", "v-{$i}", $headers, null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, "produce {$i} fail: " . \json_encode($r) . "\n");
        exit(1);
    }
}
echo '  produced 5 messages with 3 headers each (traceparent + source + index)' . \PHP_EOL;

// === 阶段 2: 订阅 + 消费 + 验证 headers ===
echo '=== 2. subscribe + consume + verify ===' . \PHP_EOL;
$sub = $client->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);

$received = [];
$rounds = 0;
while (\count($received) < 5 && $rounds < 8) {
    $batch = $client->poll($sub, 100, 2000);
    foreach ($batch as $m) {
        $received[(int) \mb_substr($m['key'], 2)] = $m;
    }
    $rounds++;
}

if (5 !== \count($received)) {
    \fwrite(\STDERR, 'FAIL: 只收到 ' . \count($received) . "/5 条\n");
    exit(1);
}

foreach ($received as $i => $m) {
    echo "  msg #{$i}: o={$m['offset']} headers=" . \json_encode($m['headers']) . \PHP_EOL;
    $expected = $tracesSent[$i];

    foreach ($expected as $k => $v) {
        if (! isset($m['headers'][$k])) {
            \fwrite(\STDERR, "FAIL: msg {$i} 缺 header '{$k}'\n");
            exit(1);
        }
        if ($m['headers'][$k] !== $v) {
            \fwrite(\STDERR, \sprintf(
                "FAIL: msg %d header '%s' 不匹配\n  期望 (%d bytes): %s\n  实际 (%d bytes): %s\n",
                $i,
                $k,
                \mb_strlen($v),
                \bin2hex($v),
                \mb_strlen($m['headers'][$k]),
                \bin2hex($m['headers'][$k]),
            ));
            exit(1);
        }
    }
}

$client->commit($sub);
$client->unsubscribe($sub);

echo \PHP_EOL . '★ headers e2e PASS（UTF-8 headers 完整透传 PHP → broker → PHP）' . \PHP_EOL;
