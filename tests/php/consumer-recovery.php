<?php

declare(strict_types=1);

if ($argc < 3) {
    \fwrite(\STDERR, "usage: consumer-recovery.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$group = 'recovery-grp-' . \posix_getpid();
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', [
    'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

// 阶段 1：先生产 5 条
echo '=== 阶段 1: 生产 5 条（offset 0..4）===' . \PHP_EOL;
for ($i = 0; $i < 5; $i++) {
    $r = $client->produceSync('default', $topic, 'k', "phase1-{$i}", [], null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, "produce fail\n");
        exit(1);
    }
}
echo '  done' . \PHP_EOL;

// 阶段 2: 订阅 + 消费 + commit
echo '=== 阶段 2: 订阅 → 消费 5 → commit ===' . \PHP_EOL;
// 降低 session.timeout 让 kill 后的旧 consumer 实例快速从 broker 视角下线
$sub = $client->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);
echo "  virtual subscription_id: {$sub}" . \PHP_EOL;

$got = 0;
$rounds = 0;
while ($got < 5 && $rounds < 10) {
    $batch = $client->poll($sub, 100, 2000);
    foreach ($batch as $m) {
        echo "  msg: o={$m['offset']} v={$m['value']}" . \PHP_EOL;
        $got++;
    }
    $rounds++;
}
echo "  consumed {$got}/5" . \PHP_EOL;
$client->commit($sub);
echo '  committed' . \PHP_EOL;

// 阶段 3: kill -9 worker（通过 PID 文件定位）
echo '=== 阶段 3: kill -9 worker ===' . \PHP_EOL;
$pidFile = $socket . '.pid';
$pid = \is_file($pidFile) ? (int) \trim(\file_get_contents($pidFile)) : 0;
echo "  worker PID: {$pid}" . \PHP_EOL;
if ($pid > 0) {
    \posix_kill($pid, \SIGKILL);
}
\usleep(300_000);

// 阶段 4: 再生产 5 条
echo '=== 阶段 4: 生产新 5 条（offset 5..9）' . \PHP_EOL;
for ($i = 0; $i < 5; $i++) {
    $r = $client->produceSync('default', $topic, 'k', "phase4-{$i}", [], null, null, 10000);
    if (! $r['ok']) {
        \fwrite(\STDERR, "produce fail in phase4\n");
        exit(1);
    }
}
echo "  done, last offset={$r['offset']}" . \PHP_EOL;

// 阶段 5: 用同一个 $sub 继续 poll，期望透明重订阅 + 拿到新 5 条
echo "=== 阶段 5: 用原 \$sub={$sub} 继续 poll（期望透明重订阅）===" . \PHP_EOL;
$start = \microtime(true);
$got2 = 0;
$rounds = 0;
while ($got2 < 5 && $rounds < 10) {
    try {
        $batch = $client->poll($sub, 100, 3000);
        foreach ($batch as $m) {
            echo "  msg: o={$m['offset']} v={$m['value']}" . \PHP_EOL;
            $got2++;
        }
    } catch (Throwable $e) {
        echo "  poll error: {$e->getMessage()}" . \PHP_EOL;
        break;
    }
    $rounds++;
}
$ms = (\microtime(true) - $start) * 1000;
\printf("  consumed %d/5 in %.0fms (rounds=%d)\n", $got2, $ms, $rounds);

// 阶段 6: commit + unsubscribe
$client->commit($sub);
$client->unsubscribe($sub);
echo '=== 阶段 6: commit + unsubscribe OK ===' . \PHP_EOL;

// 阶段 7: 统计
echo '=== 阶段 7: 自愈统计 ===' . \PHP_EOL;
echo '  ipc retry: ' . \json_encode(\hi_kafka_retry_stats()) . \PHP_EOL;
echo '  resubscribe: ' . \json_encode(\hi_kafka_resubscribe_stats()) . \PHP_EOL;

if ($got2 < 5) {
    \fwrite(\STDERR, "FAIL: 期望恢复后拿到 5 条，实际 {$got2}\n");
    exit(1);
}
echo \PHP_EOL . 'PASS' . \PHP_EOL;
