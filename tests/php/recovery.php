<?php

declare(strict_types=1);

if ($argc < 3) {
    \fwrite(\STDERR, "usage: recovery.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', [
    'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

// 阶段 1：正常生产 5 条
echo '=== 阶段 1: 正常生产 ===' . \PHP_EOL;
for ($i = 0; $i < 5; $i++) {
    $r = $client->produceSync('default', $topic, 'k', "phase1-{$i}", [], null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, 'phase1 fail: ' . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
}
echo "  5/5 ok, last offset={$r['offset']}" . \PHP_EOL;

// 阶段 2: kill -9 worker（通过 PID 文件定位）
echo '=== 阶段 2: kill -9 worker ===' . \PHP_EOL;
$pidFile = $socket . '.pid';
$pid = \is_file($pidFile) ? (int) \trim(\file_get_contents($pidFile)) : 0;
echo "  worker PID: {$pid}" . \PHP_EOL;
if ($pid > 0) {
    \posix_kill($pid, \SIGKILL);
}
\usleep(300_000);

// 阶段 3：再次 produce，期望透明恢复
echo '=== 阶段 3: kill 后第一次 produce ===' . \PHP_EOL;
$start = \microtime(true);

try {
    $r = $client->produceSync('default', $topic, 'k', 'phase3-after-kill', [], null, null, 10000);
    $ms = (\microtime(true) - $start) * 1000;
    if ($r['ok']) {
        \printf("  RECOVERED in %.0fms, offset=%d\n", $ms, $r['offset']);
    } else {
        echo '  not-ok: ' . \json_encode($r) . \PHP_EOL;
        exit(1);
    }
} catch (Throwable $e) {
    \printf("  FAILED: %s (after %.0fms)\n", $e->getMessage(), (\microtime(true) - $start) * 1000);
    exit(1);
}

// 阶段 4：恢复后继续生产
echo '=== 阶段 4: 恢复后继续生产 ===' . \PHP_EOL;
for ($i = 0; $i < 5; $i++) {
    $r = $client->produceSync('default', $topic, 'k', "phase4-{$i}", [], null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, "phase4 fail at {$i}: " . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
}
echo "  5/5 ok, last offset={$r['offset']}" . \PHP_EOL;

// 阶段 5：统计
echo '=== 阶段 5: 统计 ===' . \PHP_EOL;
echo '  retry: ' . \json_encode(\hi_kafka_retry_stats()) . \PHP_EOL;
echo '  pool:  ' . \json_encode(\hi_kafka_pool_stats()) . \PHP_EOL;

echo \PHP_EOL . '★ Worker 自动重启 + producer 自愈 PASS' . \PHP_EOL;
