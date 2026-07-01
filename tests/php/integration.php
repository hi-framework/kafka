<?php

declare(strict_types=1);

/**
 * 综合集成测试：跑 producer + consumer 全链路，作为 CI 回归基线。
 *
 * 用法：
 *   php -d extension=path/to/libhi_kafka.{so,dylib} \
 *       -d hi_kafka.worker_bin=path/to/hi-kafka-worker \
 *       tests/php/integration.php SOCKET TOPIC
 *
 * 退出码：
 *   0 = 全部 PASS
 *   1 = 某个阶段 FAIL
 *   2 = 参数错误
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: integration.php SOCKET TOPIC [--with-kafka]\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$withKafka = \in_array('--with-kafka', $argv, true);
$group = 'integration-grp-' . \posix_getpid();

$failures = [];

function step(string $label, callable $fn): void
{
    global $failures;
    echo "--- {$label} ---" . \PHP_EOL;
    $start = \microtime(true);

    try {
        $fn();
        $ms = (\microtime(true) - $start) * 1000;
        \printf("  PASS (%.0fms)\n", $ms);
    } catch (Throwable $e) {
        $ms = (\microtime(true) - $start) * 1000;
        \printf("  FAIL (%.0fms): %s\n", $ms, $e->getMessage());
        $failures[] = $label;
    }
}

function assertTrue(bool $cond, string $msg): void
{
    if (! $cond) {
        throw new RuntimeException("assert failed: {$msg}");
    }
}

$client = new Hi\Kafka\Client($socket);
echo 'ext version: ' . \hi_kafka_version() . \PHP_EOL;
echo 'runtime: ' . \implode(', ', \hi_kafka_runtime()) . \PHP_EOL;
echo "socket: {$socket}" . \PHP_EOL;
echo "topic: {$topic}" . \PHP_EOL;
echo 'with_kafka: ' . ($withKafka ? 'yes' : 'no (LoggingProducer)') . \PHP_EOL . \PHP_EOL;

// ============================================================================
\step('1. ensure_worker 显式拉起', static function () use ($client): void {
    $client->ensureWorker();
});

// ============================================================================
\step('1b. registerCluster default', static function () use ($client): void {
    $brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
    $client->registerCluster('default', [
        'bootstrap.servers' => $brokers,
    ]);
});

// ============================================================================
\step('2. produce fire-and-forget × 10', static function () use ($client, $topic): void {
    for ($i = 0; $i < 10; $i++) {
        $client->produceFnf('default', $topic, "fnf-{$i}", "fnf-value-{$i}", []);
    }
});

// ============================================================================
\step('3. produce sync × 10（验证 offset）', static function () use ($client, $topic, $withKafka): void {
    $lastOffset = -2;
    for ($i = 0; $i < 10; $i++) {
        $r = $client->produceSync('default', $topic, "sync-{$i}", "sync-value-{$i}", [], null, null, 5000);
        \assertTrue($r['ok'], "sync {$i}: " . \json_encode($r));
        if ($withKafka) {
            \assertTrue($r['offset'] >= 0, 'real offset expected, got ' . \json_encode($r));
            if ($lastOffset >= 0) {
                \assertTrue($r['offset'] === $lastOffset + 1, "offset 不连续: {$lastOffset} → {$r['offset']}");
            }
            $lastOffset = $r['offset'];
        }
    }
});

// ============================================================================
$subscriptionId = null;
\step('4. subscribe', static function () use ($client, $topic, $group, &$subscriptionId): void {
    $subscriptionId = $client->subscribe('default', $group, [$topic], [
        'auto.offset.reset' => 'earliest',
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '2000',
    ]);
    \assertTrue($subscriptionId > 0, 'subscription_id should be positive');
});

// ============================================================================
if ($withKafka) {
    $consumedFnf = 0;
    $consumedSync = 0;
    \step('5. poll 全部 20 条', static function () use ($client, &$subscriptionId, &$consumedFnf, &$consumedSync): void {
        $deadline = \microtime(true) + 15;
        while (\microtime(true) < $deadline) {
            $batch = $client->poll($subscriptionId, 100, 1500);
            foreach ($batch as $m) {
                if (\str_starts_with($m['value'], 'fnf-')) {
                    $consumedFnf++;
                }
                if (\str_starts_with($m['value'], 'sync-')) {
                    $consumedSync++;
                }
            }
            if ($consumedFnf + $consumedSync >= 20) {
                break;
            }
        }
        \assertTrue(10 === $consumedFnf, "fnf 收到 {$consumedFnf}/10");
        \assertTrue(10 === $consumedSync, "sync 收到 {$consumedSync}/10");
    });

    \step('6. commit + unsubscribe', static function () use ($client, &$subscriptionId): void {
        $client->commit($subscriptionId);
        $client->unsubscribe($subscriptionId);
    });
} else {
    \step('5. poll (LoggingConsumer dry-run)', static function () use ($client, &$subscriptionId): void {
        // LoggingConsumer 没有真实消息，poll 应返回空 array
        $batch = $client->poll($subscriptionId, 10, 500);
        \assertTrue(\is_array($batch), 'poll should return array');
    });

    \step('6. commit + unsubscribe（dry-run）', static function () use ($client, &$subscriptionId): void {
        $client->commit($subscriptionId);
        $client->unsubscribe($subscriptionId);
    });
}

// ============================================================================
\step('7. 统计快照', static function (): void {
    $pool = \hi_kafka_pool_stats();
    $retry = \hi_kafka_retry_stats();
    $resub = \hi_kafka_resubscribe_stats();

    echo '  pool:  ' . \json_encode($pool, \JSON_UNESCAPED_SLASHES) . \PHP_EOL;
    echo '  retry: ' . \json_encode($retry) . \PHP_EOL;
    echo '  resub: ' . \json_encode($resub) . \PHP_EOL;

    // 基本健康：retry/resub failures 必须为 0（前面没注入故障）
    \assertTrue(0 === $retry['failures'], 'retry.failures != 0');
    \assertTrue(0 === $resub['failures'], 'resub.failures != 0');

    // 池命中率检查：30+ produce → hits 占绝大多数
    foreach ($pool as $entry) {
        if ($entry['acquires'] >= 10) {
            $hitRate = $entry['hits'] / $entry['acquires'];
            \assertTrue($hitRate > 0.7, "pool hit rate {$hitRate} 偏低");
        }
    }
});

// ============================================================================
echo \PHP_EOL;
if (empty($failures)) {
    echo '★ 全部 PASS' . \PHP_EOL;
    exit(0);
}
echo '✗ 失败阶段: ' . \implode(', ', $failures) . \PHP_EOL;
exit(1);
