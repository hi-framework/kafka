<?php

declare(strict_types=1);

/**
 * 验证 L：业务 registerCluster **一次**，扩展端记住；worker 死后所有 RPC
 * 透明 retry 时自动重放 cluster 注册。业务侧无须再调 registerCluster。
 *
 * 与 control-recovery.php 区别：那个测试每次 kill 后业务手动再注册一次；
 * 这个测试只在最开始注册一次，后续 produce / subscribe 触发自愈链。
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: replay-recovery.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$client = new Hi\Kafka\Client($socket);

function killWorker(string $socket): void
{
    $pidFile = $socket . '.pid';
    $pid = \is_file($pidFile) ? (int) \trim(\file_get_contents($pidFile)) : 0;
    if ($pid > 0) {
        \posix_kill($pid, \SIGKILL);
        echo "  killed pid={$pid}" . \PHP_EOL;
        \usleep(300_000);
    }
}

echo '=== 1. 一次性 registerCluster ===' . \PHP_EOL;
$client->registerCluster('main', ['bootstrap.servers' => $brokers]);
$client->registerCluster('audit', ['bootstrap.servers' => $brokers]);
echo '  注册 2 个 cluster: main + audit' . \PHP_EOL;

echo '=== 2. baseline produce ===' . \PHP_EOL;
$r = $client->produceSync('main', $topic, 'k', 'main-baseline', [], null, null, 5000);
$r = $client->produceSync('audit', $topic, 'k', 'audit-baseline', [], null, null, 5000);
echo '  main + audit 各 1 条 OK' . \PHP_EOL;

echo '=== 3. kill worker，业务不重新 register ===' . \PHP_EOL;
\killWorker($socket);

echo '=== 4. produce 到 main——靠 L 重放 cluster ===' . \PHP_EOL;
$start = \microtime(true);

try {
    $r = $client->produceSync('main', $topic, 'k', 'main-after-kill', [], null, null, 10000);
    if (! $r['ok']) {
        \fwrite(\STDERR, 'FAIL: main produce not-ok: ' . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
    \printf("  ✓ main RECOVERED in %.0fms, offset=%d\n", (\microtime(true) - $start) * 1000, $r['offset']);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: main produce throw: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 5. produce 到 audit——同一新 worker，cluster 已被一并 replay ===' . \PHP_EOL;
$start = \microtime(true);

try {
    $r = $client->produceSync('audit', $topic, 'k', 'audit-after-kill', [], null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, 'FAIL: audit produce not-ok: ' . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
    \printf("  ✓ audit RECOVERED in %.0fms\n", (\microtime(true) - $start) * 1000);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: audit produce throw: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 6. kill 再来一遍 + subscribe，subscribe 也走 replay ===' . \PHP_EOL;
\killWorker($socket);
$start = \microtime(true);

try {
    $sub = $client->subscribe(
        'main',
        'replay-grp-' . \posix_getpid(),
        [$topic],
        ['auto.offset.reset' => 'earliest'],
    );
    \printf("  ✓ subscribe RECOVERED in %.0fms, sub_id=%d\n", (\microtime(true) - $start) * 1000, $sub);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: subscribe throw: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 7. ensureWorker 也带 replay：kill 后调 ensureWorker 单独验证 ===' . \PHP_EOL;
\killWorker($socket);
$start = \microtime(true);

try {
    $client->ensureWorker();
    // ensureWorker 后 cluster 应已重放——这条 produce 不会再触发 retry
    $statsBefore = \hi_kafka_retry_stats()['attempts'];
    $r = $client->produceSync('main', $topic, 'k', 'after-ensure', [], null, null, 5000);
    $statsAfter = \hi_kafka_retry_stats()['attempts'];
    if ($statsAfter !== $statsBefore) {
        \fwrite(\STDERR, 'FAIL: ensureWorker 后 produce 仍触发 retry（attempts 增长 ' . ($statsAfter - $statsBefore) . "），说明 replay 没起作用\n");
        exit(1);
    }
    \printf("  ✓ ensureWorker + produce 一气呵成 in %.0fms\n", (\microtime(true) - $start) * 1000);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: ensureWorker + produce throw: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 8. retry stats ===' . \PHP_EOL;
echo '  ' . \json_encode(\hi_kafka_retry_stats()) . \PHP_EOL;

$client->unsubscribe($sub);
echo \PHP_EOL . '★ L: cluster replay + ensureWorker 透明自愈 PASS' . \PHP_EOL;
