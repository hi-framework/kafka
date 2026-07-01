<?php

declare(strict_types=1);

/**
 * 验证 K：所有控制类 RPC（不止 produce）在 worker 死后都能透明 retry。
 *
 * 跑 5 个阶段：
 *  1. 起 worker，正常 registerCluster
 *  2. kill -9 worker
 *  3. 第一个 RPC 是 registerCluster——期望自动 retry 成功（worker 重启 + cluster 重注册）
 *  4. 第二个 RPC 是 subscribe——期望自动 retry 成功
 *  5. 第三个 RPC 是 unsubscribe（fire-and-forget）——期望不抛
 *
 * 验证 hi_kafka_retry_stats() 中 attempts ≥ 3 / successes ≥ 3 / failures == 0。
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: control-recovery.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;
$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$client = new Hi\Kafka\Client($socket);

echo '=== 1. 起 worker + registerCluster ===' . \PHP_EOL;
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);
$client->produceSync('default', $topic, 'k', 'baseline', [], null, null, 5000);
echo '  baseline produce ok' . \PHP_EOL;

echo '=== 2. kill -9 worker ===' . \PHP_EOL;
$pidFile = $socket . '.pid';
$pid = \is_file($pidFile) ? (int) \trim(\file_get_contents($pidFile)) : 0;
if ($pid <= 0) {
    \fwrite(\STDERR, "FAIL: 找不到 worker pid 文件\n");
    exit(1);
}
\posix_kill($pid, \SIGKILL);
echo "  killed pid={$pid}" . \PHP_EOL;
\usleep(300_000);

$attemptsBefore = \hi_kafka_retry_stats()['attempts'];

echo '=== 3. 第一个 RPC: registerCluster（控制类）===' . \PHP_EOL;
$start = \microtime(true);

try {
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);
    \printf("  RECOVERED in %.0fms\n", (\microtime(true) - $start) * 1000);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: registerCluster: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 4. 第二个 RPC: subscribe（控制类）===' . \PHP_EOL;
// 把当前 worker 也杀一遍，强制再次自愈
$pid = \is_file($pidFile) ? (int) \trim(\file_get_contents($pidFile)) : 0;
if ($pid > 0) {
    \posix_kill($pid, \SIGKILL);
    echo "  re-killed pid={$pid}" . \PHP_EOL;
    \usleep(300_000);
}
// subscribe 前需要 cluster 注册——但 worker 是 fresh 的
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);
$start = \microtime(true);

try {
    $sub = $client->subscribe('default', 'ctrl-rcv-grp-$$', [$topic], ['auto.offset.reset' => 'earliest']);
    \printf("  RECOVERED in %.0fms, sub_id=%d\n", (\microtime(true) - $start) * 1000, $sub);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: subscribe: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 5. 第三个 RPC: unsubscribe（fire-and-forget）===' . \PHP_EOL;
$pid = \is_file($pidFile) ? (int) \trim(\file_get_contents($pidFile)) : 0;
if ($pid > 0) {
    \posix_kill($pid, \SIGKILL);
    echo "  re-killed pid={$pid}" . \PHP_EOL;
    \usleep(300_000);
}
$start = \microtime(true);

try {
    $client->unsubscribe($sub);
    \printf("  RECOVERED in %.0fms\n", (\microtime(true) - $start) * 1000);
} catch (Throwable $e) {
    \fwrite(\STDERR, 'FAIL: unsubscribe: ' . $e->getMessage() . \PHP_EOL);
    exit(1);
}

echo '=== 6. retry stats ===' . \PHP_EOL;
$stats = \hi_kafka_retry_stats();
echo '  ' . \json_encode($stats) . \PHP_EOL;
$delta = $stats['attempts'] - $attemptsBefore;
if ($delta < 3) {
    \fwrite(\STDERR, "FAIL: 期望 ≥3 次 retry attempts（实际 {$delta）}\n");
    exit(1);
}
if ($stats['failures'] > 0) {
    \fwrite(\STDERR, "FAIL: 期望 retry failures = 0（实际 {$stats['failures']}）\n");
    exit(1);
}
echo "  ✓ {$delta} 次 retry，全部成功，无 failures" . \PHP_EOL;

echo \PHP_EOL . '★ 控制类 RPC 自动 retry PASS' . \PHP_EOL;
