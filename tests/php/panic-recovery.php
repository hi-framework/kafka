<?php

declare(strict_types=1);

/**
 * 验证扩展端 IPC 自动重试（`hi_kafka_retry_stats`）在 worker 崩溃场景下正确记账。
 *
 * 场景：worker 被 kill -9 后，客户端下一次调用应：
 *   1. 首次撞 BrokenPipe / ConnectionRefused（socket 半关闭 / 连不上）
 *   2. `should_invalidate` 判断为需 invalidate → 重试
 *   3. 重试期间自动重新 spawn worker → 成功
 *   4. `retry_stats.attempts` +1、`retry_stats.successes` +1
 *
 * 与 recovery.php 的区别：recovery 只验业务侧透明恢复；本脚本专门核对
 * `hi_kafka_retry_stats` 的三个字段（attempts / successes / failures）在
 * 崩溃 + 恢复流程中的**准确变化**——这是运维观测点，回归会掩盖 metric 漂移。
 *
 * 不依赖 Prometheus 端点（`hi_kafka_retry_stats` 直接从 PHP 侧读原子计数）。
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: panic-recovery.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$topic = $topicPrefix . '-' . \posix_getpid();
$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';

$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

// === Phase 1: 拿基准 retry_stats + 正常 produce 1 条 ============================
echo '=== 1. baseline + normal produce ===' . \PHP_EOL;
$before = \hi_kafka_retry_stats();
\printf("  baseline: attempts=%d successes=%d failures=%d\n",
    $before['attempts'], $before['successes'], $before['failures']);

$r = $client->produceSync('default', $topic, 'k', 'baseline', [], null, null, 5000);
if (! $r['ok']) {
    \fwrite(\STDERR, "FAIL: baseline produce 未 ok: " . \json_encode($r) . \PHP_EOL);
    exit(1);
}
echo "  ✓ produce ok, offset={$r['offset']}" . \PHP_EOL;

$afterBaseline = \hi_kafka_retry_stats();
if ($afterBaseline['attempts'] !== $before['attempts']) {
    \fwrite(\STDERR, "FAIL: baseline 阶段 retry attempts 意外变化\n");
    exit(1);
}
echo "  ✓ baseline 阶段 retry_stats 不变（无 worker 故障）" . \PHP_EOL;

// === Phase 2: kill -9 worker，读 pid 文件定位进程 ==============================
echo \PHP_EOL . '=== 2. kill -9 worker ===' . \PHP_EOL;
$pidFile = $socket . '.pid';
if (! \is_file($pidFile)) {
    \fwrite(\STDERR, "FAIL: worker pid file 缺失: {$pidFile}\n");
    exit(1);
}
$pid = (int) \trim(\file_get_contents($pidFile));
if ($pid <= 0) {
    \fwrite(\STDERR, "FAIL: 非法 worker pid: {$pid}\n");
    exit(1);
}
echo "  worker pid={$pid}" . \PHP_EOL;
\posix_kill($pid, \SIGKILL);
// 等 socket 半关闭 / 内核清理 fd
\usleep(300_000);

// === Phase 3: worker 死后第一次 produce 应透明恢复 =============================
echo \PHP_EOL . '=== 3. first produce after kill (expects retry) ===' . \PHP_EOL;
$statsBeforeKill = \hi_kafka_retry_stats();
$start = \microtime(true);
try {
    $r = $client->produceSync('default', $topic, 'k', 'after-kill', [], null, null, 10000);
    $elapsed = (\microtime(true) - $start) * 1000;
    if (! $r['ok']) {
        \fwrite(\STDERR, "FAIL: 恢复后 produce 未 ok: " . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
    \printf("  ✓ RECOVERED in %.0fms, offset=%d\n", $elapsed, $r['offset']);
} catch (\Throwable $e) {
    \fwrite(\STDERR, "FAIL: 未透明恢复：" . $e->getMessage() . \PHP_EOL);
    exit(1);
}

$statsAfterRecover = \hi_kafka_retry_stats();
\printf(
    "  retry_stats after recover: attempts=%d (+%d) successes=%d (+%d) failures=%d (+%d)\n",
    $statsAfterRecover['attempts'],
    $statsAfterRecover['attempts'] - $statsBeforeKill['attempts'],
    $statsAfterRecover['successes'],
    $statsAfterRecover['successes'] - $statsBeforeKill['successes'],
    $statsAfterRecover['failures'],
    $statsAfterRecover['failures'] - $statsBeforeKill['failures'],
);

$attemptsDelta = $statsAfterRecover['attempts'] - $statsBeforeKill['attempts'];
$successesDelta = $statsAfterRecover['successes'] - $statsBeforeKill['successes'];
$failuresDelta = $statsAfterRecover['failures'] - $statsBeforeKill['failures'];

$fail = 0;
if ($attemptsDelta < 1) {
    \fwrite(\STDERR, "  FAIL: attempts delta 应 ≥1，实际 {$attemptsDelta}\n");
    $fail++;
}
if ($successesDelta < 1) {
    \fwrite(\STDERR, "  FAIL: successes delta 应 ≥1（重试成功），实际 {$successesDelta}\n");
    $fail++;
}
if ($failuresDelta !== 0) {
    \fwrite(\STDERR, "  FAIL: failures delta 应 =0（重试后成功），实际 {$failuresDelta}\n");
    $fail++;
}

// === Phase 4: 恢复后连续 3 条不应再触发 retry ==================================
echo \PHP_EOL . '=== 4. subsequent produces 不应再触发 retry ===' . \PHP_EOL;
$statsBeforeSteady = \hi_kafka_retry_stats();
for ($i = 0; $i < 3; $i++) {
    $r = $client->produceSync('default', $topic, 'k', "steady-{$i}", [], null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, "FAIL steady {$i}: " . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
}
$statsAfterSteady = \hi_kafka_retry_stats();
$steadyAttempts = $statsAfterSteady['attempts'] - $statsBeforeSteady['attempts'];
if ($steadyAttempts !== 0) {
    \fwrite(\STDERR, "  FAIL: 稳态阶段仍触发 {$steadyAttempts} 次 retry\n");
    $fail++;
} else {
    echo "  ✓ 稳态：3 条 produce 无 retry" . \PHP_EOL;
}

if ($fail === 0) {
    echo \PHP_EOL . "★ panic-recovery PASS （retry_stats 三字段变化符合预期）" . \PHP_EOL;
    exit(0);
}
exit(1);
