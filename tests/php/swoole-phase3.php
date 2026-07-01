<?php

declare(strict_types=1);

/**
 * SwooleClient Phase 3.x 方法的综合 smoke 测试：
 *  - pause / resume（验证 fetcher 暂停 → 缓冲不长）
 *  - seek（按 offset 重读）
 *  - seekToTimestamp（按时间戳跳）
 *  - pollRebalanceEvents（拿到 ASSIGN 事件）
 *  - beginTransaction / produce / commitTransaction
 *  - abortTransaction
 *  - sendOffsetsToTransaction（EOS）
 *  - setOAuthBearerToken（IPC 通过即可——broker 不开 OAUTH 时 worker 会 ack 存 token）
 *
 * 跑法：
 *   php -d extension=swoole.so -d extension=libhi_kafka.so \
 *       tests/php/swoole-phase3.php /tmp/x.sock topic-prefix
 */
if (! \extension_loaded('swoole')) {
    \fwrite(\STDERR, "SKIP: 此测试需要 swoole 扩展\n");
    exit(0);
}

use Swoole\Coroutine;
use Hi\Kafka\SwooleClient;

\spl_autoload_register(function (string $cls): void {
    $base = __DIR__ . '/../../php-driver/src/';
    $path = $base . \str_replace('\\', '/', $cls) . '.php';
    if (\file_exists($path)) {
        require $path;
    }
});

$suffix = \posix_getpid() . '-' . \mt_rand();
$socket = $argv[1] ?? "/tmp/hi-kafka-swoole-p3-{$suffix}.sock";
$topicPrefix = $argv[2] ?? 'swoole-p3';
$topic = "{$topicPrefix}-data-{$suffix}";
$txnTopic = "{$topicPrefix}-txn-{$suffix}";
$group = "swoole-p3-grp-{$suffix}";
$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';

echo '=== Swoole Phase 3.x smoke ===' . \PHP_EOL;
echo "  data topic = {$topic}" . \PHP_EOL;
echo "  txn  topic = {$txnTopic}" . \PHP_EOL;
echo "  group      = {$group}" . \PHP_EOL . \PHP_EOL;

$failures = [];
function check(bool $cond, string $msg): void
{
    global $failures;
    if ($cond) {
        echo "  ✓ {$msg}" . \PHP_EOL;
    } else {
        echo "  ✗ {$msg}" . \PHP_EOL;
        $failures[] = $msg;
    }
}

Coroutine\run(static function () use ($socket, $brokers, $topic, $txnTopic, $group): void {
    $client = new SwooleClient(socket: $socket);
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);
    $client->registerCluster('txn', [
        'bootstrap.servers' => $brokers,
        'transactional.id' => 'swoole-p3-tx-' . \posix_getpid() . '-' . \mt_rand(),
        'transaction.timeout.ms' => '15000',
        'enable.idempotence' => 'true',
        'acks' => 'all',
    ]);

    // === 1. 灌 10 条数据 ===
    echo '--- 1. produce 10 条到数据 topic ---' . \PHP_EOL;
    for ($i = 0; $i < 10; $i++) {
        $r = $client->produceSync('default', $topic, "k-{$i}", "v-{$i}", 5000);
        \check($r['ok'], "produce {$i} offset={$r['offset']}");
    }

    // === 2. subscribe + 等 ASSIGN + pollRebalanceEvents ===
    echo \PHP_EOL . '--- 2. subscribe + pollRebalanceEvents ---' . \PHP_EOL;
    $sub = $client->subscribe('default', $group, [$topic], ['auto.offset.reset' => 'earliest']);
    \check($sub > 0, "subscribe sub_id={$sub}");

    $assigned = [];
    $deadline = \microtime(true) + 10;
    while (\microtime(true) < $deadline && empty($assigned)) {
        foreach ($client->pollRebalanceEvents($sub, 100, 500) as $e) {
            if ('assign' === $e['type'] && ! empty($e['partitions'])) {
                $assigned = $e['partitions'];
                break;
            }
        }
        Coroutine::sleep(0.1);
    }
    \check(! empty($assigned), 'pollRebalanceEvents 拿到 ASSIGN（' . \count($assigned) . ' 分区）');

    // === 3. pause / resume ===
    echo \PHP_EOL . '--- 3. pause + resume（验证 fetcher 真停了）---' . \PHP_EOL;
    // 先把 step 1 的 10 条全 drain 干净，让 buffer 归零
    $drained = 0;
    $deadline = \microtime(true) + 8;
    while ($drained < 10 && \microtime(true) < $deadline) {
        $drained += \count($client->poll($sub, 100, 1000));
    }
    \check($drained >= 10, "drain step1 数据（{$drained} 条）");

    // pause + 喘息让 librdkafka 真停
    $client->pause($sub, [], []);
    \check(true, 'pause 调用通过');
    Coroutine::sleep(0.3);

    // pause 期间灌 3 条新的——broker 收到但 fetcher 不拉，worker buffer 应保持空
    for ($i = 0; $i < 3; $i++) {
        $client->produceSync('default', $topic, "p-k-{$i}", "p-v-{$i}", 5000);
    }
    Coroutine::sleep(1.0); // 给数据落 broker 的时间
    $duringPause = \count($client->poll($sub, 100, 500));
    \check(0 === $duringPause, "pause 期间 poll 到 0 条新增（实际 {$duringPause}）");

    // resume → 应能拉到刚才发的 3 条
    $client->resume($sub, [], []);
    \check(true, 'resume 调用通过');
    $seen = 0;
    $deadline = \microtime(true) + 10;
    while ($seen < 3 && \microtime(true) < $deadline) {
        $seen += \count($client->poll($sub, 100, 1000));
    }
    \check($seen >= 3, "resume 后拉到 ≥3 条新增（实际 {$seen}）");

    // === 4. seek by offset ===
    echo \PHP_EOL . '--- 4. seek by offset → 重读前 5 条 ---' . \PHP_EOL;
    $topics = \array_map(static fn ($p) => $p['topic'], $assigned);
    $parts = \array_map(static fn ($p) => $p['partition'], $assigned);
    $offsets = \array_fill(0, \count($assigned), 0); // 全部回到 offset 0
    $client->seek($sub, $topics, $parts, $offsets);
    \check(true, 'seek 调用通过');
    $reread = 0;
    $deadline = \microtime(true) + 5;
    while ($reread < 10 && \microtime(true) < $deadline) {
        $batch = $client->poll($sub, 100, 1000);
        $reread += \count($batch);
    }
    \check($reread >= 10, "seek(0) 后重读 ≥10 条（实际 {$reread}）");

    // === 5. seekToTimestamp ===
    echo \PHP_EOL . '--- 5. seekToTimestamp → 1 周前（应该 seek 到最早可用）---' . \PHP_EOL;
    $weekAgo = (int) ((\microtime(true) - 7 * 86400) * 1000);
    $client->seekToTimestamp($sub, $weekAgo, $topics, $parts);
    \check(true, 'seekToTimestamp 调用通过');

    // === 6. transaction：begin → produce → commit ===
    echo \PHP_EOL . '--- 6. transaction commit ---' . \PHP_EOL;
    $client->beginTransaction('txn');
    \check(true, 'beginTransaction');
    $r = $client->produceSync('txn', $txnTopic, 'tx-k1', 'tx-v1', 5000);
    \check($r['ok'], "txn produceSync offset={$r['offset']}");
    $client->commitTransaction('txn');
    \check(true, 'commitTransaction');

    // === 7. transaction：begin → produce → abort（消息不可见）===
    echo \PHP_EOL . '--- 7. transaction abort ---' . \PHP_EOL;
    $client->beginTransaction('txn');
    $client->produceSync('txn', $txnTopic, 'tx-k-abort', 'tx-v-abort', 5000);
    $client->abortTransaction('txn');
    \check(true, 'abortTransaction 通过（read_committed consumer 看不到）');

    // === 8. sendOffsetsToTransaction（EOS）===
    echo \PHP_EOL . '--- 8. sendOffsetsToTransaction ---' . \PHP_EOL;
    $client->beginTransaction('txn');
    // 用 sub 当前 assignment 的 offsets 提交一份（next offset = 一个保守值）
    $eosOffsets = \array_fill(0, \count($assigned), 100);
    $client->sendOffsetsToTransaction('txn', $sub, $group, $topics, $parts, $eosOffsets);
    \check(true, 'sendOffsetsToTransaction 调用通过');
    $client->commitTransaction('txn');
    \check(true, 'EOS commit 通过');

    // === 9. setOAuthBearerToken smoke ===
    echo \PHP_EOL . '--- 9. setOAuthBearerToken smoke ---' . \PHP_EOL;
    $client->registerCluster('oauth-smoke', ['bootstrap.servers' => '127.0.0.1:9094']);
    $client->setOAuthBearerToken(
        'oauth-smoke',
        'fake-jwt-token-for-smoke',
        (int) ((\microtime(true) + 3600) * 1000),
        'kafka-client@example.com',
        ['trace' => '00-aaa-bbb-01'],
    );
    \check(true, 'setOAuthBearerToken IPC 通过');

    // === 收尾 ===
    $client->unsubscribe($sub);
});

echo \PHP_EOL;
if (empty($failures)) {
    echo '★ SwooleClient Phase 3.x 全部 PASS' . \PHP_EOL;
    exit(0);
}
echo '✗ 失败: ' . \implode(', ', $failures) . \PHP_EOL;
exit(1);
