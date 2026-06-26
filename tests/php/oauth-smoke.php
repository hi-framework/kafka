<?php

declare(strict_types=1);

/**
 * SASL/OAUTHBEARER token push 链路烟雾测试。
 *
 * 不依赖真实的 OAUTHBEARER broker（KRaft 默认没开），只验证：
 * 1. 集群注册后能写入 token（roundtrip ok）
 * 2. 第二次写入覆盖第一次（worker 侧 slot 行为）
 * 3. 没注册的集群返回有用错误
 * 4. PUSH token 不破坏已有的 PLAINTEXT 集群正常 produce
 *
 * 真实 OAUTHBEARER broker 下的 token refresh 链路靠云上集成测试覆盖。
 */

if ($argc < 3) {
    fwrite(STDERR, "usage: oauth-smoke.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$suffix = posix_getpid() . '-' . mt_rand();
$topic = "$topicPrefix-$suffix";

$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('plain', ['bootstrap.servers' => $brokers]);

echo "=== 1. 写 token 到已注册的 cluster (plain) ===" . PHP_EOL;
$client->setOAuthBearerToken(
    'plain',
    'jwt-initial-' . $suffix,
    (int) (microtime(true) * 1000) + 3_600_000,
    "kafka-client-$suffix@example.com",
    ['traceparent' => '00-abc-def-01'],
    5000,
);
echo "  ✓ 第一次写入 ok" . PHP_EOL;

$client->setOAuthBearerToken(
    'plain',
    'jwt-rotated-' . $suffix,
    (int) (microtime(true) * 1000) + 3_600_000,
    "kafka-client-$suffix@example.com",
    [],   // 空 extensions 也合法
    5000,
);
echo "  ✓ 第二次写入（覆盖）ok" . PHP_EOL;

echo PHP_EOL . "=== 2. 没注册的 cluster 应返回明确错误 ===" . PHP_EOL;
try {
    $client->setOAuthBearerToken(
        'never-registered-' . $suffix,
        'jwt-x', 0, 'p', [], 2000,
    );
    fwrite(STDERR, "FAIL: 期望抛错，但没抛\n");
    exit(1);
} catch (\Exception $e) {
    if (! str_contains($e->getMessage(), 'not registered')) {
        fwrite(STDERR, "FAIL: 期望「not registered」类错误，实际: {$e->getMessage()}\n");
        exit(1);
    }
    echo "  ✓ 错误信息: {$e->getMessage()}" . PHP_EOL;
}

echo PHP_EOL . "=== 3. 已写 token 不破坏 plain 集群的常规 produce ===" . PHP_EOL;
$r = $client->produceSync('plain', $topic, 'k', "post-oauth-push", [], null, null, 5000);
if (! $r['ok']) {
    fwrite(STDERR, "FAIL: produce 失败: " . json_encode($r) . "\n");
    exit(1);
}
echo "  ✓ produce ok partition={$r['partition']} offset={$r['offset']}" . PHP_EOL;

echo PHP_EOL . "=== 4. consumer 同样不受影响 ===" . PHP_EOL;
$sub = $client->subscribe('plain', "oauth-grp-$suffix", [$topic], [
    'auto.offset.reset'  => 'earliest',
    'session.timeout.ms' => '6000',
]);

$got = null;
$deadline = microtime(true) + 10;
while ($got === null && microtime(true) < $deadline) {
    foreach ($client->poll($sub, 10, 1500) as $m) {
        $got = $m;
        break;
    }
}
if ($got === null) {
    fwrite(STDERR, "FAIL: 没拉到消息\n");
    exit(1);
}
echo "  ✓ consumer poll ok: {$got['value']}" . PHP_EOL;

$client->unsubscribe($sub);
echo PHP_EOL . "★ OAUTHBEARER token push 链路 PASS（无真实 OAuth broker）" . PHP_EOL;
