<?php

declare(strict_types=1);

/**
 * 验证 Hi\Kafka 配置类组合：ConnectionConfig + ProducerConfig + ConsumerConfig
 * 全套翻译为 librdkafka 参数 + 真实 broker 上跑通。
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: configs.php SOCKET TOPIC\n");
    exit(2);
}

[$_, $socket, $topic] = $argv;

require_once __DIR__ . '/../../../src/Kafka/ConnectionConfig.php';
require_once __DIR__ . '/../../../src/Kafka/ConsumerConfig.php';
require_once __DIR__ . '/../../../src/Kafka/ProducerConfig.php';
require_once __DIR__ . '/../../../src/Kafka/ConsumeOffsetType.php';

use Hi\Kafka\ConnectionConfig;
use Hi\Kafka\ConsumerConfig;
use Hi\Kafka\ProducerConfig;
use Hi\Kafka\ConsumeOffsetType;

$client = new Hi\Kafka\Client($socket);

// === 1. ConnectionConfig (PLAINTEXT) ===
echo '=== Connection config ===' . \PHP_EOL;
$conn = new ConnectionConfig(
    brokers: ['127.0.0.1:9094'],
);
$connMap = $conn->toLibrdkafkaConfig();
echo '  connection map: ' . \json_encode($connMap) . \PHP_EOL;
\assert('127.0.0.1:9094' === $connMap['bootstrap.servers']);
\assert('PLAINTEXT' === $connMap['security.protocol']);

// === 2. ProducerConfig ===
echo \PHP_EOL . '=== Producer config ===' . \PHP_EOL;
$prod = (new ProducerConfig)
    ->setCompressionType('lz4')
    ->setLingerMs(5)
    ->setBatchSize(16384)
    ->setAcks('all')
    ->setIdempotent(true)
    ->setMessageMaxBytes(1_000_000)
;
$prodMap = $prod->toLibrdkafkaConfig();
echo '  producer map: ' . \json_encode($prodMap) . \PHP_EOL;
\assert('lz4' === $prodMap['compression.type']);
\assert('all' === $prodMap['acks']);
\assert('true' === $prodMap['enable.idempotence']);

// === 3. 注册集群（合并 connection + producer 配置）===
echo \PHP_EOL . '=== registerCluster ===' . \PHP_EOL;
$client->registerCluster('typed', \array_merge($connMap, $prodMap));
echo "  registered cluster 'typed' with " . (\count($connMap) + \count($prodMap)) . ' keys' . \PHP_EOL;

// === 4. produce ===
echo \PHP_EOL . '=== produceSync × 5 ===' . \PHP_EOL;
for ($i = 0; $i < 5; $i++) {
    $r = $client->produceSync('typed', $topic, "k-{$i}", "configs-test-{$i}", [], null, null, 5000);
    if (! $r['ok']) {
        \fwrite(\STDERR, 'produce fail: ' . \json_encode($r) . \PHP_EOL);
        exit(1);
    }
}
echo "  5/5 ok, last offset={$r['offset']}" . \PHP_EOL;

// === 5. ConsumerConfig ===
echo \PHP_EOL . '=== Consumer config ===' . \PHP_EOL;
$cons = (new ConsumerConfig)
    ->setGroupId('configs-grp-' . \posix_getpid())
    ->setTopics([$topic])
    ->setOffset(ConsumeOffsetType::AtStart)              // ← 从头消费
    ->setSessionTimeoutMs(6000)
    ->setHeartbeatIntervalMs(2000)
    ->setMaxPollIntervalMs(60000)
    ->setIsolationLevel('read_committed')
    ->setFetchMinBytes(1)
    ->setExtra([
        'partition.assignment.strategy' => 'cooperative-sticky',
    ])
;
$consMap = $cons->toLibrdkafkaConfig();
echo '  consumer map: ' . \json_encode($consMap) . \PHP_EOL;
\assert('earliest' === $consMap['auto.offset.reset']);
\assert('read_committed' === $consMap['isolation.level']);

// === 6. subscribe + poll ===
echo \PHP_EOL . '=== subscribe + poll ===' . \PHP_EOL;
$sub = $client->subscribe('typed', $cons->getGroupId(), $cons->getTopics(), $consMap);
echo "  subscription_id: {$sub}" . \PHP_EOL;

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
$client->commit($sub);
$client->unsubscribe($sub);

if (5 !== $got) {
    \fwrite(\STDERR, "FAIL: 期望 5 条，实际 {$got}\n");
    exit(1);
}

echo \PHP_EOL . '★ 所有配置类 PASS' . \PHP_EOL;
