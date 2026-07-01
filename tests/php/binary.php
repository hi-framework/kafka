<?php

declare(strict_types=1);

/**
 * 验证 binary-safe produce：key / value / header value 均含任意字节
 * （NUL、0xFF、非 UTF-8 序列），全链路保留不变。
 *
 * 阶段 1：produceSyncBin 写入 1 条含 NUL 字节的 value + binary header
 * 阶段 2：subscribe + poll 拿到那条消息
 * 阶段 3：bytewise 校验 key / value / 每个 header value 与原始字节完全一致
 *
 * 这是「protobuf / msgpack / 加密 payload 可以直接当 value 用」的回归保证。
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: binary.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$suffix = \posix_getpid() . '-' . \mt_rand();
$topic = "{$topicPrefix}-{$suffix}";
$group = "bin-grp-{$suffix}";

$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

echo "topic={$topic} group={$group}" . \PHP_EOL . \PHP_EOL;

// ============================================================================
// 构造刁钻的二进制 payload
// ============================================================================
// key：含 NUL 字节
$key = "k\x00ey\x00with\x00nuls";

// value：含 NUL + 高位字节 + 非 UTF-8 序列（0xC3 后接非续接字节）
$value = "binary\x00value\xFF\xC3\xFF" . \pack('N', 0xCAFEBABE) . \random_bytes(16);

// header：name 是 ASCII（Kafka 协议要求），value 是任意字节
$headerNames = ['content-type', 'trace-id', 'signature'];
$headerValues = [
    'application/x-protobuf',
    \pack('NN', 0xDEADBEEF, 0xCAFEBABE),     // 8 bytes binary
    \random_bytes(32),                         // 32 bytes random binary
];

echo '=== 1. produceSyncBin 写入二进制消息 ===' . \PHP_EOL;
echo '  key bytes (hex)        : ' . \bin2hex($key) . \PHP_EOL;
echo '  value bytes (hex, head) : ' . \bin2hex(\mb_substr($value, 0, 20)) . '... (len=' . \mb_strlen($value) . ')' . \PHP_EOL;
foreach ($headerValues as $i => $hv) {
    echo "  header[{$headerNames[$i]}]: " . \bin2hex(\mb_substr($hv, 0, 16))
        . (\mb_strlen($hv) > 16 ? '...' : '') . ' (len=' . \mb_strlen($hv) . ')' . \PHP_EOL;
}

$resp = $client->produceSyncBin(
    'default',
    $topic,
    $key,
    $value,
    $headerNames,
    $headerValues,
    null,
    null,
    5000,
);
if (! $resp['ok']) {
    \fwrite(\STDERR, 'FAIL: produceSyncBin: ' . \json_encode($resp) . "\n");
    exit(1);
}
echo "  ok partition={$resp['partition']} offset={$resp['offset']}" . \PHP_EOL . \PHP_EOL;

// ============================================================================
// 订阅 + 等 ASSIGN + poll
// ============================================================================
echo '=== 2. subscribe + poll ===' . \PHP_EOL;
$sub = $client->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);

\waitAssign($client, $sub, 15);

$got = null;
$deadline = \microtime(true) + 10;
while (null === $got && \microtime(true) < $deadline) {
    foreach ($client->poll($sub, 10, 1500) as $m) {
        $got = $m;
        break;
    }
}
if (null === $got) {
    \fwrite(\STDERR, "FAIL: 没拉到消息\n");
    exit(1);
}
echo "  poll offset={$got['offset']} key_len=" . \mb_strlen($got['key'])
    . ' value_len=' . \mb_strlen($got['value']) . ' headers=' . \count($got['headers']) . \PHP_EOL . \PHP_EOL;

// ============================================================================
// bytewise 校验
// ============================================================================
echo '=== 3. bytewise 校验 ===' . \PHP_EOL;

if ($got['key'] !== $key) {
    \fwrite(\STDERR, "FAIL: key 字节不匹配\n");
    \fwrite(\STDERR, '  expected hex: ' . \bin2hex($key) . "\n");
    \fwrite(\STDERR, '  got hex     : ' . \bin2hex($got['key']) . "\n");
    exit(1);
}
echo '  ✓ key 完全一致 (' . \mb_strlen($key) . ' bytes, 含 NUL)' . \PHP_EOL;

if ($got['value'] !== $value) {
    \fwrite(\STDERR, "FAIL: value 字节不匹配\n");
    \fwrite(\STDERR, '  expected hex (head): ' . \bin2hex(\mb_substr($value, 0, 32)) . "\n");
    \fwrite(\STDERR, '  got hex      (head): ' . \bin2hex(\mb_substr($got['value'], 0, 32)) . "\n");
    \fwrite(\STDERR, '  expected len = ' . \mb_strlen($value) . ', got len = ' . \mb_strlen($got['value']) . "\n");
    exit(1);
}
echo '  ✓ value 完全一致 (' . \mb_strlen($value) . ' bytes, 含 NUL + 0xFF + 非 UTF-8)' . \PHP_EOL;

foreach ($headerNames as $i => $name) {
    if (! isset($got['headers'][$name])) {
        \fwrite(\STDERR, "FAIL: header[{$name}] 缺失\n");
        exit(1);
    }
    $expected = $headerValues[$i];
    $actual = $got['headers'][$name];
    if ($actual !== $expected) {
        \fwrite(\STDERR, "FAIL: header[{$name}] 字节不匹配\n");
        \fwrite(\STDERR, '  expected hex: ' . \bin2hex($expected) . "\n");
        \fwrite(\STDERR, '  got hex     : ' . \bin2hex($actual) . "\n");
        exit(1);
    }
    echo "  ✓ header[{$name}] 完全一致 (" . \mb_strlen($expected) . ' bytes)' . \PHP_EOL;
}

$client->unsubscribe($sub);
echo \PHP_EOL . '★ Binary-safe key/value/headers PASS' . \PHP_EOL;

// ============================================================================
// helpers
// ============================================================================

function waitAssign(Hi\Kafka\Client $client, int $sub, float $maxSecs): array
{
    $assigned = [];
    $deadline = \microtime(true) + $maxSecs;
    while (\microtime(true) < $deadline && empty($assigned)) {
        foreach ($client->pollRebalanceEvents($sub, 100, 500) as $e) {
            if ('assign' === $e['type'] && ! empty($e['partitions'])) {
                $assigned = $e['partitions'];
                break;
            }
        }
        if (empty($assigned)) {
            \usleep(200_000);
        }
    }
    if (empty($assigned)) {
        \fwrite(\STDERR, "FAIL: 等不到 ASSIGN\n");
        exit(1);
    }
    return $assigned;
}
