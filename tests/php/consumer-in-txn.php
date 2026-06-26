<?php

declare(strict_types=1);

/**
 * 验证 consumer-in-transaction (Kafka exactly-once stream 处理)。
 *
 * 场景：经典 read → transform → write 流水线，崩溃恢复 EOS。
 *
 * - Phase 1：种子 N 条消息进 IN topic（普通 producer）
 *
 * - Phase 2 commit 路径：
 *     subscribe IN (read_committed, group=A)
 *     等 ASSIGN → poll N 条
 *     beginTransaction(txn)
 *     produceSync N 条变换后的 → OUT
 *     sendOffsetsToTransaction(txn, sub, A, IN offsets+1)
 *     commitTransaction(txn)
 *
 *   验证：
 *     a) reader 看 OUT 能看到 N 条变换后的消息
 *     b) 用同样 group=A 重新订阅 IN → poll 0 条（offset 已 commit）
 *
 * - Phase 3 abort 路径：
 *     再种 M 条进 IN
 *     subscribe IN (read_committed, group=B)
 *     poll M 条
 *     beginTransaction → produceSync M 条 → OUT
 *     sendOffsetsToTransaction(txn, sub, B, IN offsets+1)
 *     abortTransaction
 *
 *   验证：
 *     a) reader 看 OUT 看不到 abort 的输出（read_committed 隔离）
 *     b) 用同样 group=B 重新订阅 IN → 拿回 M 条（offset 没 commit）
 */

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-in-txn.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;
$suffix = posix_getpid() . '-' . mt_rand();
$txnId  = 'hi-kafka-eos-' . $suffix;
$topicIn  = "$topicPrefix-$suffix-in";
$topicOut = "$topicPrefix-$suffix-out";
$groupA = "eos-A-$suffix";
$groupB = "eos-B-$suffix";

$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$client = new Hi\Kafka\Client($socket);

// 三个集群：txn 用 producer 事务，seed 给输入种子（无事务），reader 给消费验证
$client->registerCluster('txn', [
    'bootstrap.servers'      => $brokers,
    'transactional.id'       => $txnId,
    'transaction.timeout.ms' => '15000',
    'enable.idempotence'     => 'true',
    'acks'                   => 'all',
]);
$client->registerCluster('seed', [
    'bootstrap.servers' => $brokers,
]);
$client->registerCluster('reader', [
    'bootstrap.servers' => $brokers,
]);
echo "txn.id=$txnId" . PHP_EOL;
echo "topics: IN=$topicIn  OUT=$topicOut" . PHP_EOL . PHP_EOL;

// ----------------------------------------------------------------------------
// Phase 1：种 5 条进 IN
// ----------------------------------------------------------------------------
echo "=== 1. 种 5 条进 IN ===" . PHP_EOL;
for ($i = 0; $i < 5; $i++) {
    $r = $client->produceSync('seed', $topicIn, 'k', "msg-$i", [], null, null, 5000);
    if (! $r['ok']) {
        fwrite(STDERR, "FAIL: seed produce 失败: " . json_encode($r) . "\n");
        exit(1);
    }
}
echo "  种 5 条 (offsets 0..4)" . PHP_EOL . PHP_EOL;

// ----------------------------------------------------------------------------
// Phase 2：commit 路径 —— EOS read → transform → write
// ----------------------------------------------------------------------------
echo "=== 2. EOS commit 路径 ===" . PHP_EOL;
$sub = subscribeAndWaitAssign($client, 'reader', $groupA, $topicIn);
echo "  ASSIGN ok, group=$groupA" . PHP_EOL;

// poll 5 条
$batch = pollExactly($client, $sub, 5, 15);
echo "  poll: " . implode(', ', array_map(fn($m) => "o={$m['offset']}/{$m['value']}", $batch)) . PHP_EOL;

// 算出每分区下一条要读的 offset（last_consumed + 1）
$nextOffsets = computeNextOffsets($batch);
echo "  next-offsets: " . json_encode($nextOffsets) . PHP_EOL;

// 事务：begin → 写 OUT → send_offsets → commit
$client->beginTransaction('txn');
foreach ($batch as $m) {
    $client->produceSync('txn', $topicOut, 'k', strtoupper($m['value']), [], null, null, 5000);
}
[$topics, $partitions, $offsets] = flattenOffsets($nextOffsets);
$client->sendOffsetsToTransaction('txn', $sub, $groupA, $topics, $partitions, $offsets);
$client->commitTransaction('txn');
echo "  ✓ commit (写 OUT 5 条 + 提交 IN offsets)" . PHP_EOL;

$client->unsubscribe($sub);

// ----- 验证 2a: reader 看 OUT 能看到 5 条变换后的消息 -----
echo "  验证 2a: OUT 可见 5 条 UPPERCASE" . PHP_EOL;
$outSub = subscribeAndWaitAssign(
    $client, 'reader', 'verify-out-' . posix_getpid(), $topicOut,
    ['auto.offset.reset' => 'earliest', 'isolation.level' => 'read_committed'],
);
$outMsgs = pollExactly($client, $outSub, 5, 6);
echo "  OUT: " . implode(', ', array_map(fn($m) => $m['value'], $outMsgs)) . PHP_EOL;
foreach ($outMsgs as $m) {
    if (! preg_match('/^MSG-[0-4]$/', $m['value'])) {
        fwrite(STDERR, "FAIL: OUT 不应有 '{$m['value']}'\n");
        exit(1);
    }
}
$client->unsubscribe($outSub);

// ----- 验证 2b: 同 groupA 重订阅 IN → 拿不到消息（offset 已 commit）-----
echo "  验证 2b: 同 group=$groupA 重订阅 IN → 应为空" . PHP_EOL;
$replay = subscribeAndWaitAssign(
    $client, 'reader', $groupA, $topicIn,
    ['auto.offset.reset' => 'earliest', 'isolation.level' => 'read_committed'],
);
$leftover = [];
$deadline = microtime(true) + 3;
while (microtime(true) < $deadline) {
    foreach ($client->poll($replay, 100, 500) as $m) {
        $leftover[] = $m;
    }
}
echo "  leftover: " . count($leftover) . " 条" . PHP_EOL;
if (! empty($leftover)) {
    fwrite(STDERR, "FAIL: 同 group 重订阅 IN 应该为空（offset 应已 commit），实际拿到 "
        . count($leftover) . " 条：" . json_encode(array_map(fn($m) => $m['offset'], $leftover)) . "\n");
    exit(1);
}
$client->unsubscribe($replay);
echo "  ✓ 2b PASS（offsets 在事务里成功提交）" . PHP_EOL . PHP_EOL;

// ----------------------------------------------------------------------------
// Phase 3：abort 路径 —— 输入 offset 不应被 commit，输出不应可见
// ----------------------------------------------------------------------------
echo "=== 3. EOS abort 路径 ===" . PHP_EOL;
for ($i = 5; $i < 8; $i++) {
    $client->produceSync('seed', $topicIn, 'k', "msg-$i", [], null, null, 5000);
}
echo "  再种 3 条进 IN (msg-5..msg-7)" . PHP_EOL;

$subB = subscribeAndWaitAssign($client, 'reader', $groupB, $topicIn);
$batchB = pollExactly($client, $subB, 3, 12);
echo "  poll: " . implode(', ', array_map(fn($m) => "o={$m['offset']}/{$m['value']}", $batchB)) . PHP_EOL;
$nextOffsetsB = computeNextOffsets($batchB);

$client->beginTransaction('txn');
foreach ($batchB as $m) {
    $client->produceSync('txn', $topicOut, 'k', 'ABORTED-' . $m['value'], [], null, null, 5000);
}
[$tB, $pB, $oB] = flattenOffsets($nextOffsetsB);
$client->sendOffsetsToTransaction('txn', $subB, $groupB, $tB, $pB, $oB);
$client->abortTransaction('txn');
echo "  ✗ abort (放弃 OUT 写入 + 放弃 IN offsets)" . PHP_EOL;

$client->unsubscribe($subB);

// ----- 验证 3a: reader 看 OUT 不应有 ABORTED-* -----
echo "  验证 3a: OUT 不应有 ABORTED-*（read_committed 隔离）" . PHP_EOL;
$verifyOut = subscribeAndWaitAssign(
    $client, 'reader', 'verify-abort-' . posix_getpid(), $topicOut,
    ['auto.offset.reset' => 'earliest', 'isolation.level' => 'read_committed'],
);
$allOut = [];
$deadline = microtime(true) + 4;
while (microtime(true) < $deadline) {
    foreach ($client->poll($verifyOut, 100, 1000) as $m) {
        $allOut[] = $m['value'];
    }
}
echo "  OUT 当前内容: " . json_encode($allOut) . PHP_EOL;
foreach ($allOut as $v) {
    if (str_starts_with($v, 'ABORTED-')) {
        fwrite(STDERR, "FAIL: read_committed 看到了 abort 的消息: $v\n");
        exit(1);
    }
}
$client->unsubscribe($verifyOut);

// ----- 验证 3b: 同 groupB 重订阅 IN → 应能拿回与 batchB 同样的消息 -----
echo "  验证 3b: 同 group=$groupB 重订阅 IN → 应能重读 batchB 的消息（offset 没 commit）" . PHP_EOL;
$replayB = subscribeAndWaitAssign(
    $client, 'reader', $groupB, $topicIn,
    ['auto.offset.reset' => 'earliest', 'isolation.level' => 'read_committed'],
);
$got = pollExactly($client, $replayB, count($batchB), 12);
$gotByPartition = [];
foreach ($got as $m) {
    $gotByPartition[$m['partition']][] = $m['offset'];
}
$origByPartition = [];
foreach ($batchB as $m) {
    $origByPartition[$m['partition']][] = $m['offset'];
}
echo "  原始 offsets: " . json_encode($origByPartition) . PHP_EOL;
echo "  replay offsets: " . json_encode($gotByPartition) . PHP_EOL;

// 关键：replay 的最小 offset 必须 ≤ batchB 的最小 offset（offset 没被 commit）
foreach ($origByPartition as $part => $origOffs) {
    $replayOffs = $gotByPartition[$part] ?? [];
    if (empty($replayOffs)) {
        fwrite(STDERR, "FAIL: partition $part 没有 replay 消息\n");
        exit(1);
    }
    if (min($replayOffs) > min($origOffs)) {
        fwrite(STDERR, "FAIL: partition $part 的 replay 起点 (" . min($replayOffs)
            . ") 大于 batchB 起点 (" . min($origOffs) . "); offset 似乎已 commit\n");
        exit(1);
    }
}
$client->unsubscribe($replayB);
echo "  ✓ 3b PASS（abort 后 input offset 没动，可重读）" . PHP_EOL . PHP_EOL;

echo "★ Consumer-in-transaction (EOS) PASS" . PHP_EOL;

// ============================================================================
// helpers
// ============================================================================

function subscribeAndWaitAssign(
    Hi\Kafka\Client $client,
    string $cluster,
    string $group,
    string $topic,
    array $extraCfg = [],
): int {
    $cfg = array_merge([
        'auto.offset.reset'     => 'earliest',
        'isolation.level'       => 'read_committed',
        'session.timeout.ms'    => '6000',
        'heartbeat.interval.ms' => '2000',
    ], $extraCfg);

    $sub = $client->subscribe($cluster, $group, [$topic], $cfg);
    $assigned = false;
    $deadline = microtime(true) + 15;
    // 不调 poll() 来等 ASSIGN：worker 端 StreamConsumer 在后台 task 里跑，
    // librdkafka 自身的 poll 线程会触发 rebalance callback；PHP poll 反而会
    // 消耗已 fetch 进缓冲的业务消息（max_messages=0 在 ext 层被 clamp 到 1）。
    while (microtime(true) < $deadline && ! $assigned) {
        foreach ($client->pollRebalanceEvents($sub, 100, 500) as $e) {
            if ($e['type'] === 'assign' && ! empty($e['partitions'])) {
                $assigned = true;
                break;
            }
        }
        if (! $assigned) {
            usleep(200_000);
        }
    }
    if (! $assigned) {
        fwrite(STDERR, "FAIL: 等不到 ASSIGN 事件 (group=$group, topic=$topic)\n");
        exit(1);
    }
    return $sub;
}

function pollExactly(Hi\Kafka\Client $client, int $sub, int $want, float $maxSecs): array
{
    $out = [];
    $deadline = microtime(true) + $maxSecs;
    while (count($out) < $want && microtime(true) < $deadline) {
        foreach ($client->poll($sub, $want, 2000) as $m) {
            $out[] = $m;
            if (count($out) >= $want) {
                break;
            }
        }
    }
    if (count($out) < $want) {
        $got = array_map(fn($m) => "o={$m['offset']}/{$m['value']}", $out);
        fwrite(STDERR, "FAIL: 期望 poll $want 条，实际 " . count($out) . " 条: " . implode(', ', $got) . "\n");
        exit(1);
    }
    return $out;
}

/**
 * 从一批消息算出每 (topic, partition) 的「下一条要读的 offset」 = max(offset)+1。
 */
function computeNextOffsets(array $batch): array
{
    $next = [];
    foreach ($batch as $m) {
        $key = $m['topic'] . ':' . $m['partition'];
        $cur = $next[$key]['offset'] ?? -1;
        if ($m['offset'] > $cur) {
            $next[$key] = [
                'topic'     => $m['topic'],
                'partition' => $m['partition'],
                'offset'    => $m['offset'] + 1,
            ];
        }
    }
    return array_values($next);
}

/** 把 [{topic,partition,offset}] 拆成三个平行数组。 */
function flattenOffsets(array $next): array
{
    $topics = $parts = $offsets = [];
    foreach ($next as $n) {
        $topics[]  = $n['topic'];
        $parts[]   = $n['partition'];
        $offsets[] = $n['offset'];
    }
    return [$topics, $parts, $offsets];
}
