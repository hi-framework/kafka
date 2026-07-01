<?php

declare(strict_types=1);

/**
 * 验证 worker hysteresis 自动背压（双水位）：
 *   缓冲累积超 PAUSE_AT → auto-pause（fetcher 停），metric `hi_kafka_consumer_pause_total` +1
 *   缓冲被消费降到 RESUME_AT 以下 → auto-resume，metric `hi_kafka_consumer_resume_total` +1
 *
 * 通过极小的缓冲上限 + 大量塞消息 + 慢速 poll 触发 auto-pause；再一次性快速 poll
 * 让缓冲降到 resume 水位下触发 auto-resume。
 *
 * 必需 env：
 *   HI_KAFKA_BROKERS                        broker 地址
 *   HI_KAFKA_METRICS_ADDR=127.0.0.1:PORT   worker 暴露 Prometheus /metrics 端点
 *   HI_KAFKA_CONSUMER_BUFFER_CAPACITY=200  缓冲上限
 *   HI_KAFKA_CONSUMER_PAUSE_AT=160         80% 触发 pause
 *   HI_KAFKA_CONSUMER_RESUME_AT=40         20% 触发 resume
 *
 * run-all-e2e.sh 会 export 这些 env；单跑本脚本时手动 export。
 */
if ($argc < 3) {
    \fwrite(\STDERR, "usage: auto-backpressure.php SOCKET TOPIC_PREFIX\n");
    exit(2);
}

[$_, $socket, $topicPrefix] = $argv;

$metricsAddr = \getenv('HI_KAFKA_METRICS_ADDR') ?: '';
if ('' === $metricsAddr) {
    // 需要 metrics 端点才能观测计数；未配置就跳过（run-all-e2e.sh 会打 SKIP）
    echo "SKIP: HI_KAFKA_METRICS_ADDR 未设置，无法读取 auto-pause 计数" . \PHP_EOL;
    exit(0);
}

$suffix = \posix_getpid() . '-' . \mt_rand();
$topic = "{$topicPrefix}-{$suffix}";
$group = "backp-grp-{$suffix}";
$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';

$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

echo "topic={$topic} group={$group} metrics=http://{$metricsAddr}/metrics" . \PHP_EOL;

/** 抓 Prometheus 计数（单值）。 */
function fetch_counter(string $addr, string $name): int
{
    $body = @\file_get_contents("http://{$addr}/metrics");
    if (false === $body) {
        return -1;
    }
    foreach (\explode("\n", $body) as $line) {
        $line = \trim($line);
        if ('' === $line || $line[0] === '#') {
            continue;
        }
        // "name{...} value" or "name value"
        if (\preg_match('/^' . \preg_quote($name, '/') . '(?:\{[^}]*\})?\s+(\d+)/', $line, $m)) {
            return (int) $m[1];
        }
    }
    return 0;
}

// === Phase 1: 塞 500 条大 payload（超过 PAUSE_AT=160，触发 auto-pause）=======
echo \PHP_EOL . '=== 1. 生产 500 条 · 每条 4 KB ===' . \PHP_EOL;
$value = \str_repeat('x', 4 * 1024);
for ($i = 0; $i < 500; $i++) {
    $client->produceFnf('default', $topic, 'k', $value);
}
// 让 fnf 都真的 flush 到 broker（fnf 只等 worker enqueue，未等 broker）
\sleep(1);
echo "  produced 500 msgs" . \PHP_EOL;

$pauseBefore = fetch_counter($metricsAddr, 'hi_kafka_consumer_pause_total');
$resumeBefore = fetch_counter($metricsAddr, 'hi_kafka_consumer_resume_total');
echo "  metric before: pause={$pauseBefore} resume={$resumeBefore}" . \PHP_EOL;

// === Phase 2: subscribe + 慢 poll → worker 缓冲堆积 → 触发 auto-pause =======
echo \PHP_EOL . '=== 2. subscribe + slow poll，等 auto-pause ===' . \PHP_EOL;
$sub = $client->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
]);

// 慢 poll：每轮拉 5 条，等 200ms；worker 后台 stream_loop 会把消息塞满缓冲
$totalRead = 0;
$deadline = \microtime(true) + 10.0;
while (\microtime(true) < $deadline) {
    $msgs = $client->poll($sub, 5, 100);
    $totalRead += \count($msgs);
    \usleep(200_000); // 慢消费：200ms
    $pauseNow = fetch_counter($metricsAddr, 'hi_kafka_consumer_pause_total');
    if ($pauseNow > $pauseBefore) {
        echo "  ✓ auto-pause 触发（+" . ($pauseNow - $pauseBefore) . "），共消费 {$totalRead} 条" . \PHP_EOL;
        break;
    }
}

$pauseMid = fetch_counter($metricsAddr, 'hi_kafka_consumer_pause_total');
if ($pauseMid <= $pauseBefore) {
    \fwrite(\STDERR, "FAIL: 10s 内未触发 auto-pause（pauseBefore={$pauseBefore} pauseMid={$pauseMid}）\n");
    $client->unsubscribe($sub);
    exit(1);
}

// === Phase 3: 快速 drain → 缓冲降到 resume 水位下 → auto-resume ==============
echo \PHP_EOL . '=== 3. 快速 drain，等 auto-resume ===' . \PHP_EOL;
$deadline = \microtime(true) + 10.0;
while (\microtime(true) < $deadline) {
    $msgs = $client->poll($sub, 200, 500);
    if (! $msgs) {
        // 缓冲空了，pause 后 fetcher 停了 → 让 resume 生效 broker 才有新数据
        $resumeNow = fetch_counter($metricsAddr, 'hi_kafka_consumer_resume_total');
        if ($resumeNow > $resumeBefore) {
            echo "  ✓ auto-resume 触发（+" . ($resumeNow - $resumeBefore) . "）" . \PHP_EOL;
            break;
        }
        \usleep(100_000);
        continue;
    }
    $totalRead += \count($msgs);
    $resumeNow = fetch_counter($metricsAddr, 'hi_kafka_consumer_resume_total');
    if ($resumeNow > $resumeBefore) {
        echo "  ✓ auto-resume 触发（+" . ($resumeNow - $resumeBefore) . "），共消费 {$totalRead} 条" . \PHP_EOL;
        break;
    }
}

$resumeAfter = fetch_counter($metricsAddr, 'hi_kafka_consumer_resume_total');
$pauseAfter = fetch_counter($metricsAddr, 'hi_kafka_consumer_pause_total');

$client->unsubscribe($sub);

echo \PHP_EOL;
echo "metric after: pause={$pauseAfter} resume={$resumeAfter}" . \PHP_EOL;
echo "delta       : pause=+" . ($pauseAfter - $pauseBefore)
    . " resume=+" . ($resumeAfter - $resumeBefore) . \PHP_EOL;

if ($pauseAfter > $pauseBefore && $resumeAfter > $resumeBefore) {
    echo \PHP_EOL . "★ auto-backpressure PASS （pause + resume 计数都 +1 以上）" . \PHP_EOL;
    exit(0);
}
\fwrite(\STDERR, "FAIL: auto-pause 或 auto-resume 未触发\n");
exit(1);
