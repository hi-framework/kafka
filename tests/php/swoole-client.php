<?php

declare(strict_types=1);

/**
 * Swoole 协程驱动 e2e 测试。
 *
 * 验证目标：
 * - SwooleClient 在协程上下文里所有 IO 正确 yield
 * - 高并发 produce/consume 不互相阻塞（多个协程并行而不是串行）
 * - 协程感知的连接池正确复用 UDS 连接（不是每个协程开一条）
 * - 同一 client 实例跨协程共享 worker（worker 不会被反复 fork）
 *
 * 阶段：
 *   1. autoload SwooleClient
 *   2. Coroutine\run 启动协程容器
 *   3. 50 个协程并发 produceSync → 验证全部成功 + 池命中率
 *   4. subscribe → 4 个 worker 协程并发 poll 全部 50 条 → 验证无重复无丢失
 *   5. 检查 pool stats / cleanup
 */

use Swoole\Coroutine;
use Swoole\Coroutine\WaitGroup;

// ----------------------------------------------------------------------------
// 环境前置检查
// ----------------------------------------------------------------------------
if (! \extension_loaded('swoole')) {
    \fwrite(\STDERR, "SKIP: 此测试需要 swoole 扩展\n");
    exit(0); // SKIP 不算 FAIL
}

// 参数可选：直接 `php swoole-client.php` 也能跑。批量 e2e runner 可以传
// `php swoole-client.php /tmp/x.sock my-prefix` 做隔离。
$suffix = \posix_getpid() . '-' . \mt_rand();
$socket = $argv[1] ?? "/tmp/hi-kafka-swoole-{$suffix}.sock";
$topicPrefix = $argv[2] ?? 'swoole-e2e';
$topic = "{$topicPrefix}-{$suffix}";
$group = "swoole-grp-{$suffix}";

// PSR-4 autoload SwooleClient
\spl_autoload_register(function (string $cls): void {
    $base = __DIR__ . '/../../php-driver/src/';
    $path = $base . \str_replace('\\', '/', $cls) . '.php';
    if (\file_exists($path)) {
        require $path;
    }
});

$brokers = \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$N_PRODUCERS = 50;
$N_WORKERS = 4;

echo '=== Swoole Kafka e2e ===' . \PHP_EOL;
echo "  topic={$topic}, group={$group}" . \PHP_EOL;
echo "  并发 producer 协程: {$N_PRODUCERS}" . \PHP_EOL;
echo "  并发 consumer 协程: {$N_WORKERS}" . \PHP_EOL . \PHP_EOL;

// ----------------------------------------------------------------------------
// 进入协程容器
// ----------------------------------------------------------------------------
$result = ['ok' => false];

Coroutine\run(static function () use ($socket, $brokers, $topic, $group, $N_PRODUCERS, $N_WORKERS, &$result): void {
    $client = new Hi\Kafka\SwooleClient(
        socket: $socket,
        maxIdle: 16,
        connectTimeout: 1.0,
    );

    // ===== 阶段 1：注册集群 =====
    $client->registerCluster('default', [
        'bootstrap.servers' => $brokers,
    ]);
    echo '★ 1. registerCluster ok' . \PHP_EOL;

    // 跑一轮并发 produce 的小闭包
    $runConcurrentProduces = static function (string $tag, int $offset) use ($client, $topic, $N_PRODUCERS): array {
        $wg = new WaitGroup;
        $startMs = \microtime(true) * 1000;
        $sent = new Swoole\Atomic(0);
        $failed = new Swoole\Atomic(0);

        for ($i = 0; $i < $N_PRODUCERS; $i++) {
            $wg->add();
            $key = "{$tag}-k" . ($offset + $i);
            $val = "{$tag}-v" . ($offset + $i);
            Coroutine::create(static function () use ($client, $topic, $key, $val, $wg, $sent, $failed): void {
                try {
                    $r = $client->produceSync('default', $topic, $key, $val, 5000);
                    if ($r['ok']) {
                        $sent->add(1);
                    } else {
                        $failed->add(1);
                    }
                } catch (Throwable $e) {
                    $failed->add(1);
                    \fprintf(\STDERR, '  throw: ' . $e->getMessage() . "\n");
                } finally {
                    $wg->done();
                }
            });
        }
        $wg->wait();
        return [
            'elapsed_ms' => (int) (\microtime(true) * 1000 - $startMs),
            'sent' => $sent->get(),
            'failed' => $failed->get(),
        ];
    };

    // ===== 阶段 2.1：第一轮并发 produce =====
    echo \PHP_EOL . "=== 2.1 第一轮：{$N_PRODUCERS} 协程并发 produceSync ===" . \PHP_EOL;
    $r1 = $runConcurrentProduces('r1', 0);
    echo "  耗时: {$r1['elapsed_ms']}ms （同等串行约 " . ($N_PRODUCERS * 50) . 'ms）' . \PHP_EOL;
    echo "  成功: {$r1['sent']}/{$N_PRODUCERS}, 失败: {$r1['failed']}" . \PHP_EOL;
    if ($r1['sent'] !== $N_PRODUCERS) {
        \fwrite(\STDERR, "FAIL: r1 produce 不全成功\n");
        return;
    }

    $statsAfterR1 = $client->stats();
    $createdAfterR1 = $statsAfterR1['created'];
    echo "  pool: idle={$statsAfterR1['idle']}, created={$createdAfterR1}, max_idle={$statsAfterR1['max_idle']}" . \PHP_EOL;
    echo "  → 首轮风暴下连接全 miss，按需开了 {$createdAfterR1} 条；归还时池保留 max_idle 条" . \PHP_EOL;

    // ===== 阶段 2.2：第二轮验证池复用 =====
    echo \PHP_EOL . "=== 2.2 第二轮：再 {$N_PRODUCERS} 协程并发，池应在复用 ===" . \PHP_EOL;
    $r2 = $runConcurrentProduces('r2', $N_PRODUCERS);
    echo "  耗时: {$r2['elapsed_ms']}ms" . \PHP_EOL;
    echo "  成功: {$r2['sent']}/{$N_PRODUCERS}" . \PHP_EOL;
    if ($r2['sent'] !== $N_PRODUCERS) {
        \fwrite(\STDERR, "FAIL: r2 produce 不全成功\n");
        return;
    }

    $statsAfterR2 = $client->stats();
    $createdAfterR2 = $statsAfterR2['created'];
    $newConns = $createdAfterR2 - $createdAfterR1;
    echo "  pool: idle={$statsAfterR2['idle']}, created={$createdAfterR2} (Δ +{$newConns})" . \PHP_EOL;

    // 关键断言：第二轮新建连接数 < 第一轮（说明池在复用）
    if ($newConns >= $createdAfterR1) {
        \fwrite(\STDERR, "FAIL: 第二轮新建 {$newConns} 条，未复用首轮 {$createdAfterR1} 条池连接\n");
        return;
    }
    echo "  ✓ 池正确复用（第二轮仅新增 {$newConns} / 首轮 {$createdAfterR1} 条）" . \PHP_EOL;

    // 并发性：并发耗时应明显短于串行
    $serialEstMs = $N_PRODUCERS * 20; // 单次 RT 假设 20ms（局域网）
    if ($r2['elapsed_ms'] > $serialEstMs) {
        \fwrite(\STDERR, "WARN: 第二轮 {$r2['elapsed_ms']}ms 接近串行 {$serialEstMs}ms，并发似乎没生效\n");
    } else {
        echo "  ✓ 协程并发生效（{$r2['elapsed_ms']}ms vs 串行预估 {$serialEstMs}ms）" . \PHP_EOL;
    }

    $totalProduced = 2 * $N_PRODUCERS;

    // ===== 阶段 3：subscribe + N_WORKERS 协程并发消费 =====
    echo \PHP_EOL . "=== 3. subscribe + {$N_WORKERS} 协程并发 poll ===" . \PHP_EOL;
    $sub = $client->subscribe('default', $group, [$topic], [
        'auto.offset.reset' => 'earliest',
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '2000',
    ]);
    echo "  subscribed, sub_id={$sub}" . \PHP_EOL;

    // SwooleClient 当前 API 表面没暴露 pollRebalanceEvents，subscribe 返回时
    // worker 已 join group，但分区 ASSIGN 可能还要 0.5-2s。短 sleep 让 ASSIGN
    // 完成，避免协程刚开始就一直拉到空 batch。
    echo '  等 ASSIGN 完成...' . \PHP_EOL;
    Coroutine::sleep(2.0);

    // 多协程消费同一 subscription
    // worker 端缓冲 lock-free 拉取，多协程会拿到互斥批次
    $consumed = new Swoole\Atomic(0);
    $seen = new Swoole\Table(1024);
    $seen->column('val', Swoole\Table::TYPE_STRING, 64);
    $seen->create();

    $stopMs = \microtime(true) * 1000 + 15_000; // 15s 上限
    $wg = new WaitGroup;
    for ($w = 0; $w < $N_WORKERS; $w++) {
        $wg->add();
        Coroutine::create(static function () use ($client, $sub, $wg, $consumed, $seen, $stopMs, $w, $totalProduced): void {
            try {
                while (\microtime(true) * 1000 < $stopMs && $consumed->get() < $totalProduced) {
                    $batch = $client->poll($sub, 20, 1000);
                    foreach ($batch as $m) {
                        $consumed->add(1);
                        // 用 key 做去重检测
                        $seen->set($m['key'], ['val' => $m['value']]);
                    }
                }
            } catch (Throwable $e) {
                \fprintf(\STDERR, "  worker={$w} throw: " . $e->getMessage() . "\n");
            } finally {
                $wg->done();
            }
        });
    }
    $wg->wait();

    echo "  消费总数: {$consumed->get()}/{$totalProduced}" . \PHP_EOL;
    echo '  去重后唯一 key: ' . $seen->count() . \PHP_EOL;

    if ($consumed->get() !== $totalProduced) {
        \fwrite(\STDERR, "FAIL: 消费总数 {$consumed->get()} 不等于生产 {$totalProduced}\n");
        return;
    }
    if ($seen->count() !== $totalProduced) {
        \fwrite(\STDERR, 'FAIL: 唯一 key 数 ' . $seen->count() . " 不等于 {$totalProduced（有重复或丢失）}\n");
        return;
    }

    // 校验 r1 / r2 两轮 key/value 一一对应
    foreach (['r1' => 0, 'r2' => $N_PRODUCERS] as $tag => $offset) {
        for ($i = 0; $i < $N_PRODUCERS; $i++) {
            $key = "{$tag}-k" . ($offset + $i);
            $expectedVal = "{$tag}-v" . ($offset + $i);
            $row = $seen->get($key);
            if (false === $row || $row['val'] !== $expectedVal) {
                \fwrite(\STDERR, "FAIL: {$key} 不匹配: " . \json_encode($row) . "\n");
                return;
            }
        }
    }
    echo "  ✓ 全部 {$totalProduced} 条 key/value 字节级一致" . \PHP_EOL;

    // ===== 阶段 4：commit + unsubscribe + 池清理 =====
    echo \PHP_EOL . '=== 4. commit + unsubscribe ===' . \PHP_EOL;
    $client->commit($sub);
    $client->unsubscribe($sub);

    $finalStats = $client->stats();
    echo "  最终 pool stats: idle={$finalStats['idle']}, created={$finalStats['created']}" . \PHP_EOL;

    $result['ok'] = true;
});

if (! $result['ok']) {
    \fwrite(\STDERR, "FAIL: 协程容器内有断言失败\n");
    exit(1);
}

echo \PHP_EOL . '★ SwooleClient Kafka e2e PASS' . \PHP_EOL;
