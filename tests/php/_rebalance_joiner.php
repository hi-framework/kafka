<?php

declare(strict_types=1);

/**
 * 给 rebalance.php 当伴跑：加入指定 group 持续 poll N 秒后退出，
 * 用于触发 rebalance。
 *
 * 用法：php _rebalance_joiner.php SOCKET TOPIC GROUP DURATION_SEC
 */
[$_, $socket, $topic, $group, $duration] = $argv;
$duration = (float) $duration;

$c = new Hi\Kafka\Client($socket);
$c->registerCluster('default', [
    'bootstrap.servers' => \getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094',
]);

$sub = $c->subscribe('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);

$deadline = \microtime(true) + $duration;
while (\microtime(true) < $deadline) {
    $c->poll($sub, 10, 500);
}
$c->unsubscribe($sub);
