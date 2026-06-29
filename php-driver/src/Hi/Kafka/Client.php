<?php

declare(strict_types=1);

namespace Hi\Kafka;

/*
 * `Hi\Kafka\Client` 由 hi-kafka C 扩展在 MINIT 注册。本文件是**仅在扩展缺失时**
 * 才会被 PSR-4 autoload 触发的同名桩——给 IDE / 静态分析 / 无扩展 CI 兜底，
 * 让上层代码 `new Hi\Kafka\Client(...)` 至少在词法上能解析。
 *
 * `class_exists(..., false)` 关掉 autoload 探测自身，避免回环：
 *
 * - 扩展存在 → MINIT 阶段类已注册 → autoload 根本不触发 → 文件不加载
 * - 扩展存在 + 误 require → guard 命中，跳过桩定义 → **不撞 "already declared"**
 * - 扩展缺失 → autoload 触发 → guard 不命中 → 落桩
 */

if (! \class_exists(Client::class, false)) {
    /**
     * Kafka 客户端（对象式 API）。
     *
     * 每个实例代表一个 worker socket 连接点；同一进程多 Client 实例共用底层
     * worker（基于 socket 路径区分）。线程不安全——多线程 / 多 fiber 各自持有。
     *
     * 仅当 hi-kafka 扩展未加载时本桩生效；调用任何方法都是 no-op。
     * 真实运行时实现在 ext 端，签名以本文件为准（IDE / 静态分析 / docblock）。
     */
    final class Client
    {
        /**
         * @param string|null $socket UDS 路径；null = `/tmp/hi-kafka.sock` 或 `HI_KAFKA_SOCKET` env
         */
        public function __construct(?string $socket = null)
        {
        }

        /** 当前 client 的 socket 路径 */
        public function socket(): string
        {
            return '';
        }

        /**
         * 注册或覆盖 Kafka 集群配置（连接 / SASL / SSL 等）。
         *
         * @param array<string,string> $config librdkafka 配置，必须含 `bootstrap.servers`
         */
        public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void
        {
        }

        /**
         * 显式拉起 worker 进程（命中缓存时零开销直接返回）。
         * 业务里几乎不用主动调，第一次 produce / subscribe 会自动触发。
         */
        public function ensureWorker(): void
        {
        }

        /**
         * Fire-and-forget 生产。**不等 broker ack**，吞吐量最高，无投递成功保证。
         *
         * @param array<string,string> $headers Kafka 消息头（关联数组，UTF-8）
         * @param int|null             $partition   null = 由 librdkafka partitioner（key hash）决定
         * @param int|null             $timestampMs null = 当前时间戳
         */
        public function produceFnf(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            array $headers = [],
            ?int $partition = null,
            ?int $timestampMs = null
        ): void {
        }

        /**
         * 同步生产，等 broker delivery report。
         *
         * @param array<string,string> $headers
         *
         * @return array{
         *     ok: bool,
         *     partition?: int,
         *     offset?: int,
         *     code?: int,
         *     message?: string,
         *     retryable?: bool,
         * }
         */
        public function produceSync(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            array $headers = [],
            ?int $partition = null,
            ?int $timestampMs = null,
            ?int $timeoutMs = null
        ): array {
            return ['ok' => true];
        }

        /**
         * Binary-safe F&F：key / value / header value 接受**任意字节**（NUL / 0xFF / 非 UTF-8）。
         *
         * @param string[] $headerNames  header 名 UTF-8（Kafka 协议要求）
         * @param string[] $headerValues 平行数组，每个元素是 binary string
         */
        public function produceFnfBin(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            array $headerNames,
            array $headerValues,
            ?int $partition = null,
            ?int $timestampMs = null
        ): void {
        }

        /**
         * Binary-safe 同步生产。返回结构同 {@see produceSync()}。
         *
         * @param string[] $headerNames
         * @param string[] $headerValues
         *
         * @return array{ok: bool, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool}
         */
        public function produceSyncBin(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            array $headerNames,
            array $headerValues,
            ?int $partition = null,
            ?int $timestampMs = null,
            ?int $timeoutMs = null
        ): array {
            return ['ok' => true];
        }

        /**
         * 订阅 topics 到 group。
         *
         * @param string[]                  $topics
         * @param array<string,string>|null $config librdkafka consumer 配置
         *
         * @return int virtual subscription_id；worker 崩溃自愈后 id 不变
         */
        public function subscribe(
            string $cluster,
            string $groupId,
            array $topics,
            ?array $config = null,
            ?int $timeoutMs = null
        ): int {
            return 0;
        }

        /**
         * 拉一批消息。`timeoutMs=0` 非阻塞快照；否则 long-poll。
         *
         * @return list<array{
         *     topic: string,
         *     partition: int,
         *     offset: int,
         *     timestamp_ms: int,
         *     key: string,
         *     value: string,
         *     headers: array<string,string>,
         * }>
         */
        public function poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array
        {
            return [];
        }

        /** 同步提交当前持有的 offsets */
        public function commit(int $subscriptionId, ?int $timeoutMs = null): void
        {
        }

        /** 退订（幂等）。worker 端 close consumer 走 spawn_blocking */
        public function unsubscribe(int $subscriptionId): void
        {
        }

        /**
         * 拉取 rebalance 事件队列。
         *
         * @return list<array{type: string, partitions?: list<array{topic: string, partition: int}>, message?: string}>
         */
        public function pollRebalanceEvents(
            int $subscriptionId,
            ?int $maxEvents = null,
            ?int $timeoutMs = null
        ): array {
            return [];
        }

        /**
         * 按 offset seek。三个平行数组，长度一致。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets
         */
        public function seek(
            int $subscriptionId,
            array $topics,
            array $partitions,
            array $offsets,
            ?int $timeoutMs = null
        ): void {
        }

        /**
         * 按时间戳 seek（librdkafka `offsets_for_times`）。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function seekToTimestamp(
            int $subscriptionId,
            int $timestampMs,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null
        ): void {
        }

        /**
         * 暂停一组 (topic, partition) 的 fetch；**不丢分区分配**。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function pause(
            int $subscriptionId,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null
        ): void {
        }

        /**
         * 恢复被 {@see pause()} 暂停的分区。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function resume(
            int $subscriptionId,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null
        ): void {
        }

        /**
         * 开启事务。cluster 配置必须含 `transactional.id`。
         */
        public function beginTransaction(string $cluster, ?int $timeoutMs = null): void
        {
        }

        /** 原子提交事务里所有 in-flight 消息 */
        public function commitTransaction(string $cluster, ?int $timeoutMs = null): void
        {
        }

        /** 回滚事务，read_committed consumer 看不到这些消息 */
        public function abortTransaction(string $cluster, ?int $timeoutMs = null): void
        {
        }

        /**
         * **EOS Stream（KIP-447）**：把 consumer offsets 提交进当前事务，
         * 与 producer 写出的消息原子可见。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets next offset = last_consumed + 1
         */
        public function sendOffsetsToTransaction(
            string $producerCluster,
            int $subscriptionId,
            string $groupId,
            array $topics,
            array $partitions,
            array $offsets,
            ?int $timeoutMs = null
        ): void {
        }

        /**
         * 推送 SASL/OAUTHBEARER token 给指定集群。
         *
         * @param array<string,string> $extensions
         */
        public function setOAuthBearerToken(
            string $cluster,
            string $token,
            int $lifetimeMs,
            string $principalName,
            array $extensions = [],
            ?int $timeoutMs = null
        ): void {
        }
    }
}
