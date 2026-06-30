<?php
/**
 * PHP stub for `hi-kafka` extension —— **don't `require` at runtime**.
 *
 * 这是给 IDE / 静态分析器（PHPStorm / VSCode / PHPStan / Psalm）读的类型声明，
 * 运行时类与函数由 .so 提供。这里全部是空函数体，require 进程序会和真扩展撞
 * "Function/class already declared" fatal。
 *
 * 接入方式（任选）：
 *
 * - **PHPStorm**：项目 Settings → PHP → Include Path 加上 `stubs/` 目录。
 *   或者更推荐：右键 `stubs/hi_kafka.php` → Mark As → PHP Reference File。
 *
 * - **VSCode (Intelephense)**：`settings.json` 加 `"intelephense.environment.includePaths": ["stubs"]`。
 *
 * - **PHPStan**：`phpstan.neon` 里 `scanFiles: [stubs/hi_kafka.php]`。
 *
 * - **Psalm**：`psalm.xml` 里 `<stubs><file name="stubs/hi_kafka.php"/></stubs>`。
 *
 * - **composer**：发布成独立包后业务 `require --dev hi/kafka-stubs`，stubs 包的
 *   composer.json 用 `extra.phpstan.stubFiles` 让静态分析自动发现。
 *
 * @see docs/USAGE.md
 */

namespace Hi\Kafka {

    /**
     * Kafka 客户端（对象式 API）。
     *
     * 每个实例代表一个 worker socket 连接点；同一进程多 Client 实例共用底层
     * worker（基于 socket 路径区分）。线程不安全——多线程 / 多 fiber 各自持有。
     */
    final class Client
    {
        /**
         * @param string|null $socket UDS 路径；null = `/tmp/hi-kafka.sock` 或 `HI_KAFKA_SOCKET` env
         */
        public function __construct(?string $socket = null) {}

        /** 当前 client 的 socket 路径 */
        public function socket(): string {}

        /**
         * 注册或覆盖 Kafka 集群配置（连接 / SASL / SSL 等）。
         *
         * @param string                 $cluster   逻辑集群名，业务侧 `produce*` / `subscribe` 用它引用
         * @param array<string,string>   $config    librdkafka 配置，必须含 `bootstrap.servers`
         * @param int|null               $timeoutMs IPC 超时；默认 5000
         *
         * @throws \Exception 集群名无 `bootstrap.servers` / IPC 失败
         */
        public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void {}

        /**
         * 显式拉起 worker 进程（命中缓存时零开销直接返回）。
         * 业务里几乎不用主动调，第一次 produce / subscribe 会自动触发。
         */
        public function ensureWorker(): void {}

        /**
         * Fire-and-forget 生产。**不等 broker ack**，吞吐量最高，无投递成功保证。
         *
         * @param string                  $cluster
         * @param string                  $topic
         * @param string                  $key
         * @param string                  $value
         * @param array<string,string>    $headers     Kafka 消息头（关联数组，UTF-8）
         * @param int|null                $partition   null = 由 librdkafka partitioner（key hash）决定
         * @param int|null                $timestampMs null = 当前时间戳
         */
        public function produceFnf(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            array $headers = [],
            ?int $partition = null,
            ?int $timestampMs = null
        ): void {}

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
         * } 成功时 ok=true + partition + offset；失败时 ok=false + code/message/retryable
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
        ): array {}

        /**
         * Binary-safe F&F：key / value / header value 接受**任意字节**（NUL / 0xFF / 非 UTF-8）。
         * 用于 protobuf / msgpack / 加密 payload 等场景。
         *
         * @param string   $key           PHP binary string
         * @param string   $value         PHP binary string
         * @param string[] $headerNames   header 名 UTF-8（Kafka 协议要求）
         * @param string[] $headerValues  平行数组，每个元素是 binary string
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
        ): void {}

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
        ): array {}

        /**
         * 订阅 topics 到 group。
         *
         * @param string                    $cluster
         * @param string                    $groupId        Kafka consumer group
         * @param string[]                  $topics
         * @param array<string,string>|null $config         librdkafka consumer 配置（auto.offset.reset / session.timeout.ms / isolation.level 等）
         * @param int|null                  $timeoutMs
         *
         * @return int **virtual** subscription_id；worker 崩溃后自愈重订阅这个 id 不变
         */
        public function subscribe(
            string $cluster,
            string $groupId,
            array $topics,
            ?array $config = null,
            ?int $timeoutMs = null
        ): int {}

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
        public function poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array {}

        /** 同步提交当前持有的 offsets */
        public function commit(int $subscriptionId, ?int $timeoutMs = null): void {}

        /** 退订（幂等）。worker 端 close consumer 走 spawn_blocking 不阻塞 tokio */
        public function unsubscribe(int $subscriptionId): void {}

        /**
         * 拉取 rebalance 事件队列。
         *
         * @return list<array{type: string, partitions?: list<array{topic: string, partition: int}>, message?: string}>
         *         事件结构：
         *         - `['type' => 'assign', 'partitions' => [...]]`
         *         - `['type' => 'revoke', 'partitions' => [...]]`
         *         - `['type' => 'error', 'message' => string]`
         */
        public function pollRebalanceEvents(
            int $subscriptionId,
            ?int $maxEvents = null,
            ?int $timeoutMs = null
        ): array {}

        /**
         * 按 offset seek。三个**平行数组**，长度必须一致：
         *  - `$topics[i]` / `$partitions[i]` / `$offsets[i]` 描述第 i 个 seek 目标
         *
         * 必须在订阅已建立且 partition 已被分配（即拿到 ASSIGN 事件）后调。
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
        ): void {}

        /**
         * 按时间戳 seek（librdkafka `offsets_for_times` 解析后 seek 到对应 offset）。
         * `$topics` 与 `$partitions` 均空 → 应用到当前 assignment 全部分区。
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
        ): void {}

        /**
         * 暂停一组 (topic, partition) 的 fetch；**不丢分区分配 / 不触发 rebalance**。
         * 已经预取的消息仍会消费完。空数组 = 应用到当前 assignment 全部。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function pause(
            int $subscriptionId,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null
        ): void {}

        /**
         * 恢复被 {@see pause()} 暂停的分区，从上次 fetch 位置继续。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function resume(
            int $subscriptionId,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null
        ): void {}

        /**
         * 开启事务。集群配置必须含 `transactional.id`，每个 PHP 实例 / 节点唯一。
         *
         * @throws \Exception cluster 没启用事务 / 上一个事务未完成 / broker fencing
         */
        public function beginTransaction(string $cluster, ?int $timeoutMs = null): void {}

        /** 原子提交事务里所有 in-flight 消息 */
        public function commitTransaction(string $cluster, ?int $timeoutMs = null): void {}

        /** 回滚事务，read_committed consumer 看不到这些消息 */
        public function abortTransaction(string $cluster, ?int $timeoutMs = null): void {}

        /**
         * **Exactly-Once Stream（KIP-447）**：把 consumer offsets 提交进当前事务，
         * 与 producer 写出的消息原子可见。崩溃恢复时 EOS 语义。
         *
         * 调用顺序：
         * 1. `beginTransaction($producerCluster)`
         * 2. `produceSync(...)` 写派生消息
         * 3. `sendOffsetsToTransaction(...)` — `$offsets[i]` 是 last_consumed + 1
         * 4. `commitTransaction($producerCluster)`
         *
         * @param string   $producerCluster 事务 producer 所在 cluster
         * @param int      $subscriptionId  consumer 的 virtual sub_id
         * @param string   $groupId         consumer 的 group.id（仅日志）
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets         next offset = last_consumed + 1
         */
        public function sendOffsetsToTransaction(
            string $producerCluster,
            int $subscriptionId,
            string $groupId,
            array $topics,
            array $partitions,
            array $offsets,
            ?int $timeoutMs = null
        ): void {}

        /**
         * 推送 SASL/OAUTHBEARER token 给指定集群。
         * librdkafka 触发 token refresh 时 worker 用最后一次推送的 token。
         *
         * 业务侧典型刷新策略：定时器每 `lifetime_ms - now - 5min` 刷一次；或监听 token rotate 事件。
         *
         * @param string                $cluster
         * @param string                $token          JWT / opaque token
         * @param int                   $lifetimeMs     token 失效时间（unix epoch ms）
         * @param string                $principalName  Kafka principal
         * @param array<string,string>  $extensions     SASL extensions（预留，rdkafka 0.38 还没透传）
         */
        public function setOAuthBearerToken(
            string $cluster,
            string $token,
            int $lifetimeMs,
            string $principalName,
            array $extensions = [],
            ?int $timeoutMs = null
        ): void {}
    }

    /**
     * Swoole 协程感知客户端。**仅在 Swoole 协程上下文使用**。
     * 与 {@see Client} 同样的 API，但 IO 走 `Swoole\Coroutine\Socket`，调度器自动 yield。
     *
     * @see Client 阻塞版（PHP-FPM / CLI）
     */
    final class SwooleClient
    {
        public function __construct(string $socket = '/tmp/hi-kafka.sock', int $maxIdle = 16, float $connectTimeout = 1.0) {}

        public function registerCluster(string $cluster, array $config, int $timeoutMs = 5000): void {}

        public function produceFnf(
            string $cluster, string $topic, string $key, string $value,
            ?array $headers = null, ?int $partition = null, ?int $timestampMs = null
        ): void {}

        /** @return array{ok: bool, cid: int, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool} */
        public function produceSync(
            string $cluster, string $topic, string $key, string $value,
            int $timeoutMs = 5000, ?array $headers = null,
            ?int $partition = null, ?int $timestampMs = null
        ): array {}

        public function subscribe(string $cluster, string $groupId, array $topics, ?array $config = null, int $timeoutMs = 5000): int {}
        public function poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array {}
        public function commit(int $subscriptionId, int $timeoutMs = 5000): void {}
        public function unsubscribe(int $subscriptionId): void {}
        public function ensureWorker(): void {}
        /** @return array{socket: string, max_idle: int, idle: int, created: int} */
        public function stats(): array {}
        // Phase 3.x（与 Client 对齐）
        public function pause(int $subscriptionId, array $topics, array $partitions, int $timeoutMs = 5000): void {}
        public function resume(int $subscriptionId, array $topics, array $partitions, int $timeoutMs = 5000): void {}
        public function seek(int $subscriptionId, array $topics, array $partitions, array $offsets, int $timeoutMs = 10000): void {}
        public function seekToTimestamp(int $subscriptionId, int $timestampMs, array $topics, array $partitions, int $timeoutMs = 15000): void {}
        public function beginTransaction(string $cluster, int $timeoutMs = 30000): void {}
        public function commitTransaction(string $cluster, int $timeoutMs = 30000): void {}
        public function abortTransaction(string $cluster, int $timeoutMs = 30000): void {}
        public function sendOffsetsToTransaction(string $producerCluster, int $subscriptionId, string $groupId, array $topics, array $partitions, array $offsets, int $timeoutMs = 30000): void {}
        public function setOAuthBearerToken(string $cluster, string $token, int $lifetimeMs, string $principalName, array $extensions = [], int $timeoutMs = 5000): void {}
        public function pollRebalanceEvents(int $subscriptionId, int $maxEvents = 100, int $timeoutMs = 5000): array {}
    }

    /**
     * Swow 协程感知客户端。**仅在 Swow 协程上下文使用**。
     * 与 {@see Client} 同样的 API，IO 走 `Swow\Socket`。
     */
    final class SwowClient
    {
        public function __construct(string $socket = '/tmp/hi-kafka.sock', int $maxIdle = 16, int $connectTimeoutMs = 1000) {}

        public function registerCluster(string $cluster, array $config, int $timeoutMs = 5000): void {}

        public function produceFnf(
            string $cluster, string $topic, string $key, string $value,
            ?array $headers = null, ?int $partition = null, ?int $timestampMs = null
        ): void {}

        /** @return array{ok: bool, cid: int, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool} */
        public function produceSync(
            string $cluster, string $topic, string $key, string $value,
            int $timeoutMs = 5000, ?array $headers = null,
            ?int $partition = null, ?int $timestampMs = null
        ): array {}

        public function subscribe(string $cluster, string $groupId, array $topics, ?array $config = null, int $timeoutMs = 5000): int {}
        public function poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array {}
        public function commit(int $subscriptionId, int $timeoutMs = 5000): void {}
        public function unsubscribe(int $subscriptionId): void {}
        public function ensureWorker(): void {}
        /** @return array{socket: string, max_idle: int, idle: int, created: int} */
        public function stats(): array {}
        // Phase 3.x（与 Client 对齐）
        public function pause(int $subscriptionId, array $topics, array $partitions, int $timeoutMs = 5000): void {}
        public function resume(int $subscriptionId, array $topics, array $partitions, int $timeoutMs = 5000): void {}
        public function seek(int $subscriptionId, array $topics, array $partitions, array $offsets, int $timeoutMs = 10000): void {}
        public function seekToTimestamp(int $subscriptionId, int $timestampMs, array $topics, array $partitions, int $timeoutMs = 15000): void {}
        public function beginTransaction(string $cluster, int $timeoutMs = 30000): void {}
        public function commitTransaction(string $cluster, int $timeoutMs = 30000): void {}
        public function abortTransaction(string $cluster, int $timeoutMs = 30000): void {}
        public function sendOffsetsToTransaction(string $producerCluster, int $subscriptionId, string $groupId, array $topics, array $partitions, array $offsets, int $timeoutMs = 30000): void {}
        public function setOAuthBearerToken(string $cluster, string $token, int $lifetimeMs, string $principalName, array $extensions = [], int $timeoutMs = 5000): void {}
        public function pollRebalanceEvents(int $subscriptionId, int $maxEvents = 100, int $timeoutMs = 5000): array {}
    }
}

namespace {
    /**
     * 扩展版本号
     */
    function hi_kafka_version(): string {}

    /**
     * 显式启动 worker（如果还没在跑）。命中缓存时零开销。
     */
    function hi_kafka_ensure_worker(?string $socket = null): void {}

    /**
     * 注册或覆盖 Kafka 集群配置。全局函数版（不需要 Client 实例）。
     *
     * @param array<string,string> $config
     */
    function hi_kafka_register_cluster(
        string $cluster,
        array $config,
        ?string $socket = null,
        ?int $timeoutMs = null
    ): void {}

    /**
     * Fire-and-forget 生产。全局函数版。
     *
     * @param array<string,string>|null $headers
     */
    function hi_kafka_produce_fnf(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null,
        ?string $socket = null
    ): void {}

    /**
     * 同步生产。
     *
     * @param array<string,string>|null $headers
     *
     * @return array{ok: bool, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool}
     */
    function hi_kafka_produce_sync(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null,
        ?int $timeoutMs = null,
        ?string $socket = null
    ): array {}

    /**
     * 订阅。返回 virtual subscription_id。
     *
     * @param string[]                  $topics
     * @param array<string,string>|null $config
     */
    function hi_kafka_subscribe(
        string $cluster,
        string $groupId,
        array $topics,
        ?array $config = null,
        ?string $socket = null,
        ?int $timeoutMs = null
    ): int {}

    /**
     * 拉一批消息。
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
    function hi_kafka_poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array {}

    function hi_kafka_commit(int $subscriptionId, ?int $timeoutMs = null): void {}
    function hi_kafka_unsubscribe(int $subscriptionId): void {}

    /** @internal 协程 driver 登记订阅，供进程退出时主动 unsubscribe + Goodbye */
    function hi_kafka_track_subscription(int $subscriptionId, ?string $socket = null): void {}
    /** @internal 与 hi_kafka_track_subscription 配对，driver 主动 unsubscribe 后注销 */
    function hi_kafka_untrack_subscription(int $subscriptionId, ?string $socket = null): void {}

    /**
     * 扩展端连接池统计（按 socket 路径分组）。
     *
     * @return array<string, array{
     *     max_idle: int,
     *     idle: int,
     *     acquires: int,
     *     hits: int,
     *     misses: int,
     *     closed: int,
     *     poisoned: int,
     * }>
     */
    function hi_kafka_pool_stats(): array {}

    /**
     * IPC 自动重试统计（worker 崩溃后的恢复次数）。
     *
     * @return array{attempts: int, successes: int, failures: int}
     */
    function hi_kafka_retry_stats(): array {}

    /**
     * Consumer 自动重订阅统计（worker 崩溃后的 virtual_id 自愈）。
     *
     * @return array{attempts: int, successes: int, failures: int}
     */
    function hi_kafka_resubscribe_stats(): array {}

    /**
     * 当前 PHP 进程加载了哪些协程运行时（检测 `swoole_version` / `swow\version` 等）。
     *
     * @return list<string> 例如 `["blocking"]` 或 `["blocking", "swoole"]`
     */
    function hi_kafka_runtime(): array {}

    // ========================================================================
    // 低级协议编解码原语，给 PHP 协程 driver（SwooleClient / SwowClient）用。
    // 业务代码无需调用。
    // ========================================================================

    /**
     * 全进程单调自增 correlation id
     * @internal
     */
    function hi_kafka_next_cid(): int {}

    /**
     * 协议帧头长度（常量 13）
     * @internal
     */
    function hi_kafka_header_len(): int {}

    /**
     * 编一帧 HELLO 协议握手，返回完整 14B 帧字节
     * @internal
     */
    function hi_kafka_encode_hello_frame(): string {}

    /**
     * 校验 HELLO RESP 完整帧（14B），版本不匹配或格式错误抛异常
     * @internal
     */
    function hi_kafka_verify_hello_resp(string $bytes): void {}

    /**
     * 编一帧 PRODUCE_FNF（fire-and-forget），返回完整帧字节
     * @param array<string,string>|null $headers
     * @internal
     */
    function hi_kafka_encode_fnf_frame(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null
    ): string {}

    /**
     * 编一帧 PRODUCE_REQ
     * @param array<string,string>|null $headers
     * @return array{cid: int, frame: string}
     * @internal
     */
    function hi_kafka_encode_req_frame(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null
    ): array {}

    /**
     * 解析 13B 帧头
     * @return array{kind: int, cid: int, payload_len: int}
     * @internal
     */
    function hi_kafka_parse_header(string $bytes): array {}

    /**
     * 解析 PRODUCE_RESP 完整帧
     * @return array{cid: int, ok: bool, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool}
     * @internal
     */
    function hi_kafka_decode_resp_frame(string $bytes): array {}

    /**
     * 编一帧 SUBSCRIBE_REQ
     * @param string[] $topics
     * @param array<string,string>|null $config
     * @return array{cid: int, frame: string}
     * @internal
     */
    function hi_kafka_encode_subscribe_frame(
        string $cluster,
        string $groupId,
        array $topics,
        ?array $config = null
    ): array {}

    /**
     * 编一帧 POLL_REQ
     * @return array{cid: int, frame: string}
     * @internal
     */
    function hi_kafka_encode_poll_frame(int $subscriptionId, int $maxMessages, int $timeoutMs): array {}

    /**
     * 编一帧 COMMIT_REQ
     * @return array{cid: int, frame: string}
     * @internal
     */
    function hi_kafka_encode_commit_frame(int $subscriptionId): array {}

    /**
     * 编一帧 UNSUBSCRIBE
     * @internal
     */
    function hi_kafka_encode_unsubscribe_frame(int $subscriptionId): string {}

    /**
     * 编一帧 REGISTER_CLUSTER_REQ
     * @param array<string,string> $config
     * @return array{cid: int, frame: string}
     * @internal
     */
    function hi_kafka_encode_register_cluster_frame(string $cluster, array $config): array {}

    /**
     * 解析 consumer 响应帧（按 kind 分发）
     *
     * @return array{
     *     kind: string,
     *     cid: int,
     *     ok: bool,
     *     subscription_id?: int,
     *     messages?: array,
     *     events?: array,
     *     message?: string,
     * }
     * @internal
     */
    function hi_kafka_decode_consumer_resp(string $bytes): array {}

    // ========================================================================
    // Phase 3.x REQ encoders（给 SwooleClient/SwowClient driver 用）
    // ========================================================================

    /**
     * 编一帧 PAUSE_RESUME_REQ。$op 0=Pause / 1=Resume；空数组 = 当前 assignment 全部。
     * @param string[] $topics
     * @param int[]    $partitions
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_pause_resume_frame(int $subscriptionId, int $op, array $topics, array $partitions): array {}

    /**
     * 编一帧 SEEK_REQ（按 offset 模式）
     * @param string[] $topics
     * @param int[]    $partitions
     * @param int[]    $offsets
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_seek_by_offset_frame(int $subscriptionId, array $topics, array $partitions, array $offsets): array {}

    /**
     * 编一帧 SEEK_REQ（按 timestamp 模式）
     * @param string[] $topics
     * @param int[]    $partitions
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_seek_by_timestamp_frame(int $subscriptionId, int $timestampMs, array $topics, array $partitions): array {}

    /**
     * 编一帧 TXN_REQ。$op 0=Begin / 1=Commit / 2=Abort
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_txn_frame(string $cluster, int $op): array {}

    /**
     * 编一帧 SEND_OFFSETS_REQ（EOS stream）
     * @param string[] $topics
     * @param int[]    $partitions
     * @param int[]    $offsets
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_send_offsets_frame(string $producerCluster, int $subscriptionId, string $groupId, array $topics, array $partitions, array $offsets): array {}

    /**
     * 编一帧 SET_OAUTH_BEARER_TOKEN_REQ
     * @param array<string,string>|null $extensions
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_set_oauth_token_frame(string $cluster, string $token, int $lifetimeMs, string $principalName, ?array $extensions = null): array {}

    /**
     * 编一帧 POLL_REBALANCE_REQ
     * @return array{cid:int, frame:string}
     * @internal
     */
    function hi_kafka_encode_poll_rebalance_frame(int $subscriptionId, int $maxEvents): array {}
}
