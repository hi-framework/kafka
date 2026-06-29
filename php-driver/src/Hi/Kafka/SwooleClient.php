<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Socket;

/**
 * Swoole 协程感知的 Kafka 客户端。
 *
 * 与 `Hi\Kafka\Client`（C 扩展暴露、阻塞 IO）相对，本类：
 *
 * - 使用 `Swoole\Coroutine\Socket` 做 UDS 通信，所有 IO 走 Swoole reactor
 * - 用 `Channel` 实现协程感知的连接池，多协程并发不会互相阻塞
 * - 协议编解码复用扩展暴露的 `hi_kafka_*` 函数，**协议逻辑单源**
 *
 * 仅在 Swoole 协程上下文中使用。PHP-FPM / CLI / 非协程 Swoole 用 `Client`。
 *
 * 用法：
 *
 * ```php
 * use Swoole\Coroutine;
 * use Hi\Kafka\SwooleClient;
 *
 * Coroutine\run(function () {
 *     $client = new SwooleClient('/tmp/hi-kafka.sock');
 *     $client->produceFnf('default', 'topic', 'k', 'v');
 *     $r = $client->produceSync('default', 'topic', 'k', 'v', 5000);
 *     // $r => ['ok' => true, 'partition' => 0, 'offset' => 42]
 * });
 * ```
 */
final class SwooleClient
{
    private const SOCK_STREAM = 1;
    private const AF_UNIX = 1;

    private Channel $idleConns;
    private int $created = 0;
    private bool $workerEnsured = false;

    /**
     * @param string $socket     Worker UDS 路径
     * @param int    $maxIdle    池上限。idle 满了归还时直接 close
     * @param float  $connectTimeout
     */
    public function __construct(
        private readonly string $socket = '/tmp/hi-kafka.sock',
        private readonly int $maxIdle = 16,
        private readonly float $connectTimeout = 1.0,
    ) {
        $this->idleConns = new Channel($maxIdle);
        $this->assertExtension();
    }

    public function __destruct()
    {
        // 关闭所有 idle 连接
        while (! $this->idleConns->isEmpty()) {
            $conn = $this->idleConns->pop(0.001);
            if ($conn instanceof Socket) {
                $conn->close();
            }
        }
    }

    /**
     * Fire-and-forget 生产。立即返回，不等 ack。
     *
     * @param array<string,string>|null $headers Kafka 消息头（traceparent / source 等）
     * @param int|null $partition 明确写入分区；null = librdkafka partitioner（key hash）
     * @param int|null $timestampMs 消息时间戳（毫秒）；null = librdkafka 当前时间
     */
    public function produceFnf(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null,
    ): void {
        $frame = hi_kafka_encode_fnf_frame($cluster, $topic, $key, $value, $headers, $partition, $timestampMs);
        $conn = $this->acquire();
        try {
            $sent = $conn->sendAll($frame);
            if ($sent === false || $sent < strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write')
                );
            }
            $this->release($conn);
        } catch (\Throwable $e) {
            $conn->close();
            throw $e;
        }
    }

    /**
     * 同步生产，等 broker ack。
     *
     * 返回：
     *  - 成功：['ok' => true, 'cid' => int, 'partition' => int, 'offset' => int]
     *  - 失败：['ok' => false, 'cid' => int, 'code' => int, 'message' => string, 'retryable' => bool]
     *
     * @param int $timeoutMs 单次 IO 操作超时（不是总耗时）
     */
    /**
     * @param array<string,string>|null $headers Kafka 消息头
     * @param int|null $partition 明确写入分区；null = librdkafka partitioner
     * @param int|null $timestampMs 消息时间戳（毫秒）
     */
    public function produceSync(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        int $timeoutMs = 5000,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null,
    ): array {
        $encoded = hi_kafka_encode_req_frame($cluster, $topic, $key, $value, $headers, $partition, $timestampMs);
        $cid = $encoded['cid'];
        $frame = $encoded['frame'];
        $timeoutSec = $timeoutMs / 1000.0;

        $conn = $this->acquire();
        try {
            $sent = $conn->sendAll($frame, $timeoutSec);
            if ($sent === false || $sent < strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write')
                );
            }

            $headerLen = hi_kafka_header_len();
            $header = $conn->recvAll($headerLen, $timeoutSec);
            if ($header === false || strlen($header) < $headerLen) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll header failed: ' . ($conn->errMsg ?: 'short read')
                );
            }
            $parsed = hi_kafka_parse_header($header);
            if ($parsed['cid'] !== $cid) {
                $conn->close();
                throw new \RuntimeException(
                    "cid mismatch: sent $cid, got {$parsed['cid']}"
                );
            }

            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $conn->recvAll($payloadLen, $timeoutSec)
                : '';
            if ($payloadLen > 0 && (
                $payload === false || strlen($payload) < $payloadLen
            )) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll payload failed: ' . ($conn->errMsg ?: 'short read')
                );
            }

            $this->release($conn);
            return hi_kafka_decode_resp_frame($header . $payload);
        } catch (\Throwable $e) {
            $conn->close();
            throw $e;
        }
    }

    /**
     * 注册或覆盖一个 Kafka 集群。`$config` 须含 `bootstrap.servers`。
     *
     * @param array<string,string> $config
     */
    public function registerCluster(string $cluster, array $config, int $timeoutMs = 5000): void
    {
        $encoded = hi_kafka_encode_register_cluster_frame($cluster, $config);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("registerCluster failed: {$resp['message']}");
        }
    }

    /**
     * 订阅 topics，返回 subscription_id。
     *
     * @param string[] $topics
     * @param array<string,string>|null $config 例如 ['auto.offset.reset' => 'earliest']
     */
    public function subscribe(
        string $cluster,
        string $groupId,
        array $topics,
        ?array $config = null,
        int $timeoutMs = 5000,
    ): int {
        $encoded = hi_kafka_encode_subscribe_frame($cluster, $groupId, $topics, $config ?? []);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("subscribe failed: {$resp['message']}");
        }
        return $resp['subscription_id'];
    }

    /**
     * 拉一批消息。
     *
     * @return array<int, array{topic:string,partition:int,offset:int,timestamp_ms:int,key:string,value:string}>
     */
    public function poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array
    {
        $encoded = hi_kafka_encode_poll_frame($subscriptionId, $maxMessages, $timeoutMs);
        // IPC 超时 = poll 业务超时 + 2s 安全裕度
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs + 2000);
        if (! $resp['ok']) {
            throw new \RuntimeException("poll failed: {$resp['message']}");
        }
        return $resp['messages'];
    }

    /**
     * 同步提交 offset。
     */
    public function commit(int $subscriptionId, int $timeoutMs = 5000): void
    {
        $encoded = hi_kafka_encode_commit_frame($subscriptionId);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("commit failed: {$resp['message']}");
        }
    }

    /**
     * 退订（fire-and-forget，不等响应）。
     */
    public function unsubscribe(int $subscriptionId): void
    {
        $frame = hi_kafka_encode_unsubscribe_frame($subscriptionId);
        $conn = $this->acquire();
        try {
            $sent = $conn->sendAll($frame);
            if ($sent === false || $sent < strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write')
                );
            }
            $this->release($conn);
        } catch (\Throwable $e) {
            $conn->close();
            throw $e;
        }
    }

    // === Phase 3.x methods ====================================================

    /**
     * 暂停一组分区的 fetch（不丢分区分配，不触发 rebalance）。空数组 = 当前 assignment 全部。
     *
     * @param string[] $topics
     * @param int[]    $partitions
     */
    public function pause(int $subscriptionId, array $topics, array $partitions, int $timeoutMs = 5000): void
    {
        $encoded = hi_kafka_encode_pause_resume_frame($subscriptionId, 0, $topics, $partitions);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("pause failed: {$resp['message']}");
        }
    }

    /**
     * 恢复被 pause 暂停的分区。
     *
     * @param string[] $topics
     * @param int[]    $partitions
     */
    public function resume(int $subscriptionId, array $topics, array $partitions, int $timeoutMs = 5000): void
    {
        $encoded = hi_kafka_encode_pause_resume_frame($subscriptionId, 1, $topics, $partitions);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("resume failed: {$resp['message']}");
        }
    }

    /**
     * 按 offset seek。必须在订阅已 ASSIGN 后调。三个平行数组同长度。
     *
     * @param string[] $topics
     * @param int[]    $partitions
     * @param int[]    $offsets
     */
    public function seek(int $subscriptionId, array $topics, array $partitions, array $offsets, int $timeoutMs = 10000): void
    {
        $encoded = hi_kafka_encode_seek_by_offset_frame($subscriptionId, $topics, $partitions, $offsets);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("seek failed: {$resp['message']}");
        }
    }

    /**
     * 按时间戳 seek。$topics/$partitions 均空 = 应用到当前 assignment 全部分区。
     *
     * @param string[] $topics
     * @param int[]    $partitions
     */
    public function seekToTimestamp(
        int $subscriptionId,
        int $timestampMs,
        array $topics,
        array $partitions,
        int $timeoutMs = 15000,
    ): void {
        $encoded = hi_kafka_encode_seek_by_timestamp_frame(
            $subscriptionId, $timestampMs, $topics, $partitions
        );
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("seekToTimestamp failed: {$resp['message']}");
        }
    }

    /**
     * 开启事务。集群配置必须含 `transactional.id`。
     */
    public function beginTransaction(string $cluster, int $timeoutMs = 30000): void
    {
        $encoded = hi_kafka_encode_txn_frame($cluster, 0);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("beginTransaction failed: {$resp['message']}");
        }
    }

    public function commitTransaction(string $cluster, int $timeoutMs = 30000): void
    {
        $encoded = hi_kafka_encode_txn_frame($cluster, 1);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("commitTransaction failed: {$resp['message']}");
        }
    }

    public function abortTransaction(string $cluster, int $timeoutMs = 30000): void
    {
        $encoded = hi_kafka_encode_txn_frame($cluster, 2);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("abortTransaction failed: {$resp['message']}");
        }
    }

    /**
     * EOS：把 consumer offsets 提交进当前 producer 事务。
     * 调用顺序：beginTransaction → produceSync* → sendOffsetsToTransaction → commitTransaction。
     *
     * @param string[] $topics
     * @param int[]    $partitions
     * @param int[]    $offsets   每个是 last_consumed + 1
     */
    public function sendOffsetsToTransaction(
        string $producerCluster,
        int $subscriptionId,
        string $groupId,
        array $topics,
        array $partitions,
        array $offsets,
        int $timeoutMs = 30000,
    ): void {
        $encoded = hi_kafka_encode_send_offsets_frame(
            $producerCluster, $subscriptionId, $groupId, $topics, $partitions, $offsets
        );
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("sendOffsetsToTransaction failed: {$resp['message']}");
        }
    }

    /**
     * 推 SASL/OAUTHBEARER token 给指定 cluster。
     *
     * @param array<string,string> $extensions
     */
    public function setOAuthBearerToken(
        string $cluster,
        string $token,
        int $lifetimeMs,
        string $principalName,
        array $extensions = [],
        int $timeoutMs = 5000,
    ): void {
        $encoded = hi_kafka_encode_set_oauth_token_frame(
            $cluster, $token, $lifetimeMs, $principalName, $extensions
        );
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("setOAuthBearerToken failed: {$resp['message']}");
        }
    }

    /**
     * 拉取 rebalance 事件队列。每事件结构同 Hi\Kafka\Client::pollRebalanceEvents。
     *
     * @return list<array{type:string, partitions?:list<array{topic:string,partition:int}>, message?:string}>
     */
    public function pollRebalanceEvents(int $subscriptionId, int $maxEvents = 100, int $timeoutMs = 5000): array
    {
        $encoded = hi_kafka_encode_poll_rebalance_frame($subscriptionId, $maxEvents);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("pollRebalanceEvents failed: {$resp['message']}");
        }
        return $resp['events'] ?? [];
    }

    /**
     * 池统计。供监控/排障使用。
     */
    public function stats(): array
    {
        return [
            'socket' => $this->socket,
            'max_idle' => $this->maxIdle,
            'idle' => $this->idleConns->length(),
            'created' => $this->created,
        ];
    }

    /**
     * 通用的「发请求→读 13B header→按 cid 校验→读 payload→解析」。
     * 适用于所有 consumer req/resp 帧。
     *
     * @return array 结构同 hi_kafka_decode_consumer_resp 输出
     */
    private function roundTrip(int $cid, string $frame, int $timeoutMs): array
    {
        $timeoutSec = $timeoutMs / 1000.0;
        $conn = $this->acquire();
        try {
            $sent = $conn->sendAll($frame, $timeoutSec);
            if ($sent === false || $sent < strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write')
                );
            }

            $headerLen = hi_kafka_header_len();
            $header = $conn->recvAll($headerLen, $timeoutSec);
            if ($header === false || strlen($header) < $headerLen) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll header failed: ' . ($conn->errMsg ?: 'short read')
                );
            }
            $parsed = hi_kafka_parse_header($header);
            if ($parsed['cid'] !== $cid) {
                $conn->close();
                throw new \RuntimeException(
                    "cid mismatch: sent $cid, got {$parsed['cid']}"
                );
            }

            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $conn->recvAll($payloadLen, $timeoutSec)
                : '';
            if ($payloadLen > 0 && (
                $payload === false || strlen($payload) < $payloadLen
            )) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll payload failed: ' . ($conn->errMsg ?: 'short read')
                );
            }

            $this->release($conn);
            return hi_kafka_decode_consumer_resp($header . $payload);
        } catch (\Throwable $e) {
            $conn->close();
            throw $e;
        }
    }

    private function acquire(): Socket
    {
        // 池里有空闲就拿，没有就建新
        if (! $this->idleConns->isEmpty()) {
            $conn = $this->idleConns->pop(0.001);
            if ($conn instanceof Socket) {
                return $conn;
            }
        }
        return $this->newConn();
    }

    private function release(Socket $conn): void
    {
        if ($this->idleConns->isFull()) {
            $conn->close();
            return;
        }
        $this->idleConns->push($conn, 0.001);
    }

    private function newConn(): Socket
    {
        // 首次连接前确保 worker 已 fork 起来。
        // 走扩展的 hi_kafka_ensure_worker，重用其 flock + double-fork 互斥逻辑。
        if (! $this->workerEnsured) {
            hi_kafka_ensure_worker($this->socket);
            $this->workerEnsured = true;
        }

        $conn = new Socket(self::AF_UNIX, self::SOCK_STREAM, 0);
        if (! $conn->connect($this->socket, 0, $this->connectTimeout)) {
            throw new \RuntimeException(
                "connect {$this->socket} failed: " . $conn->errMsg
            );
        }
        // F: 协议 HELLO 握手——双端 PROTOCOL_MAJOR 不一致 worker 会关连接，
        // 这里同步发 HELLO + 读 14B RESP + 校验
        try {
            $this->handshake($conn);
        } catch (\Throwable $e) {
            $conn->close();
            throw new \RuntimeException(
                "handshake {$this->socket} failed: " . $e->getMessage(), 0, $e
            );
        }
        $this->created++;
        return $conn;
    }

    private function handshake(Socket $conn): void
    {
        $frame = hi_kafka_encode_hello_frame();
        $timeoutSec = 2.0;
        $sent = $conn->sendAll($frame, $timeoutSec);
        if ($sent === false || $sent < strlen($frame)) {
            throw new \RuntimeException(
                'send HELLO failed: ' . ($conn->errMsg ?: 'short write')
            );
        }
        // HELLO RESP 固定 14B（13B header + 1B major），单次 recvAll
        $resp = $conn->recvAll(14, $timeoutSec);
        if ($resp === false || strlen($resp) < 14) {
            throw new \RuntimeException(
                'recv HELLO RESP failed: ' . ($conn->errMsg ?: 'short read')
            );
        }
        hi_kafka_verify_hello_resp($resp);
    }

    /**
     * 显式触发 worker fork（如果还没起）。一般不需要——首次 produce/subscribe 会自动触发。
     */
    public function ensureWorker(): void
    {
        if (! $this->workerEnsured) {
            hi_kafka_ensure_worker($this->socket);
            $this->workerEnsured = true;
        }
    }

    private function assertExtension(): void
    {
        if (! function_exists('hi_kafka_encode_fnf_frame')
            || ! function_exists('hi_kafka_encode_subscribe_frame')
            || ! function_exists('hi_kafka_decode_consumer_resp')
        ) {
            throw new \RuntimeException(
                'hi_kafka extension with producer+consumer protocol helpers is required'
            );
        }
        if (! extension_loaded('swoole')) {
            throw new \RuntimeException('swoole extension is required for SwooleClient');
        }
    }
}
