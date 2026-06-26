# hi-kafka-ext

Hi Framework Kafka 嵌入式 worker 扩展（Rust）。

设计文档：[src/Kafka/RFC-EMBEDDED-WORKER.md](../src/Kafka/RFC-EMBEDDED-WORKER.md)

## 仓库结构

```
hi-kafka-ext/
├── proto/             # IPC 协议（帧编解码、消息类型）
├── ext/               # PHP 扩展 cdylib（ext-php-rs 实现 + 内嵌 worker 入口）
├── worker/            # worker 核心库（被 ext 静态链接；保留 standalone binary 作 dev 工具）
├── php-driver/        # PSR-4：Hi\Kafka\SwooleClient（协程 driver）
├── tests/php/         # e2e PHP 测试脚本
├── scripts/           # build / smoke / kafka 测试
├── Dockerfile         # 跨架构 Linux 构建
└── docker-compose.kafka.yml   # 本地 KRaft 单节点 Kafka
```

## 独立性声明

本项目独立实现，**不依赖、不复用、不集成**任何第三方 PHP 扩展（含 SkyWalking PHP、php-rdkafka 等）的代码或运行时。

## 部署形态

**单 `.so` 分发**：worker 代码已链接进扩展，**部署只需 1 个文件**。

| 产物 | 位置 | 用途 |
| --- | --- | --- |
| `libhi_kafka.so` (Linux) / `.dylib` (macOS) | PHP extension dir | PHP 扩展 + 内嵌 worker（release ~2.7MB on macOS / ~3.9MB on Linux） |
| `Hi\Kafka\SwooleClient.php` | 项目 PSR-4 路径 | Swoole 协程业务可选 |
| `Hi\Kafka\{Connection,Producer,Consumer}Config.php` | 框架 `src/Kafka/` | 强类型配置 → librdkafka 参数翻译 |

**工作原理**：扩展首次需要 Kafka 时，在 `.so` 内部 `libc::fork()` 拉起守护子进程，子进程直接跳到 worker 入口（tokio + rdkafka），**不 exec 任何外部二进制**。同 pod 内多 PHP 进程通过 `flock` 互斥，只产 1 个 worker，通过 UDS 共享。子进程 `setsid()` 脱离 PHP 会话 + `prctl(PR_SET_NAME)` 改名 `hi-kafka-worker` 便于 `pgrep` 定位 + 写 PID 文件。

### 构建 `.so`

**一键 Docker（推荐，跨架构）**：

```bash
cd hi-kafka-ext
PHP_VERSION=8.3 ./scripts/build-so.sh
# → dist/hi_kafka-php8.3-linux-arm64.so
# → dist/checksums.txt
```

批量产 3 个 PHP 版本：

```bash
PHP_VERSIONS="8.2 8.3 8.4" ./scripts/build-so.sh
```

跨 CPU 架构（buildx）：

```bash
PLATFORM=linux/amd64 PHP_VERSION=8.3 ./scripts/build-so.sh
PLATFORM=linux/arm64 PHP_VERSION=8.3 ./scripts/build-so.sh
```

**直接 cargo（dev / 已在 Linux 目标机）**：

```bash
cd hi-kafka-ext
cargo build -p hi-kafka --release --features kafka
# 产物：target/release/libhi_kafka.{so,dylib}
```

### CI / Release artifact

GitHub Actions workflow：[`.github/workflows/hi-kafka-ext-ci.yml`](../.github/workflows/hi-kafka-ext-ci.yml)

矩阵：
- **test**：PHP 8.2/8.3/8.4 × {Ubuntu, macOS 8.3} → cargo fmt/clippy/test + smoke
- **integration**：跑真实 Kafka，验证 integration + producer recovery + consumer recovery 三套 PHP e2e
- **release-build**：PHP 8.2/8.3/8.4 × {linux-x86_64, linux-aarch64} → 上传 `libhi_kafka.so` + checksum

### 业务镜像示例

```dockerfile
FROM php:8.3-fpm
RUN apt-get update && apt-get install -y librdkafka1 && rm -rf /var/lib/apt/lists/*
COPY hi_kafka.so /usr/lib/php/20230831/
RUN echo "extension=hi_kafka.so" > /usr/local/etc/php/conf.d/30-hi-kafka.ini
# 完。无 worker binary、无 env var、无 INI 配置。
```

业务代码 4 行投产：

```php
$c = new Hi\Kafka\Client();
$c->registerCluster('default', ['bootstrap.servers' => $brokers]);
$c->produceSync('default', 'topic', $key, $value);
```

## 开发

### 依赖

- Rust 1.85+
- PHP 8.2+（含 `php-config` 在 PATH 中）
- 构建 Linux 扩展时还需 `libclang`（ext-php-rs 用 bindgen）

### 构建

```bash
# 单独 check
cargo check --workspace

# 构建（debug）
./scripts/build-dev.sh

# release
PROFILE=release ./scripts/build-dev.sh

# 构建并安装扩展到 php extension dir
INSTALL=1 ./scripts/build-dev.sh
```

### 单元测试

```bash
cargo test --workspace   # 44 项：10 ext + 23 proto + 9 worker + 2 worker e2e
```

### Smoke / e2e 测试

```bash
# LoggingProducer 路径（不依赖 Kafka）
./scripts/smoke-test.sh

# 真实 Kafka
docker compose -f docker-compose.kafka.yml up -d

EXT=$(realpath target/debug/libhi_kafka.dylib)
HI_KAFKA_BROKERS=127.0.0.1:9094 \
    php -d extension=$EXT tests/php/integration.php /tmp/x.sock topic-$$ --with-kafka
```

可重跑的 PHP e2e 集：

- `tests/php/integration.php` — producer + consumer 全链路 + 池命中率断言
- `tests/php/recovery.php` — kill worker 后 producer 自愈（实测 62ms）
- `tests/php/consumer-recovery.php` — kill worker 后 consumer 自动重订阅（实测 5.5s）
- `tests/php/configs.php` — ConnectionConfig + ProducerConfig + ConsumerConfig 联用

## PHP API

### 全局函数

```php
// 元信息
hi_kafka_version(): string
hi_kafka_runtime(): array              // ['blocking'] 或 ['blocking', 'swoole', ...]
hi_kafka_pool_stats(): array           // 各 socket 的连接池统计
hi_kafka_retry_stats(): array          // Producer IPC 自动重试统计（worker 崩溃恢复）
hi_kafka_resubscribe_stats(): array    // Consumer 自动重订阅统计
hi_kafka_ensure_worker(?string $socket = null): void

// 集群注册（业务连接配置完全由 PHP 控制）
hi_kafka_register_cluster(string $cluster, array $config, ?string $socket = null, ?int $timeoutMs = 5000): void

// Producer
hi_kafka_produce_fnf(string $cluster, string $topic, string $key, string $value, ?string $socket = null): void
hi_kafka_produce_sync(string $cluster, string $topic, string $key, string $value, ?int $timeoutMs = 5000, ?string $socket = null): array

// Consumer
hi_kafka_subscribe(string $cluster, string $groupId, array $topics, ?array $config = null, ?string $socket = null, ?int $timeoutMs = 5000): int
hi_kafka_poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array
hi_kafka_commit(int $subscriptionId, ?int $timeoutMs = 5000): void
hi_kafka_unsubscribe(int $subscriptionId): void
```

> **socket 参数可选**：缺省 `/tmp/hi-kafka.sock`，可被环境变量 `HI_KAFKA_SOCKET` 覆盖。

### `Hi\Kafka\Client` 类（PHP-FPM / CLI / 阻塞场景）

```php
// socket 可选，缺省 /tmp/hi-kafka.sock
$c = new Hi\Kafka\Client();

// === 集群配置由 PHP 完全控制（首次调用前必须注册）===
$c->registerCluster('main', [
    'bootstrap.servers' => 'kafka-1:9092,kafka-2:9092',
    'compression.type'  => 'lz4',
    'security.protocol' => 'SASL_SSL',
    'sasl.mechanism'    => 'PLAIN',
    'sasl.username'     => $vault->get('main/user'),
    'sasl.password'     => $vault->get('main/pwd'),
]);

$c->registerCluster('audit', [
    'bootstrap.servers' => 'audit-kafka:9092',
    'security.protocol' => 'SSL',
    'ssl.ca.location'   => '/etc/ssl/audit-ca.pem',
]);

// === Producer ===
$c->produceFnf('main', 'orders', $key, $payload);
$r = $c->produceSync('main', 'orders', $key, $payload, 5000);
// $r = ['ok' => true,  'partition' => 0, 'offset' => 42]
// 或   ['ok' => false, 'code' => 7, 'message' => '...', 'retryable' => true]

// === Consumer ===
$sub = $c->subscribe('main', 'order-group', ['orders'], [
    'auto.offset.reset' => 'earliest',
]);
while ($running) {
    $batch = $c->poll($sub, maxMessages: 100, timeoutMs: 1000);
    foreach ($batch as $msg) {
        process($msg);
    }
    $c->commit($sub);
}
$c->unsubscribe($sub);
```

### 强类型配置 — `Hi\Kafka\{Connection,Producer,Consumer}Config`

避免散乱字符串键，用配置类把 librdkafka 全套参数包装好：

```php
use Hi\Kafka\{Client, ConnectionConfig, ProducerConfig, ConsumerConfig, ConsumeOffsetType};

$conn = new ConnectionConfig(
    brokers: ['kafka-1:9094', 'kafka-2:9094'],
    sasl: [
        'mechanism' => 'SCRAM-SHA-512',
        'username'  => env('KAFKA_USER'),
        'password'  => env('KAFKA_PWD'),
    ],
    ssl: [
        'root_ca' => '/etc/ssl/kafka-ca.pem',
    ],
);

$prod = (new ProducerConfig())
    ->setCompressionType('lz4')
    ->setAcks('all')
    ->setIdempotent(true)
    ->setLingerMs(5);

$client = new Client();
$client->registerCluster('main', [
    ...$conn->toLibrdkafkaConfig(),    // 自动判定 security.protocol = SASL_SSL
    ...$prod->toLibrdkafkaConfig(),    // 6 个 producer 参数
]);

$cons = (new ConsumerConfig())
    ->setGroupId('order-processor')
    ->setOffset(ConsumeOffsetType::AtStart)        // → auto.offset.reset=earliest
    ->setSessionTimeoutMs(30_000)
    ->setMaxPollIntervalMs(300_000)
    ->setIsolationLevel('read_committed')           // 仅消费已提交事务消息
    ->setPartitionAssignmentStrategy('cooperative-sticky');

$sub = $client->subscribe('main', $cons->getGroupId(), ['orders'], $cons->toLibrdkafkaConfig());
```

### 加密连接（PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL / mTLS）

`ConnectionConfig` 按字段自动判定 `security.protocol`：

| 配置组合 | 翻译为 |
|---|---|
| `brokers` 单独 | `PLAINTEXT` |
| `brokers + sasl` | `SASL_PLAINTEXT` + `sasl.{mechanism,username,password}` |
| `brokers + ssl` | `SSL` + `ssl.ca.location` |
| `brokers + ssl[cert,key]` | `SSL` 双向 mTLS |
| `brokers + sasl + ssl` | `SASL_SSL`（公网生产典型） |

**任意 librdkafka 键**通过 `extra` 兜底，librdkafka 全套 200+ 参数无死角：

```php
new ConnectionConfig(
    brokers: ['k:9094'],
    sasl: ['mechanism' => 'PLAIN', 'username' => 'u', 'password' => 'p'],
    ssl: ['root_ca' => '/ca.pem', 'verify_hostname' => false],
    extra: [
        'client.id' => 'my-service-v2',
        'socket.timeout.ms' => '30000',
        'enable.sasl.oauthbearer.unsecure.jwt' => 'true',
    ],
);
```

### 多集群同时连接

每集群独立 `librdkafka` 实例、独立连接池、独立故障域。一个 PHP 服务连 N 个集群完全自然：

```php
$c->registerCluster('main',  ['bootstrap.servers' => 'main-kafka:9092',  ...]);
$c->registerCluster('audit', ['bootstrap.servers' => 'audit-kafka:9092', ...]);
$c->registerCluster('cdc',   ['bootstrap.servers' => 'cdc-kafka:9092',   ...]);

$c->produceSync('main',  'orders',     $k, $payload);   // → 主集群
$c->produceSync('audit', 'access-log', $k, $audit);     // → 审计集群

$cdcSub = $c->subscribe('cdc', 'sync', ['db.public.orders']);  // ← CDC 流
foreach ($c->poll($cdcSub, 100, 1000) as $msg) {
    $c->produceSync('main', 'orders.normalized', $msg['key'], normalize($msg));
}
```

### `Hi\Kafka\SwooleClient` 类（Swoole 协程场景）

纯 PHP 实现，协程上下文用 `Swoole\Coroutine\Socket` 走 reactor，不阻塞其它协程。协议编解码复用扩展暴露的 `hi_kafka_*` 原语，**单一协议源**。

源文件：[php-driver/src/Hi/Kafka/SwooleClient.php](php-driver/src/Hi/Kafka/SwooleClient.php)

```php
use Swoole\Coroutine;

require_once 'path/to/Hi/Kafka/SwooleClient.php';

Coroutine\run(function () {
    $client = new Hi\Kafka\SwooleClient();   // 同样支持 socket 缺省

    // 集群注册：PHP 控制
    $client->registerCluster('main', ['bootstrap.servers' => 'kafka:9094']);

    // Producer
    $r = $client->produceSync('main', 'orders', 'k', 'v', 5000);

    // Consumer
    $sub = $client->subscribe('main', 'order-group', ['orders'], [
        'auto.offset.reset' => 'earliest',
    ]);
    while ($running) {
        $batch = $client->poll($sub, maxMessages: 100, timeoutMs: 1000);
        foreach ($batch as $msg) { process($msg); }
        $client->commit($sub);
    }
    $client->unsubscribe($sub);
});
```

**协程并发实测**：
- 20 个协程并发 `produceSync` → **8.6ms 全部完成**
- 单协程 `poll` 阻塞 3.2s 期间，后台计数协程跑了 500 次 → **reactor 完全不阻塞**

### 协议编解码原语（PHP-level driver 共用）

```php
// 元
hi_kafka_next_cid(): int                              // 全进程单调自增
hi_kafka_header_len(): int                            // 常量 13
hi_kafka_parse_header(string $bytes): array           // 13B header → kind/cid/payload_len

// Producer
hi_kafka_encode_fnf_frame(string $c, string $t, string $k, string $v): string
hi_kafka_encode_req_frame(string $c, string $t, string $k, string $v): array  // ['cid', 'frame']
hi_kafka_decode_resp_frame(string $bytes): array      // PRODUCE_RESP

// Consumer
hi_kafka_encode_subscribe_frame(string $cluster, string $group, array $topics, ?array $config): array
hi_kafka_encode_poll_frame(int $subId, int $maxMessages, int $timeoutMs): array
hi_kafka_encode_commit_frame(int $subId): array
hi_kafka_encode_unsubscribe_frame(int $subId): string
hi_kafka_encode_register_cluster_frame(string $cluster, array $config): array
hi_kafka_decode_consumer_resp(string $bytes): array   // 统一解析 subscribe/poll/commit/register_cluster resp
```

## Worker（dev tool）

主路径下 worker **内嵌进 .so**，无需独立部署。但 `worker/` crate 仍可单独 build 出 standalone binary，方便单独调试 / 性能压测 / 配 systemd：

```bash
cargo build -p hi-kafka-worker --features kafka --release
./target/release/hi-kafka-worker --socket /tmp/hi-kafka.sock --brokers 127.0.0.1:9094
```

| 选项 | 环境变量 | 默认 | 说明 |
| --- | --- | --- | --- |
| `--socket` | `HI_KAFKA_SOCKET` | `/tmp/hi-kafka.sock` | Unix socket 监听路径 |
| `--brokers` | `HI_KAFKA_BROKERS` | (无) | 启动时预注册 `default` 集群（仅 dev 兼容）|
| `--log-level` | `HI_KAFKA_LOG_LEVEL` | `info` | 日志级别 |
| `--drain-timeout-ms` | `HI_KAFKA_DRAIN_TIMEOUT_MS` | `10000` | SIGTERM 后 drain 超时 |
| `--metrics-addr` | `HI_KAFKA_METRICS_ADDR` | `127.0.0.1:9876` | Prometheus 端点；空字符串禁用 |

> 生产环境用单 `.so`，所有集群配置走 `Hi\Kafka\Client::registerCluster()`，不依赖 `--brokers`。

## 扩展端环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `HI_KAFKA_SOCKET` | `/tmp/hi-kafka.sock` | 默认 UDS 路径（PHP 不传 socket 时） |
| `HI_KAFKA_BROKERS` | (无) | autospawn 时预注册 `default` 集群（兼容，新代码不用）|
| `HI_KAFKA_LOG_LEVEL` | `info` | worker 日志级别 |
| `HI_KAFKA_LOG_FILE` | (stdout/stderr → /dev/null) | worker 日志写到指定文件 |
| `HI_KAFKA_METRICS_ADDR` | `127.0.0.1:9876` | Prometheus 端点 |

## Metrics

`http://127.0.0.1:9876/metrics`：

```
hi_kafka_worker_uptime_seconds (gauge)
hi_kafka_worker_info{version="..."} (gauge)
hi_kafka_ipc_frames_total (counter)
hi_kafka_ipc_connections_total (counter)
hi_kafka_produce_fnf_total (counter)
hi_kafka_produce_fnf_failed_total (counter)
hi_kafka_produce_req_total (counter)
hi_kafka_produce_resp_ok_total (counter)
hi_kafka_produce_resp_err_total (counter)
hi_kafka_frames_dropped_draining_total (counter)
```

`http://127.0.0.1:9876/healthz`：返回 `ok` + 200。

PHP 端可观测：

```php
hi_kafka_pool_stats();         // 每 socket 的 acquires / hits / misses / closed / poisoned
hi_kafka_retry_stats();        // producer IPC 自动重试（worker 崩溃）次数 / 成功 / 失败
hi_kafka_resubscribe_stats();  // consumer 自动重订阅次数 / 成功 / 失败
```

## 状态

### Phase 1 — MVP（已完成 ✅）

- [x] Workspace skeleton（proto / worker / ext 三 crate）
- [x] IPC 帧编解码（13B header：len/type/cid + payload）
- [x] PRODUCE_FNF payload v1
- [x] Worker tokio + UDS listener，分帧解码 + dispatch
- [x] Producer 抽象 + LoggingProducer + KafkaProducer (feature gated)
- [x] PHP 扩展（ext-php-rs）暴露 `hi_kafka_version` / `hi_kafka_produce_fnf` / `hi_kafka_ensure_worker`
- [x] Worker 自动启动（`flock` 互斥 + `setsid` 守护化）
- [x] 真实 Kafka e2e 验证
- [x] 并发 PHP 进程的 autospawn 互斥验证
- [x] Docker Compose 本地 Kafka 环境

### Phase 2 — 生产可用（已完成 ✅）

**协议 + 同步 ack**：
- [x] PRODUCE_REQ / PRODUCE_RESP（cid 路由 + 同步 ack）
- [x] `produce_sync` 端到端验证（offset 严格递增、broker ack）

**生产可靠性**：
- [x] 优雅停机 drain（SIGTERM → 拒绝新 REQ → flush rdkafka → 退出）
- [x] Prometheus `/metrics` 端点 + `/healthz`
- [x] 11 项 worker 指标
- [x] `Hi\Kafka\Client` PHP 类封装

**性能**：
- [x] **UDS 连接池**（全局共享、RAII、半关闭探测、poison 机制）
- [x] **池效果验证**：300 produce → 99.67% 命中率
- [x] **`ensure_worker` 探测缓存**（300 produces 仅 2 个连接，1259ms → 4.4ms）

**协程**：
- [x] **协程运行时检测**（`hi_kafka_runtime()`）
- [x] **Swoole 协程 driver**（`Hi\Kafka\SwooleClient` + 协议原语）
- [x] 20 协程并发 produceSync 8.6ms；poll 阻塞期间 reactor 不阻塞

**Consumer 完整闭环**：
- [x] **Consumer 协议**：SUBSCRIBE/POLL/COMMIT/UNSUBSCRIBE 帧 + payload
- [x] **Worker Consumer trait** + LoggingConsumer + KafkaConsumer
- [x] **Worker dispatch consumer 帧**（拉流后台 task + buffer + notify）
- [x] **PHP consumer API**：4 个全局函数 + Client 类 4 个方法
- [x] **Consumer 协议原语暴露** + **SwooleClient consumer**

**崩溃自愈**：
- [x] **扩展端自动重试 worker 崩溃**：BrokenPipe/EOF/connect-refused 透明 invalidate + ensure_worker + 重试一次
- [x] **Producer 自愈 e2e**：worker 死后下一条 produce **62ms 内透明完成**
- [x] **Consumer 虚拟 ID + 自动重订阅**：`virtual_id → (subscribe 参数, real_id)` 注册表；real subscription 消失时透明 resubscribe + 重试
- [x] **Consumer 自愈 e2e**：消费 5 条 + commit → kill worker → 同 `$sub` 继续 poll **5.5s 内拿到新 5 条**

**工程化**：
- [x] **综合集成测试**：`tests/php/integration.php` 7 阶段回归
- [x] **Dockerfile 跨架构**：多 PHP 版本 × Linux x86_64/arm64
- [x] **`scripts/build-so.sh`**：一键产 + sha256
- [x] **GitHub Actions CI 矩阵**：test + integration + release-build
- [x] **单 `.so` 分发**：worker 内嵌进扩展，`libc::fork()` 不 exec 外部二进制；release 单文件 ~2.7 MB (macOS) / ~3.9 MB (Linux)

**集群配置 PHP 化**（最终态）：
- [x] **REGISTER_CLUSTER 协议帧** + worker 端 `ClusterRegistry`
- [x] **`registerCluster()`** PHP API（Client、SwooleClient、全局函数）
- [x] **多集群同时连接**：每集群独立 librdkafka 实例 / 连接池 / 故障域
- [x] **`socket` 参数全局可选**，缺省 `/tmp/hi-kafka.sock`，可 `HI_KAFKA_SOCKET` 覆盖
- [x] **`Hi\Kafka\ConnectionConfig`** 升级：SASL/SSL/mTLS/SASL_SSL 自动判定 + `extra` 兜底
- [x] **`Hi\Kafka\ProducerConfig`** 新建：13 个 producer 参数 setter
- [x] **`Hi\Kafka\ConsumerConfig`** 升级：9 个 consumer 参数 setter + `ConsumeOffsetType` 映射
- [x] **`tests/php/configs.php`** e2e：三配置类联用 + earliest 消费 + read_committed + cooperative-sticky 全过

### Phase 3 — 待启动

- [ ] 精确 seek（`ConsumeOffsetType::At` / `AfterMilli` / `Relative`）
- [ ] Per-message headers / timestamp / partition
- [ ] Transactional producer（PRODUCE_REQ 套 TXN_BEGIN/COMMIT）
- [ ] Rebalance 事件通知 PHP
- [ ] Credit-based 背压
- [ ] OAUTHBEARER 动态 token 刷新 callback
- [ ] Swow driver（`Hi\Kafka\SwowClient`，结构同 SwooleClient）
- [ ] Worker 进程内 panic 防护（rdkafka stream 出错时单 subscription 重建）

详见 [RFC §12 实施计划](../src/Kafka/RFC-EMBEDDED-WORKER.md#12-实施计划)。
</content>
