// 业务 API（produce/subscribe + 完整 options）天然多参数；保留可读签名
// 而不是塞一个 Options struct 让 PHP 调用方更难用。
#![allow(clippy::too_many_arguments)]
// 数组组合 helpers 用 `.iter()/.into_iter()/.zip()/.collect()` 链式可读性比
// "fold + 显式 IntoIterator" 强，且 PHP 调用层的代码生成器倾向 explicit。
#![allow(clippy::useless_conversion)]
// ext-php-rs 的 ZendHashTable::insert 在错误时只能透传错误链，map_err 到
// PhpException 是标准用法——切到 inspect_err 反而丢类型。
#![allow(
    clippy::manual_map,
    clippy::manual_ok_err,
    clippy::manual_inspect,
    clippy::manual_flatten,
    clippy::redundant_pattern_matching
)]

mod client;
mod client_interface;
mod cluster_replay;
mod ini_config;
mod ipc;
mod lifecycle;
mod pool;
mod protocol;
mod spawn;
mod subscription;
mod worker_entry;
mod worker_health;

use ext_php_rs::binary::Binary;
use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

pub use client::Client;

/// `Hi\Kafka\KafkaException` —— 所有 Kafka 操作失败抛出的统一异常。
/// `extends \Exception`，额外携带机器可读的错误分类（见 proto `ErrorKind`），
/// 让业务能 `catch (KafkaException $e)` 后按 `$e->getKind()` / `$e->isRetryable()`
/// 精确处理，而非靠 message 字符串匹配。
#[php_class(name = "Hi\\Kafka\\KafkaException")]
#[extends(ext_php_rs::zend::ce::exception())]
#[derive(Debug)]
pub struct KafkaException {
    #[prop(flags = ext_php_rs::flags::PropertyFlags::Public)]
    message: String,
    #[prop(flags = ext_php_rs::flags::PropertyFlags::Public)]
    code: i64,
    #[prop(flags = ext_php_rs::flags::PropertyFlags::Public)]
    kind: i64,
    #[prop(flags = ext_php_rs::flags::PropertyFlags::Public)]
    kind_name: String,
    #[prop(flags = ext_php_rs::flags::PropertyFlags::Public)]
    retryable: bool,
    #[prop(flags = ext_php_rs::flags::PropertyFlags::Public)]
    native_code: i64,
}

#[php_impl]
impl KafkaException {
    /// 供 PHP（协程 driver）构造：
    /// `new KafkaException($msg, $kind, $kindName, $retryable, $nativeCode)`。
    pub fn __construct(
        message: String,
        kind: i64,
        kind_name: String,
        retryable: bool,
        native_code: i64,
    ) -> Self {
        Self {
            message,
            code: kind,
            kind,
            kind_name,
            retryable,
            native_code,
        }
    }

    /// 机器可读错误大类（数值，见 `ErrorKind`）。
    pub fn get_kind(&self) -> i64 {
        self.kind
    }
    /// 错误大类名（如 `"BROKER_RETRYABLE"` / `"AUTHN_AUTHZ"`）。
    pub fn get_kind_name(&self) -> String {
        self.kind_name.clone()
    }
    /// 是否值得重试。
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
    /// 原生 librdkafka 错误码（无则 0）。
    pub fn get_native_code(&self) -> i64 {
        self.native_code
    }
}

/// 把 IPC 错误转成 PHP 异常：worker 回的结构化错误（`Error` 帧）→ 带 kind 的
/// `KafkaException` 实例；其余（本地 IO / spawn / 协议错乱）→ 通用 `PhpException`。
pub(crate) fn ipc_err_to_php(e: ipc::IpcError) -> PhpException {
    if let ipc::IpcError::Worker {
        kind,
        retryable,
        native_code,
        message,
    } = &e
    {
        let ex = KafkaException {
            message: message.clone(),
            code: kind.as_u16() as i64,
            kind: kind.as_u16() as i64,
            kind_name: kind.as_str().to_string(),
            retryable: *retryable,
            native_code: *native_code as i64,
        };
        if let Ok(zv) = ex.into_zval(true) {
            let mut ph = PhpException::from_class::<KafkaException>(message.clone());
            ph.set_object(Some(zv));
            return ph;
        }
    }
    PhpException::default(e.to_string())
}

/// 默认 Unix socket 路径。所有 `socket` 参数缺省时使用此值。
/// 可通过环境变量 `HI_KAFKA_SOCKET` 覆盖（仅在扩展首次解析时读取）。
pub(crate) const DEFAULT_SOCKET: &str = "/tmp/hi-kafka.sock";

pub(crate) fn resolve_socket(socket: Option<&str>) -> String {
    if let Some(s) = socket.filter(|s| !s.is_empty()) {
        return s.to_string();
    }
    std::env::var("HI_KAFKA_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET.to_string())
}

/// 扩展版本
#[php_function]
pub fn hi_kafka_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 显式启动 worker（如果还没在跑）。
/// L: 业务显式调用时**强制 fresh probe**——invalidate 内部缓存后 ensure 会调
/// spawn::ensure_worker（内部 worker_alive 是 ~50μs UDS probe），死了就重新 spawn
/// + 重放 cluster；活着就直接通过。这保证"业务调完 ensureWorker 后下一行 produce
/// 一定不会再触发 worker 自愈 retry"语义成立。频繁 produce 的 hot path 仍走
/// `ipc::ensure` 自己的 cache 快路径，不受影响。
#[php_function]
pub fn hi_kafka_ensure_worker(socket: Option<String>) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    worker_health::invalidate(&socket);
    ipc::ensure(&socket).map_err(|e| PhpException::default(format!("ensure_worker: {e}")))?;
    Ok(())
}

/// 注册或覆盖一个 Kafka 集群。
///
/// `$config` 必须包含 `bootstrap.servers`，其它键值原样透传给 librdkafka。
/// 同名集群配置会被覆盖（注意：已建立的连接不会立即重建）。
///
/// 业务侧的标准模式：在请求开始前注册所需集群，之后 produce/subscribe
/// 用 cluster 名引用。worker 端按集群独立维护客户端，多集群天然隔离。
#[php_function]
pub fn hi_kafka_register_cluster(
    cluster: &str,
    config: std::collections::HashMap<String, String>,
    socket: Option<String>,
    timeout_ms: Option<i64>,
) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    let cfg_vec: Vec<(String, String)> = config.into_iter().collect();
    ipc::register_cluster(&socket, cluster, cfg_vec, timeout).map_err(ipc_err_to_php)?;
    Ok(())
}

/// Fire-and-forget 全局函数 API。
///
/// 高级选项（全部可选，缺省 None 表示由 librdkafka 自动决定）：
/// - `$headers`：Kafka 消息头（关联数组）
/// - `$partition`：明确写入分区编号；`null`/缺省 = 走 partitioner（key hash）
/// - `$timestampMs`：消息时间戳（毫秒）；`null`/缺省 = librdkafka 当前时间
#[php_function]
pub fn hi_kafka_produce_fnf(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    socket: Option<String>,
) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    let opts = build_options(headers, partition, timestamp_ms);
    ipc::produce_fnf(&socket, cluster, topic, key, value, opts).map_err(ipc_err_to_php)
}

/// 同步带 ack 全局函数 API。参数同 [`hi_kafka_produce_fnf`] + `timeout_ms`。
#[php_function]
pub fn hi_kafka_produce_sync(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    timeout_ms: Option<i64>,
    socket: Option<String>,
) -> PhpResult<Zval> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    let opts = build_options(headers, partition, timestamp_ms);
    let resp = ipc::produce_sync(&socket, cluster, topic, key, value, opts, timeout)
        .map_err(ipc_err_to_php)?;
    client::resp_to_zval(resp)
}

pub(crate) fn build_options(
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> ipc::ProduceOptions {
    ipc::ProduceOptions {
        headers: headers
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, bytes::Bytes::from(v.into_bytes())))
            .collect(),
        partition: partition
            .map(|p| p.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
            .unwrap_or(-1),
        timestamp_ms: timestamp_ms.unwrap_or(-1),
    }
}

// ============================================================================
// Consumer 全局函数 API
// ============================================================================

/// 创建订阅。返回 virtual subscription_id（int），后续 poll/commit/unsubscribe 都用它。
///
/// **自愈语义**：返回的 ID 是扩展层维护的 virtual ID。worker 崩溃重启后，
/// poll/commit 时会透明重订阅（real_id 在底层换新，virtual_id 不变）。
/// 业务侧 `$sub` 句柄全程稳定，未提交消息按 Kafka at-least-once 语义重派发。
///
/// `$topics` 字符串数组；`$config` 关联数组（可选，consumer 级别配置如
/// `auto.offset.reset`、`session.timeout.ms`）。
#[php_function]
pub fn hi_kafka_subscribe(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Option<std::collections::HashMap<String, String>>,
    socket: Option<String>,
    timeout_ms: Option<i64>,
) -> PhpResult<i64> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    let cfg_vec: Vec<(String, String)> = config.unwrap_or_default().into_iter().collect();
    let id = subscription::subscribe(&socket, cluster, group_id, topics, cfg_vec, timeout)
        .map_err(ipc_err_to_php)?;
    Ok(id as i64)
}

/// 拉一批消息。命中底层 subscription not found 时透明重订阅 + 重试。
#[php_function]
pub fn hi_kafka_poll(subscription_id: i64, max_messages: i64, timeout_ms: i64) -> PhpResult<Zval> {
    let messages = subscription::poll(
        subscription_id as u64,
        max_messages.max(1) as u32,
        timeout_ms.max(0) as u32,
    )
    .map_err(ipc_err_to_php)?;
    messages_to_zval(messages)
}

/// 同步提交 offset。命中底层 subscription not found 时透明重订阅 + 重试。
#[php_function]
pub fn hi_kafka_commit(subscription_id: i64, timeout_ms: Option<i64>) -> PhpResult<()> {
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    subscription::commit(subscription_id as u64, timeout).map_err(ipc_err_to_php)?;
    Ok(())
}

/// 退订。幂等，对已不存在的 virtual_id 直接返回。
#[php_function]
pub fn hi_kafka_unsubscribe(subscription_id: i64) -> PhpResult<()> {
    subscription::unsubscribe(subscription_id as u64).map_err(ipc_err_to_php)?;
    Ok(())
}

/// **@internal**（给协程 driver 用）：登记一个协程 driver 创建的订阅 real_id。
///
/// Swoole/Swow driver 的订阅由 PHP 层管理、不进 Rust `subscription` 注册表，进程退出
/// 时 MSHUTDOWN 默认看不到、无法 unsubscribe → worker 活跃订阅不归零 → Goodbye 被挡。
/// driver 在 `subscribe` 成功后调本函数登记，MSHUTDOWN 即会主动 unsubscribe，让协程
/// 消费者进程退出也能亚秒触发 worker 自退。阻塞 `Client` 无需调用（其订阅已在注册表里）。
#[php_function]
pub fn hi_kafka_track_subscription(subscription_id: i64, socket: Option<String>) {
    let socket = resolve_socket(socket.as_deref());
    lifecycle::track_subscription(&socket, subscription_id as u64);
}

/// **@internal**（给协程 driver 用）：注销一个订阅登记（driver 主动 `unsubscribe` 后调），
/// 与 [`hi_kafka_track_subscription`] 配对，避免 MSHUTDOWN 重复 unsubscribe 已退订的订阅。
#[php_function]
pub fn hi_kafka_untrack_subscription(subscription_id: i64, socket: Option<String>) {
    let socket = resolve_socket(socket.as_deref());
    lifecycle::untrack_subscription(&socket, subscription_id as u64);
}

/// 扩展端 consumer 自愈重订阅统计。
#[php_function]
pub fn hi_kafka_resubscribe_stats() -> PhpResult<Zval> {
    let s = subscription::resubscribe_stats();
    let mut ht = ZendHashTable::new();
    ht.insert("attempts", s.attempts as i64)
        .map_err(|e| PhpException::default(format!("attempts: {e}")))?;
    ht.insert("successes", s.successes as i64)
        .map_err(|e| PhpException::default(format!("successes: {e}")))?;
    ht.insert("failures", s.failures as i64)
        .map_err(|e| PhpException::default(format!("failures: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 把三个平行数组组合成 `Vec<(String, i32, i64)>`。长度必须一致。
pub(crate) fn build_offset_targets(
    topics: Vec<String>,
    partitions: Vec<i64>,
    offsets: Vec<i64>,
) -> Result<Vec<(String, i32, i64)>, String> {
    if topics.len() != partitions.len() || topics.len() != offsets.len() {
        return Err(format!(
            "topics({}), partitions({}), offsets({}) 长度必须一致",
            topics.len(),
            partitions.len(),
            offsets.len()
        ));
    }
    Ok(topics
        .into_iter()
        .zip(partitions.into_iter())
        .zip(offsets.into_iter())
        .map(|((t, p), o)| (t, p as i32, o))
        .collect())
}

/// 把两个平行数组组合成 binary headers `Vec<(String, Bytes)>`。
/// 头部 name UTF-8（Kafka 协议要求），value 任意字节。两数组长度必须一致。
pub(crate) fn build_binary_headers(
    names: Vec<String>,
    values: Vec<ext_php_rs::binary::Binary<u8>>,
) -> Result<Vec<(String, bytes::Bytes)>, String> {
    if names.len() != values.len() {
        return Err(format!(
            "header names({}) 和 values({}) 长度必须一致",
            names.len(),
            values.len()
        ));
    }
    Ok(names
        .into_iter()
        .zip(values.into_iter())
        .map(|(n, v)| (n, bytes::Bytes::from(Vec::<u8>::from(v))))
        .collect())
}

/// 把两个平行数组组合成 `Vec<(String, i32)>`。空数组合法（应用到全部当前 assignment）。
pub(crate) fn build_partition_specs(
    topics: Vec<String>,
    partitions: Vec<i64>,
) -> Result<Vec<(String, i32)>, String> {
    if topics.len() != partitions.len() {
        return Err(format!(
            "topics({}) 和 partitions({}) 长度必须一致",
            topics.len(),
            partitions.len(),
        ));
    }
    Ok(topics
        .into_iter()
        .zip(partitions.into_iter())
        .map(|(t, p)| (t, p as i32))
        .collect())
}

pub(crate) fn rebalance_events_to_zval(
    events: Vec<hi_kafka_proto::RebalanceEvent>,
) -> PhpResult<Zval> {
    let mut top = ZendHashTable::new();
    for e in events {
        let mut inner = ZendHashTable::new();
        match e {
            hi_kafka_proto::RebalanceEvent::Assign { partitions } => {
                inner
                    .insert("type", "assign")
                    .map_err(|e| PhpException::default(format!("type: {e}")))?;
                inner
                    .insert("partitions", partitions_to_zval(&partitions)?)
                    .map_err(|e| PhpException::default(format!("partitions: {e}")))?;
            }
            hi_kafka_proto::RebalanceEvent::Revoke { partitions } => {
                inner
                    .insert("type", "revoke")
                    .map_err(|e| PhpException::default(format!("type: {e}")))?;
                inner
                    .insert("partitions", partitions_to_zval(&partitions)?)
                    .map_err(|e| PhpException::default(format!("partitions: {e}")))?;
            }
            hi_kafka_proto::RebalanceEvent::Error { message } => {
                inner
                    .insert("type", "error")
                    .map_err(|e| PhpException::default(format!("type: {e}")))?;
                inner
                    .insert("message", message.as_str())
                    .map_err(|e| PhpException::default(format!("message: {e}")))?;
            }
        }
        let inner_zval = inner
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("inner: {e}")))?;
        top.push(inner_zval)
            .map_err(|e| PhpException::default(format!("push: {e}")))?;
    }
    top.into_zval(false)
        .map_err(|e| PhpException::default(format!("top: {e}")))
}

fn partitions_to_zval(parts: &[(String, i32)]) -> PhpResult<Zval> {
    let mut arr = ZendHashTable::new();
    for (topic, partition) in parts {
        let mut entry = ZendHashTable::new();
        entry
            .insert("topic", topic.as_str())
            .map_err(|e| PhpException::default(format!("topic: {e}")))?;
        entry
            .insert("partition", *partition as i64)
            .map_err(|e| PhpException::default(format!("partition: {e}")))?;
        let z = entry
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("entry: {e}")))?;
        arr.push(z)
            .map_err(|e| PhpException::default(format!("push: {e}")))?;
    }
    arr.into_zval(false)
        .map_err(|e| PhpException::default(format!("arr: {e}")))
}

pub(crate) fn messages_to_zval(messages: Vec<hi_kafka_proto::ConsumerMessage>) -> PhpResult<Zval> {
    let mut top = ZendHashTable::new();
    for m in messages {
        let mut inner = ZendHashTable::new();
        inner
            .insert("topic", m.topic.as_str())
            .map_err(|e| PhpException::default(format!("topic: {e}")))?;
        inner
            .insert("partition", m.partition as i64)
            .map_err(|e| PhpException::default(format!("partition: {e}")))?;
        inner
            .insert("offset", m.offset)
            .map_err(|e| PhpException::default(format!("offset: {e}")))?;
        inner
            .insert("timestamp_ms", m.timestamp_ms)
            .map_err(|e| PhpException::default(format!("timestamp_ms: {e}")))?;
        // M: binary-safe——Binary<u8> 直接走 zend string_init，全程 byte array，
        // 不经 Rust `String` 的 UTF-8 不变式（之前 from_utf8_unchecked 是 UB）。
        inner
            .insert("key", Binary::<u8>::from(m.key.to_vec()))
            .map_err(|e| PhpException::default(format!("key: {e}")))?;
        inner
            .insert("value", Binary::<u8>::from(m.value.to_vec()))
            .map_err(|e| PhpException::default(format!("value: {e}")))?;

        // Headers 关联数组：name → binary value
        let mut headers_ht = ZendHashTable::new();
        for (name, value) in &m.headers {
            headers_ht
                .insert(name.as_str(), Binary::<u8>::from(value.to_vec()))
                .map_err(|e| PhpException::default(format!("header {name}: {e}")))?;
        }
        let headers_zval = headers_ht
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("headers: {e}")))?;
        inner
            .insert("headers", headers_zval)
            .map_err(|e| PhpException::default(format!("insert headers: {e}")))?;

        let inner_zval = inner
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("inner: {e}")))?;
        top.push(inner_zval)
            .map_err(|e| PhpException::default(format!("push: {e}")))?;
    }
    top.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 返回扩展进程内所有 socket 路径的连接池统计。
///
/// 数组形如：
///
/// ```text
/// [
///   "/var/run/hi-kafka/worker.sock" => [
///     "max_idle" => 16,
///     "idle"     => 3,
///     "acquires" => 100,
///     "hits"     => 97,
///     "misses"   => 3,
///     "closed"   => 0,
///     "poisoned" => 0,
///   ],
/// ]
/// ```
#[php_function]
pub fn hi_kafka_pool_stats() -> PhpResult<Zval> {
    let mut top = ZendHashTable::new();
    for (path, stats, idle, max_idle) in pool::all_stats() {
        let mut inner = ZendHashTable::new();
        inner
            .insert("max_idle", max_idle as i64)
            .map_err(|e| PhpException::default(format!("max_idle: {e}")))?;
        inner
            .insert("idle", idle as i64)
            .map_err(|e| PhpException::default(format!("idle: {e}")))?;
        inner
            .insert("acquires", stats.acquires_total as i64)
            .map_err(|e| PhpException::default(format!("acquires: {e}")))?;
        inner
            .insert("hits", stats.hits_total as i64)
            .map_err(|e| PhpException::default(format!("hits: {e}")))?;
        inner
            .insert("misses", stats.misses_total as i64)
            .map_err(|e| PhpException::default(format!("misses: {e}")))?;
        inner
            .insert("closed", stats.closed_total as i64)
            .map_err(|e| PhpException::default(format!("closed: {e}")))?;
        inner
            .insert("poisoned", stats.poisoned_total as i64)
            .map_err(|e| PhpException::default(format!("poisoned: {e}")))?;
        let inner_zval = inner
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("inner: {e}")))?;
        top.insert(path.to_string_lossy().as_ref(), inner_zval)
            .map_err(|e| PhpException::default(format!("top: {e}")))?;
    }
    top.into_zval(false)
        .map_err(|e| PhpException::default(format!("top into_zval: {e}")))
}

/// 扩展端自动重试统计。worker 进程崩了之后业务调用的恢复次数。
///
/// 返回 `['attempts' => int, 'successes' => int, 'failures' => int]`：
/// - `attempts`：触发重试的次数（≈ worker 死亡且被 IPC 命中的次数）
/// - `successes`：重试成功的次数（业务无感）
/// - `failures`：重试也失败的次数（业务层看到错误）
#[php_function]
pub fn hi_kafka_retry_stats() -> PhpResult<Zval> {
    let s = ipc::retry_stats();
    let mut ht = ZendHashTable::new();
    ht.insert("attempts", s.attempts as i64)
        .map_err(|e| PhpException::default(format!("attempts: {e}")))?;
    ht.insert("successes", s.successes as i64)
        .map_err(|e| PhpException::default(format!("successes: {e}")))?;
    ht.insert("failures", s.failures as i64)
        .map_err(|e| PhpException::default(format!("failures: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 检测当前 PHP 加载了哪些已识别的协程运行时。
/// 返回字符串数组，例如 `["blocking"]` 或 `["blocking", "swoole"]`。
///
/// 检测策略：在 `EG(function_table)` 里查标志性函数是否注册。
/// - `swoole_version` → Swoole 已加载
/// - `Swow\Coroutine::*` 这类类方法不在函数表里，本期改查 `swow\\version`
///
/// 注：本期仍统一走阻塞 IO。返回结果**仅供观测**，不会改变扩展行为。
/// 真正的协程感知 driver 在 Phase 3。
#[php_function]
pub fn hi_kafka_runtime() -> Vec<String> {
    let mut runtimes = vec!["blocking".to_string()];
    if function_exists("swoole_version") {
        runtimes.push("swoole".to_string());
    }
    // Swow 的标志性函数（裸函数）
    if function_exists("swow\\version") {
        runtimes.push("swow".to_string());
    }
    runtimes
}

fn function_exists(name: &str) -> bool {
    use std::ffi::CString;
    let Ok(c) = CString::new(name) else {
        return false;
    };
    unsafe {
        // executor_globals.function_table 本身就是 *mut HashTable，
        // 直接读字段即可，不要再 &raw 一层
        let ft: *const ext_php_rs::ffi::HashTable =
            ext_php_rs::ffi::executor_globals.function_table;
        if ft.is_null() {
            return false;
        }
        // `_lc` 做小写匹配，符合 PHP 函数名大小写不敏感的语义
        let ptr = ext_php_rs::ffi::zend_hash_str_find_ptr_lc(ft, c.as_ptr(), name.len());
        !ptr.is_null()
    }
}

// === 协议编解码原语（给 PHP 层 Swoole/Swow driver 用） ==========================

/// 全进程单调自增 cid。
#[php_function]
pub fn hi_kafka_next_cid() -> i64 {
    protocol::next_cid() as i64
}

/// 协议帧头长度（常量 13）。便于 PHP driver 精确分两段 recv。
#[php_function]
pub fn hi_kafka_header_len() -> i64 {
    protocol::header_len() as i64
}

/// `Error` 帧的帧类型字节（`0x40`）。PHP driver 用 `hi_kafka_parse_header` 拿到 kind
/// 后与它比较，判断该帧是否要走 `hi_kafka_decode_error_frame` + 抛 KafkaException。
#[php_function]
pub fn hi_kafka_error_frame_kind() -> i64 {
    hi_kafka_proto::FrameType::Error as u8 as i64
}

/// 解析完整 `Error` 帧 →
/// `['kind'=>int, 'kind_name'=>str, 'retryable'=>bool, 'native_code'=>int, 'message'=>str]`。
#[php_function]
pub fn hi_kafka_decode_error_frame(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let pe =
        protocol::parse_error_frame(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    put(&mut ht, "kind", pe.kind as i64)?;
    put(&mut ht, "kind_name", pe.kind_name)?;
    put(&mut ht, "retryable", pe.retryable)?;
    put(&mut ht, "native_code", pe.native_code as i64)?;
    put(&mut ht, "message", pe.message.as_str())?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 编一帧 HELLO（协议握手）。返回完整 14B 帧字节（PHP binary string）。
///
/// PHP 协程 driver 在新建 UDS 连接后必须先发它再发业务帧；worker 会用
/// HELLO RESP 回应，校验 `PROTOCOL_MAJOR` 是否一致。
#[php_function]
pub fn hi_kafka_encode_hello_frame() -> PhpResult<Binary<u8>> {
    let bytes = protocol::build_hello_frame().map_err(|e| PhpException::default(e.to_string()))?;
    Ok(Binary::from(bytes))
}

/// 校验 HELLO RESP 帧；版本不匹配 / 帧格式不对 → 抛错。
#[php_function]
pub fn hi_kafka_verify_hello_resp(bytes: BinarySlice<u8>) -> PhpResult<()> {
    protocol::parse_hello_resp(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    Ok(())
}

/// 编一帧 PRODUCE_FNF（fire-and-forget），返回完整帧字节串（PHP binary string）。
#[php_function]
pub fn hi_kafka_encode_fnf_frame(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> PhpResult<Binary<u8>> {
    let opts = build_options(headers, partition, timestamp_ms);
    let bytes = protocol::build_fnf_frame(
        cluster,
        topic,
        key.as_bytes(),
        value.as_bytes(),
        opts.headers,
        opts.partition,
        opts.timestamp_ms,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(Binary::from(bytes))
}

/// 编一帧 PRODUCE_REQ，返回 `['cid' => int, 'frame' => binary]`。
#[php_function]
pub fn hi_kafka_encode_req_frame(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> PhpResult<Zval> {
    let opts = build_options(headers, partition, timestamp_ms);
    let (cid, bytes) = protocol::build_req_frame(
        cluster,
        topic,
        key.as_bytes(),
        value.as_bytes(),
        opts.headers,
        opts.partition,
        opts.timestamp_ms,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 仅解析 13B 帧头，返回 `['kind' => int, 'cid' => int, 'payload_len' => int]`。
#[php_function]
pub fn hi_kafka_parse_header(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let h =
        protocol::parse_header_only(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    ht.insert("kind", h.kind_byte as i64)
        .map_err(|e| PhpException::default(format!("kind: {e}")))?;
    ht.insert("cid", h.cid as i64)
        .map_err(|e| PhpException::default(format!("cid: {e}")))?;
    ht.insert("payload_len", h.payload_len as i64)
        .map_err(|e| PhpException::default(format!("payload_len: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 解析完整 PRODUCE_RESP 帧（含 header + payload）。
#[php_function]
pub fn hi_kafka_decode_resp_frame(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let parsed =
        protocol::parse_resp_frame(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    match parsed {
        protocol::ParsedFrame::Resp { cid, resp } => {
            ht.insert("cid", cid as i64)
                .map_err(|e| PhpException::default(format!("cid: {e}")))?;
            match resp {
                hi_kafka_proto::ProduceResp::Ok(ack) => {
                    ht.insert("ok", true)
                        .map_err(|e| PhpException::default(format!("ok: {e}")))?;
                    ht.insert("partition", ack.partition as i64)
                        .map_err(|e| PhpException::default(format!("partition: {e}")))?;
                    ht.insert("offset", ack.offset)
                        .map_err(|e| PhpException::default(format!("offset: {e}")))?;
                }
                hi_kafka_proto::ProduceResp::Err(err) => {
                    ht.insert("ok", false)
                        .map_err(|e| PhpException::default(format!("ok: {e}")))?;
                    ht.insert("code", err.code as i64)
                        .map_err(|e| PhpException::default(format!("code: {e}")))?;
                    ht.insert("message", err.message.as_str())
                        .map_err(|e| PhpException::default(format!("message: {e}")))?;
                    ht.insert("retryable", err.retryable)
                        .map_err(|e| PhpException::default(format!("retryable: {e}")))?;
                }
            }
        }
        protocol::ParsedFrame::Other { kind, cid, .. } => {
            return Err(PhpException::default(format!(
                "unexpected frame kind {kind:?} cid={cid}"
            )));
        }
    }
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

// === Consumer 协议原语 =====================================================

/// 编一帧 SUBSCRIBE_REQ。返回 `['cid' => int, 'frame' => binary]`。
#[php_function]
pub fn hi_kafka_encode_subscribe_frame(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Option<std::collections::HashMap<String, String>>,
) -> PhpResult<Zval> {
    let cfg_vec: Vec<(String, String)> = config.unwrap_or_default().into_iter().collect();
    let (cid, bytes) = protocol::build_subscribe_frame(cluster, group_id, topics, cfg_vec)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 POLL_REQ。
#[php_function]
pub fn hi_kafka_encode_poll_frame(
    subscription_id: i64,
    max_messages: i64,
    timeout_ms: i64,
) -> PhpResult<Zval> {
    let (cid, bytes) = protocol::build_poll_frame(
        subscription_id as u64,
        max_messages.max(1) as u32,
        timeout_ms.max(0) as u32,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 COMMIT_REQ。
#[php_function]
pub fn hi_kafka_encode_commit_frame(subscription_id: i64) -> PhpResult<Zval> {
    let (cid, bytes) = protocol::build_commit_frame(subscription_id as u64)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 UNSUBSCRIBE（无响应，cid=0）。
#[php_function]
pub fn hi_kafka_encode_unsubscribe_frame(subscription_id: i64) -> PhpResult<Binary<u8>> {
    let bytes = protocol::build_unsubscribe_frame(subscription_id as u64)
        .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(Binary::from(bytes))
}

/// 编一帧 REGISTER_CLUSTER_REQ。
#[php_function]
pub fn hi_kafka_encode_register_cluster_frame(
    cluster: &str,
    config: std::collections::HashMap<String, String>,
) -> PhpResult<Zval> {
    let cfg_vec: Vec<(String, String)> = config.into_iter().collect();
    let (cid, bytes) = protocol::build_register_cluster_frame(cluster, cfg_vec)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 解析任意 consumer 响应帧（SUBSCRIBE_RESP / POLL_RESP / COMMIT_RESP），按 kind 分发。
///
/// 返回结构：
/// - SubscribeResp Ok：`['kind' => 'subscribe', 'cid' => int, 'ok' => true, 'subscription_id' => int]`
/// - SubscribeResp Err：`['kind' => 'subscribe', 'cid' => int, 'ok' => false, 'message' => str]`
/// - PollResp Ok：`['kind' => 'poll', 'cid' => int, 'ok' => true, 'messages' => array]`
/// - PollResp Err：`['kind' => 'poll', 'cid' => int, 'ok' => false, 'message' => str]`
/// - CommitResp Ok：`['kind' => 'commit', 'cid' => int, 'ok' => true]`
/// - CommitResp Err：`['kind' => 'commit', 'cid' => int, 'ok' => false, 'message' => str]`
#[php_function]
pub fn hi_kafka_decode_consumer_resp(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let parsed = protocol::parse_consumer_resp_frame(&bytes)
        .map_err(|e| PhpException::default(e.to_string()))?;

    let mut ht = ZendHashTable::new();
    match parsed {
        protocol::ConsumerResp::SubscribeOk {
            cid,
            subscription_id,
        } => {
            put(&mut ht, "kind", "subscribe")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
            put(&mut ht, "subscription_id", subscription_id as i64)?;
        }
        protocol::ConsumerResp::SubscribeErr { cid, message } => {
            put(&mut ht, "kind", "subscribe")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::PollOk { cid, messages } => {
            put(&mut ht, "kind", "poll")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
            let msgs_zval = messages_to_zval(messages)?;
            ht.insert("messages", msgs_zval)
                .map_err(|e| PhpException::default(format!("messages: {e}")))?;
        }
        protocol::ConsumerResp::PollErr { cid, message } => {
            put(&mut ht, "kind", "poll")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::CommitOk { cid } => {
            put(&mut ht, "kind", "commit")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::CommitErr { cid, message } => {
            put(&mut ht, "kind", "commit")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::RegisterClusterOk { cid } => {
            put(&mut ht, "kind", "register_cluster")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::RegisterClusterErr { cid, message } => {
            put(&mut ht, "kind", "register_cluster")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        // === Phase 3.x RESPs ===
        protocol::ConsumerResp::PauseResumeOk { cid } => {
            put(&mut ht, "kind", "pause_resume")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::PauseResumeErr { cid, message } => {
            put(&mut ht, "kind", "pause_resume")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::SeekOk { cid } => {
            put(&mut ht, "kind", "seek")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::SeekErr { cid, message } => {
            put(&mut ht, "kind", "seek")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::TxnOk { cid } => {
            put(&mut ht, "kind", "txn")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::TxnErr { cid, message } => {
            put(&mut ht, "kind", "txn")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::SendOffsetsOk { cid } => {
            put(&mut ht, "kind", "send_offsets")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::SendOffsetsErr { cid, message } => {
            put(&mut ht, "kind", "send_offsets")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::SetOAuthTokenOk { cid } => {
            put(&mut ht, "kind", "set_oauth_token")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::SetOAuthTokenErr { cid, message } => {
            put(&mut ht, "kind", "set_oauth_token")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::PollRebalanceOk { cid, events } => {
            put(&mut ht, "kind", "poll_rebalance")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
            let events_zval = rebalance_events_to_zval(events)?;
            ht.insert("events", events_zval)
                .map_err(|e| PhpException::default(format!("events: {e}")))?;
        }
        protocol::ConsumerResp::PollRebalanceErr { cid, message } => {
            put(&mut ht, "kind", "poll_rebalance")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
    }
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

// === Phase 3.x REQ encoders 暴露给 PHP（给 SwooleClient/SwowClient driver 用）===

/// 编一帧 PAUSE_RESUME_REQ。`$op` 0=Pause / 1=Resume；`$topics` / `$partitions`
/// 是平行数组（同长度），空 = 应用到当前 assignment 全部。
#[php_function]
pub fn hi_kafka_encode_pause_resume_frame(
    subscription_id: i64,
    op: i64,
    topics: Vec<String>,
    partitions: Vec<i64>,
) -> PhpResult<Zval> {
    let parts = build_partition_specs(topics, partitions).map_err(PhpException::default)?;
    let op = match op {
        0 => hi_kafka_proto::PauseResumeOp::Pause,
        1 => hi_kafka_proto::PauseResumeOp::Resume,
        n => {
            return Err(PhpException::default(format!(
                "invalid op {n} (0=Pause, 1=Resume)"
            )))
        }
    };
    let (cid, bytes) = protocol::build_pause_resume_frame(subscription_id as u64, op, parts)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 SEEK_REQ（按 offset 模式）。三个平行数组同长度。
#[php_function]
pub fn hi_kafka_encode_seek_by_offset_frame(
    subscription_id: i64,
    topics: Vec<String>,
    partitions: Vec<i64>,
    offsets: Vec<i64>,
) -> PhpResult<Zval> {
    let targets =
        build_offset_targets(topics, partitions, offsets).map_err(PhpException::default)?;
    let (cid, bytes) = protocol::build_seek_by_offset_frame(subscription_id as u64, targets)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 SEEK_REQ（按 timestamp 模式）。`$topics` / `$partitions` 同长度，
/// 均空 = 应用到当前 assignment 全部分区。
#[php_function]
pub fn hi_kafka_encode_seek_by_timestamp_frame(
    subscription_id: i64,
    timestamp_ms: i64,
    topics: Vec<String>,
    partitions: Vec<i64>,
) -> PhpResult<Zval> {
    let parts = build_partition_specs(topics, partitions).map_err(PhpException::default)?;
    let (cid, bytes) =
        protocol::build_seek_by_timestamp_frame(subscription_id as u64, timestamp_ms, parts)
            .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 TXN_REQ。`$op` 0=Begin / 1=Commit / 2=Abort。
#[php_function]
pub fn hi_kafka_encode_txn_frame(cluster: &str, op: i64) -> PhpResult<Zval> {
    let op = match op {
        0 => hi_kafka_proto::TxnOp::Begin,
        1 => hi_kafka_proto::TxnOp::Commit,
        2 => hi_kafka_proto::TxnOp::Abort,
        n => return Err(PhpException::default(format!("invalid op {n} (0/1/2)"))),
    };
    let (cid, bytes) =
        protocol::build_txn_frame(cluster, op).map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 SEND_OFFSETS_REQ（EOS）。三个平行数组同长度。
#[php_function]
pub fn hi_kafka_encode_send_offsets_frame(
    producer_cluster: &str,
    subscription_id: i64,
    group_id: &str,
    topics: Vec<String>,
    partitions: Vec<i64>,
    offsets: Vec<i64>,
) -> PhpResult<Zval> {
    let offsets =
        build_offset_targets(topics, partitions, offsets).map_err(PhpException::default)?;
    let (cid, bytes) = protocol::build_send_offsets_frame(
        producer_cluster,
        subscription_id as u64,
        group_id,
        offsets,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 SET_OAUTH_BEARER_TOKEN_REQ。
#[php_function]
pub fn hi_kafka_encode_set_oauth_token_frame(
    cluster: &str,
    token: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Option<std::collections::HashMap<String, String>>,
) -> PhpResult<Zval> {
    let ext_vec: Vec<(String, String)> = extensions.unwrap_or_default().into_iter().collect();
    let (cid, bytes) =
        protocol::build_set_oauth_token_frame(cluster, token, lifetime_ms, principal_name, ext_vec)
            .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 编一帧 POLL_REBALANCE_REQ。
#[php_function]
pub fn hi_kafka_encode_poll_rebalance_frame(
    subscription_id: i64,
    max_events: i64,
) -> PhpResult<Zval> {
    let (cid, bytes) =
        protocol::build_poll_rebalance_frame(subscription_id as u64, max_events.max(1) as u32)
            .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 把（cid, frame bytes）打包成 `['cid' => int, 'frame' => binary]`。
/// 接 `Vec<u8>` 而非 `&[u8]` 是为了直接 move 进 Binary，避免一次 to_vec 拷贝。
fn cid_frame_zval(cid: u64, bytes: Vec<u8>) -> PhpResult<Zval> {
    let mut ht = ZendHashTable::new();
    ht.insert("cid", cid as i64)
        .map_err(|e| PhpException::default(format!("cid: {e}")))?;
    ht.insert("frame", Binary::<u8>::from(bytes))
        .map_err(|e| PhpException::default(format!("frame: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

fn put<V: IntoZval>(ht: &mut ZendHashTable, key: &str, value: V) -> PhpResult<()> {
    ht.insert(key, value)
        .map_err(|e| PhpException::default(format!("{key}: {e}")))
}

/// MINIT 钩子：注册 hi_kafka.* 几项 ini 给运维侧用。
///
/// X：必须用 `#[php_startup]` 而不是手动 `.startup_function(...)`。
/// 原因：ext-php-rs 0.13 检测到 `#[php_class]` 时会自动生成一个 startup 函数注册类；
/// 用户手动 `module.startup_function(fn)` 会覆盖那个自动 startup，导致 `Hi\Kafka\Client`
/// 这种 `#[php_class]` 类不被注册（PHP 端 `class_exists("Hi\\Kafka\\Client") === false`）。
/// 正确用法是把业务 ini 注册逻辑放进 `#[php_startup]` 标记的函数体内——宏会把它
/// **合并**进 class 注册的同一个 startup 里。
///
/// 函数体里 `module_number` 是宏内部 `fn internal(ty, module_number)` 的参数名，
/// 直接引用即可（不需要在签名里声明）。
///
/// **顺序约束**：`client_interface::register()` 必须在 `#[php_class]` 类注册之前
/// 完成——`Hi\Kafka\Client` 的 `#[implements(crate::client_interface::get_ce())]`
/// 在 ext-php-rs 自动生成的 class 注册块里 evaluate 那个表达式，此时 CE 必须 ready。
/// 用 `#[php_startup(before)]` 让本函数体在 class 注册**之前**跑。
#[php_startup(before)]
fn module_startup() {
    client_interface::register();
    ini_config::register(module_number);
}

/// MSHUTDOWN 钩子：PHP 进程退出时主动通知所用过的 worker 自退（见 [`lifecycle`]）。
///
/// 不与 `#[php_class]` 的自动 class 注册冲突——那套逻辑挂在 module **startup**，
/// 这里只设 module **shutdown**（`module_shutdown_func` 默认 None，无被覆盖之虞）。
///
/// `extern "C"`：跨 FFI 边界 panic 是 UB，用 `catch_unwind` 把任何意外 unwind 兜在
/// Rust 侧；清理本就是 best-effort，失败无所谓。返回 PHP 约定的 SUCCESS(0)。
extern "C" fn module_shutdown(_type: i32, _module_number: i32) -> i32 {
    let _ = std::panic::catch_unwind(lifecycle::on_module_shutdown);
    0
}

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module.shutdown_function(module_shutdown)
}
