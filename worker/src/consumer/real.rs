//! 真实 rdkafka 消费者。仅在 `kafka` feature 开启时编译。
//!
//! 每个 subscription 对应：
//! - 一个 rdkafka `StreamConsumer`
//! - 一个 tokio task，持续从 stream 拉消息塞进缓冲
//! - 一个 `Mutex<VecDeque<ConsumerMessage>>` 缓冲
//! - 一个 `Notify` 用于 poll 端 long-poll 唤醒
//!
//! poll 行为：先看缓冲，有则取走；没有则等 `Notify` 或 timeout。

use crate::cluster::{ClusterRegistryHandle, OAuthTokenSlot};
use crate::consumer::{Consumer, ConsumerError, GroupMetadataHandle, SubscriptionId};
use anyhow::Context;
use bytes::Bytes;
use dashmap::DashMap;
use hi_kafka_proto::{
    ConsumerMessage, OffsetSpec, PartitionSpec, RebalanceEvent, SubscribeReq,
};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer as _, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::message::Headers as _;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use rdkafka::{ClientConfig, Message};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

/// 单个 subscription 的 rebalance 事件队列。
type RebalanceQueue = Arc<StdMutex<VecDeque<RebalanceEvent>>>;

/// 自定义 ConsumerContext：
/// 1. 把 librdkafka 的 pre/post rebalance 回调转成事件
/// 2. 在 SASL/OAUTHBEARER 场景下，把存在 `cluster.oauth_slot` 里的 token
///    通过 `generate_oauth_token` 回调供给 librdkafka
///
/// **注意**：rdkafka 在 librdkafka 内部线程上调用这些 callback，所以这里**绝对不能** await。
/// 用 `std::sync::Mutex` 而非 tokio Mutex。
struct RebalanceCtx {
    events: RebalanceQueue,
    sub_id: SubscriptionId,
    cluster: String,
    /// per-cluster OAuth token slot；非 OAUTHBEARER 集群也持有（空 slot 不会被调用）
    oauth_slot: OAuthTokenSlot,
}

impl ClientContext for RebalanceCtx {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let slot = self.oauth_slot.lock().unwrap_or_else(|e| e.into_inner());
        match slot.as_ref() {
            Some(t) => Ok(OAuthToken {
                token: t.token_value.clone(),
                principal_name: t.principal_name.clone(),
                lifetime_ms: t.lifetime_ms,
            }),
            None => Err(format!(
                "no OAuth token set for cluster '{}'; call setOAuthBearerToken first",
                self.cluster
            )
            .into()),
        }
    }
}

impl ConsumerContext for RebalanceCtx {
    fn pre_rebalance(&self, _base: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                let partitions = topic_partition_list_to_vec(tpl);
                info!(?self.sub_id, count = partitions.len(), "pre_rebalance Revoke");
                self.push(RebalanceEvent::Revoke { partitions });
            }
            Rebalance::Assign(_tpl) => {
                // pre_rebalance Assign 是 partition 即将分配（消息还没开始 fetch）；
                // post_rebalance 才确认。我们只在 post 时发 Assign 事件，避免重复。
            }
            Rebalance::Error(err) => {
                error!(?self.sub_id, error = ?err, "pre_rebalance Error");
                self.push(RebalanceEvent::Error {
                    message: format!("pre_rebalance: {err}"),
                });
            }
        }
    }

    fn post_rebalance(&self, _base: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions = topic_partition_list_to_vec(tpl);
                info!(?self.sub_id, count = partitions.len(), "post_rebalance Assign");
                self.push(RebalanceEvent::Assign { partitions });
            }
            Rebalance::Revoke(_) => {
                // 已在 pre 阶段发过 Revoke
            }
            Rebalance::Error(err) => {
                error!(?self.sub_id, error = ?err, "post_rebalance Error");
                self.push(RebalanceEvent::Error {
                    message: format!("post_rebalance: {err}"),
                });
            }
        }
    }
}

impl RebalanceCtx {
    fn push(&self, event: RebalanceEvent) {
        const MAX_QUEUE: usize = 1024;
        let mut q = self.events.lock().unwrap_or_else(|e| e.into_inner());
        if q.len() >= MAX_QUEUE {
            q.pop_front(); // 丢最旧
        }
        q.push_back(event);
    }
}

fn topic_partition_list_to_vec(tpl: &TopicPartitionList) -> Vec<(String, i32)> {
    tpl.elements()
        .into_iter()
        .map(|el| (el.topic().to_string(), el.partition()))
        .collect()
}

#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    /// 单 subscription 的内部缓冲上限（按消息条数的硬上限）
    pub buffer_capacity: usize,
    /// 自动背压：buffer.len() ≥ pause_at 时 worker 调 librdkafka pause(assignment)，
    /// 让 fetcher 停止再从 broker 拉。0 表示禁用按条数的自动背压。
    pub pause_at: usize,
    /// buffer.len() ≤ resume_at 且当前 paused → 自动 resume。
    /// 必须 < pause_at 形成 hysteresis 防抖。
    pub resume_at: usize,
    /// **P2 #4 新增**：单 subscription 缓冲的字节上限（硬上限）。
    /// 单消息可能 1 MiB+，N 个 subscription × 10K 条 = TiB 级；条数限不住内存。
    /// 0 表示禁用字节限（不推荐生产）。
    pub buffer_bytes_capacity: usize,
    /// 字节背压高水位：buffer_bytes ≥ 此值 → auto pause
    pub pause_at_bytes: usize,
    /// 字节背压低水位：buffer_bytes ≤ 此值 + 当前 paused → auto resume
    pub resume_at_bytes: usize,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        // 可通过环境变量覆盖（worker 启动时读取一次）
        let buffer_capacity = read_env_usize("HI_KAFKA_CONSUMER_BUFFER_CAPACITY", 10_000);
        let pause_at = read_env_usize(
            "HI_KAFKA_CONSUMER_PAUSE_AT",
            (buffer_capacity * 8 / 10).max(1),
        );
        let resume_at = read_env_usize(
            "HI_KAFKA_CONSUMER_RESUME_AT",
            (buffer_capacity * 2 / 10).max(1),
        );
        // 字节限默认 64 MiB / sub，pause=80%，resume=20%
        let buffer_bytes_capacity = read_env_usize(
            "HI_KAFKA_CONSUMER_BUFFER_BYTES",
            64 * 1024 * 1024,
        );
        let pause_at_bytes = read_env_usize(
            "HI_KAFKA_CONSUMER_PAUSE_AT_BYTES",
            (buffer_bytes_capacity * 8 / 10).max(1),
        );
        let resume_at_bytes = read_env_usize(
            "HI_KAFKA_CONSUMER_RESUME_AT_BYTES",
            (buffer_bytes_capacity * 2 / 10).max(1),
        );
        Self {
            buffer_capacity,
            pause_at,
            resume_at,
            buffer_bytes_capacity,
            pause_at_bytes,
            resume_at_bytes,
        }
    }
}

fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Per-subscription 背压状态，供 worker 内部检测 + 外部观测（Prometheus / 测试断言）。
#[derive(Debug, Clone)]
pub struct BackpressureStats {
    pub paused: bool,
    pub pause_total: u64,
    pub resume_total: u64,
    pub buffer_len: usize,
    pub buffer_capacity: usize,
    /// P2 #4：当前缓冲实际占用字节数（payload + headers）
    pub buffer_bytes: usize,
    pub buffer_bytes_capacity: usize,
    /// 缓冲满（无论按条还是按字节）后被丢弃的消息累计数
    pub messages_dropped_total: u64,
}

struct SubscriptionState {
    buffer: Mutex<VecDeque<ConsumerMessage>>,
    notify: Notify,
    consumer: Arc<StreamConsumer<RebalanceCtx>>,
    /// Rebalance 事件队列（与 RebalanceCtx 共享 Arc）
    rebalance_events: RebalanceQueue,
    /// 当前是否被 worker 自动 pause 了 fetcher（用户显式 pause 不进入此状态机）
    paused: AtomicBool,
    /// 自动 pause 累计触发次数
    pause_total: AtomicU64,
    /// 自动 resume 累计触发次数
    resume_total: AtomicU64,
    /// P0 #3：stream 拉取循环已经退出（broker 协议错 / 端流关 / 永久 authn 失败）。
    /// 用 poll() 检查这个标志，立即给 PHP 端返回明确错误，业务层走 virtual_id
    /// 自愈重订阅，而不是无声 timeout。
    terminated: AtomicBool,
    /// 最近一次 stream 错误的描述（带 mutex，poll 把它读回错给 PHP）。
    last_error: StdMutex<Option<String>>,
    /// P2 #4：缓冲实际占用字节数（payload + headers）。push 时 +，drain 时 -。
    buffer_bytes: AtomicUsize,
    /// 缓冲满（条数 OR 字节）时丢弃的消息累计数；接 Prometheus
    messages_dropped_total: AtomicU64,
    /// 后台拉流 task 的 handle。Drop 时 abort。
    task: JoinHandle<()>,
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct KafkaConsumer {
    registry: ClusterRegistryHandle,
    config: KafkaConsumerConfig,
    subscriptions: DashMap<SubscriptionId, Arc<SubscriptionState>>,
}

impl KafkaConsumer {
    pub fn new(registry: ClusterRegistryHandle, config: KafkaConsumerConfig) -> Self {
        Self {
            registry,
            config,
            subscriptions: DashMap::new(),
        }
    }

    /// 取某 subscription 的实时背压观测。供 Prometheus / 测试断言用。
    /// 不存在的 subscription 返回 `None`。
    pub fn backpressure_stats(&self, sub: SubscriptionId) -> Option<BackpressureStats> {
        let state = self.subscriptions.get(&sub)?;
        let buf_len = state.buffer.try_lock().map(|b| b.len()).unwrap_or(0);
        Some(BackpressureStats {
            paused: state.paused.load(Ordering::Relaxed),
            pause_total: state.pause_total.load(Ordering::Relaxed),
            resume_total: state.resume_total.load(Ordering::Relaxed),
            buffer_len: buf_len,
            buffer_capacity: self.config.buffer_capacity,
            buffer_bytes: state.buffer_bytes.load(Ordering::Relaxed),
            buffer_bytes_capacity: self.config.buffer_bytes_capacity,
            messages_dropped_total: state.messages_dropped_total.load(Ordering::Relaxed),
        })
    }

    /// 返回当前 KafkaConsumerConfig（测试 / 观测用）
    pub fn config(&self) -> &KafkaConsumerConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl Consumer for KafkaConsumer {
    /// 注：每次 subscribe 都新建 StreamConsumer，**当场**拿当前 cluster 配置。
    /// 这意味着 `registerCluster` 覆盖后已建立的 subscription 还在用老配置，
    /// 不会自动切。PHP 端在 stream_loop terminated 后会经 virtual_id 自愈
    /// 重新 subscribe，那一刻才拿到新配置（与 #7 producer cache 失效对称）。
    async fn subscribe(&self, req: SubscribeReq) -> Result<SubscriptionId, ConsumerError> {
        let cluster_cfg = self
            .registry
            .get(&req.cluster)
            .await
            .ok_or_else(|| ConsumerError::UnknownCluster(req.cluster.clone()))?;

        let mut cfg = ClientConfig::new();
        for (k, v) in &cluster_cfg {
            cfg.set(k, v);
        }
        cfg.set("group.id", &req.group_id);
        // 由扩展端 commit 控制 offset，禁用自动提交
        cfg.set("enable.auto.commit", "false");
        for (k, v) in &req.config {
            cfg.set(k, v);
        }

        let id = SubscriptionId::next();
        let rebalance_events: RebalanceQueue = Arc::new(StdMutex::new(VecDeque::new()));
        let oauth_slot = self
            .registry
            .token_slot_for(&req.cluster)
            .await
            .ok_or_else(|| ConsumerError::UnknownCluster(req.cluster.clone()))?;
        let ctx = RebalanceCtx {
            events: rebalance_events.clone(),
            sub_id: id,
            cluster: req.cluster.clone(),
            oauth_slot,
        };

        // 设 librdkafka 日志级别（可选，避免 INFO 噪音）
        cfg.set_log_level(RDKafkaLogLevel::Info);

        let consumer: StreamConsumer<RebalanceCtx> = cfg
            .create_with_context(ctx)
            .context("create StreamConsumer with RebalanceCtx")
            .map_err(ConsumerError::Backend)?;
        let topics: Vec<&str> = req.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topics)
            .context("subscribe")
            .map_err(ConsumerError::Backend)?;

        let consumer = Arc::new(consumer);

        let buffer = Mutex::new(VecDeque::new());
        let notify = Notify::new();
        let buffer_capacity = self.config.buffer_capacity;
        let pause_at = self.config.pause_at;
        let buffer_bytes_capacity = self.config.buffer_bytes_capacity;
        let pause_at_bytes = self.config.pause_at_bytes;

        let state = Arc::new_cyclic(|weak| {
            let weak = weak.clone();
            let consumer_for_task = consumer.clone();
            let task = tokio::spawn(async move {
                stream_loop(
                    consumer_for_task,
                    weak,
                    buffer_capacity,
                    pause_at,
                    buffer_bytes_capacity,
                    pause_at_bytes,
                    id,
                )
                .await;
            });
            SubscriptionState {
                buffer,
                notify,
                consumer,
                rebalance_events,
                paused: AtomicBool::new(false),
                pause_total: AtomicU64::new(0),
                resume_total: AtomicU64::new(0),
                terminated: AtomicBool::new(false),
                last_error: StdMutex::new(None),
                buffer_bytes: AtomicUsize::new(0),
                messages_dropped_total: AtomicU64::new(0),
                task,
            }
        });

        info!(
            ?id,
            cluster = %req.cluster,
            group = %req.group_id,
            topics = ?req.topics,
            "KafkaConsumer subscribed"
        );
        self.subscriptions.insert(id, state);
        Ok(id)
    }

    async fn poll(
        &self,
        sub: SubscriptionId,
        max_messages: u32,
        timeout_ms: u32,
    ) -> Result<Vec<ConsumerMessage>, ConsumerError> {
        let state = self
            .subscriptions
            .get(&sub)
            .ok_or(ConsumerError::NotFound(sub))?
            .clone();

        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
        let resume_at = self.config.resume_at;
        let resume_at_bytes = self.config.resume_at_bytes;
        loop {
            {
                let mut buf = state.buffer.lock().await;
                if !buf.is_empty() {
                    let take = (max_messages as usize).min(buf.len());
                    let out: Vec<_> = buf.drain(..take).collect();
                    // P2 #4：drain 出去多少字节，从 buffer_bytes 里扣回
                    let drained_bytes: usize = out.iter().map(message_bytes).sum();
                    state
                        .buffer_bytes
                        .fetch_sub(drained_bytes, Ordering::Relaxed);
                    let buf_len_after = buf.len();
                    let buf_bytes_after = state.buffer_bytes.load(Ordering::Relaxed);
                    drop(buf);
                    // 自动 resume：水位（按条 OR 按字节）只要任一降到低水位以下
                    // 且当前是 worker 自动 pause 状态 → 恢复 fetcher。
                    maybe_resume(
                        &state,
                        buf_len_after,
                        resume_at,
                        buf_bytes_after,
                        resume_at_bytes,
                        sub,
                    );
                    return Ok(out);
                }
            }
            // P0 #3：检测 stream_loop 是否已退出（broker 协议错 / 端流关）。
            // 缓冲已被 drain 干净时检查；有则带末次错误明确告诉 PHP 端，让
            // virtual_id 自愈路径触发重订阅，而不是 long-poll timeout 静默。
            if state.terminated.load(Ordering::Acquire) {
                let last = state
                    .last_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
                    .unwrap_or_else(|| "stream terminated".to_string());
                return Err(ConsumerError::Backend(anyhow::anyhow!(
                    "subscription {:?} terminated: {}",
                    sub,
                    last
                )));
            }
            if timeout_ms == 0 {
                return Ok(vec![]);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(vec![]);
            }
            let remaining = deadline - now;
            tokio::select! {
                _ = state.notify.notified() => {
                    // 重新读 buffer
                }
                _ = tokio::time::sleep(remaining) => {
                    return Ok(vec![]);
                }
            }
        }
    }

    async fn commit(&self, sub: SubscriptionId) -> Result<(), ConsumerError> {
        let state = self
            .subscriptions
            .get(&sub)
            .ok_or(ConsumerError::NotFound(sub))?
            .clone();
        let consumer = state.consumer.clone();
        // rdkafka commit 是同步阻塞调用，放 spawn_blocking
        tokio::task::spawn_blocking(move || {
            consumer.commit_consumer_state(CommitMode::Sync)
        })
        .await
        .context("spawn_blocking commit")
        .map_err(ConsumerError::Backend)?
        .context("commit")
        .map_err(ConsumerError::Backend)?;
        debug!(?sub, "committed");
        Ok(())
    }

    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), ConsumerError> {
        let Some((_, state)) = self.subscriptions.remove(&sub) else {
            return Err(ConsumerError::NotFound(sub));
        };
        // P1 #5：StreamConsumer 的 Drop 是同步阻塞调用（`rd_kafka_consumer_close`
        // 等队列排空 + 离群），在 tokio worker 线程上 drop 会卡住整条线程上的所有
        // future。先 abort stream_loop task（async 操作，立即完成），再把最后一个
        // strong Arc 移到 spawn_blocking 里 drop，让 Drop::drop 跑在 blocking pool。
        state.task.abort();
        tokio::task::spawn_blocking(move || drop(state))
            .await
            .context("spawn_blocking unsubscribe drop")
            .map_err(ConsumerError::Backend)?;
        info!(?sub, "unsubscribed");
        Ok(())
    }

    async fn fetch_rebalance_events(
        &self,
        sub: SubscriptionId,
        max_events: u32,
    ) -> Result<Vec<RebalanceEvent>, ConsumerError> {
        let state = self
            .subscriptions
            .get(&sub)
            .ok_or(ConsumerError::NotFound(sub))?
            .clone();
        let mut q = state
            .rebalance_events
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let take = (max_events as usize).min(q.len());
        Ok(q.drain(..take).collect())
    }

    async fn seek_by_offset(
        &self,
        sub: SubscriptionId,
        targets: Vec<OffsetSpec>,
    ) -> Result<(), ConsumerError> {
        let state = self
            .subscriptions
            .get(&sub)
            .ok_or(ConsumerError::NotFound(sub))?
            .clone();
        let consumer = state.consumer.clone();
        // rdkafka seek 是同步阻塞
        tokio::task::spawn_blocking(move || {
            for (topic, partition, offset) in targets {
                consumer
                    .seek(&topic, partition, Offset::Offset(offset), Duration::from_secs(5))
                    .with_context(|| format!("seek {topic}:{partition} → {offset}"))?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("spawn_blocking seek_by_offset")
        .map_err(ConsumerError::Backend)?
        .map_err(ConsumerError::Backend)?;
        debug!(?sub, "seek_by_offset ok");
        Ok(())
    }

    async fn pause(
        &self,
        sub: SubscriptionId,
        partitions: Vec<(String, i32)>,
    ) -> Result<(), ConsumerError> {
        flow_control(self, sub, partitions, FlowOp::Pause).await
    }

    async fn resume(
        &self,
        sub: SubscriptionId,
        partitions: Vec<(String, i32)>,
    ) -> Result<(), ConsumerError> {
        flow_control(self, sub, partitions, FlowOp::Resume).await
    }

    async fn group_metadata(
        &self,
        sub: SubscriptionId,
    ) -> Result<GroupMetadataHandle, ConsumerError> {
        let state = self
            .subscriptions
            .get(&sub)
            .ok_or(ConsumerError::NotFound(sub))?
            .clone();
        let consumer = state.consumer.clone();
        // rdkafka group_metadata 是同步 FFI 调用，不会阻塞太久，但保险起见走 spawn_blocking
        let cgm = tokio::task::spawn_blocking(move || consumer.group_metadata())
            .await
            .context("spawn_blocking group_metadata")
            .map_err(ConsumerError::Backend)?
            .ok_or_else(|| {
                ConsumerError::Backend(anyhow::anyhow!(
                    "consumer.group_metadata() returned None (not joined to group yet?)"
                ))
            })?;
        Ok(Box::new(cgm))
    }

    async fn seek_by_timestamp(
        &self,
        sub: SubscriptionId,
        timestamp_ms: i64,
        partitions: Vec<PartitionSpec>,
    ) -> Result<(), ConsumerError> {
        let state = self
            .subscriptions
            .get(&sub)
            .ok_or(ConsumerError::NotFound(sub))?
            .clone();
        let consumer = state.consumer.clone();
        tokio::task::spawn_blocking(move || {
            // 1. 构造 TPL：空 partitions → 取当前 assignment
            let mut tpl = TopicPartitionList::new();
            if partitions.is_empty() {
                let assignment = consumer
                    .assignment()
                    .context("get current assignment")?;
                for el in assignment.elements() {
                    tpl.add_partition_offset(
                        el.topic(),
                        el.partition(),
                        Offset::Offset(timestamp_ms),
                    )
                    .context("add to tpl")?;
                }
            } else {
                for (topic, partition) in partitions {
                    tpl.add_partition_offset(
                        &topic,
                        partition,
                        Offset::Offset(timestamp_ms),
                    )
                    .context("add to tpl")?;
                }
            }
            // 2. 用 offsets_for_times 解析每分区在该 timestamp 的实际 offset
            let resolved = consumer
                .offsets_for_times(tpl, Duration::from_secs(10))
                .context("offsets_for_times")?;
            // 3. seek 到解析出来的 offset
            for el in resolved.elements() {
                let offset = match el.offset() {
                    Offset::Invalid => continue,        // 没找到对应时间戳的消息
                    Offset::Offset(o) => Offset::Offset(o),
                    other => other,
                };
                consumer
                    .seek(el.topic(), el.partition(), offset, Duration::from_secs(5))
                    .with_context(|| {
                        format!(
                            "seek {}:{} after offsets_for_times",
                            el.topic(),
                            el.partition()
                        )
                    })?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("spawn_blocking seek_by_timestamp")
        .map_err(ConsumerError::Backend)?
        .map_err(ConsumerError::Backend)?;
        debug!(?sub, timestamp_ms, "seek_by_timestamp ok");
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum FlowOp {
    Pause,
    Resume,
}

async fn flow_control(
    this: &KafkaConsumer,
    sub: SubscriptionId,
    partitions: Vec<(String, i32)>,
    op: FlowOp,
) -> Result<(), ConsumerError> {
    let state = this
        .subscriptions
        .get(&sub)
        .ok_or(ConsumerError::NotFound(sub))?
        .clone();
    let consumer = state.consumer.clone();
    tokio::task::spawn_blocking(move || {
        // 1. 构造 TPL：空 partitions → 取当前 assignment
        let mut tpl = TopicPartitionList::new();
        if partitions.is_empty() {
            let assignment = consumer
                .assignment()
                .context("get current assignment")?;
            for el in assignment.elements() {
                tpl.add_partition(el.topic(), el.partition());
            }
        } else {
            for (topic, partition) in partitions {
                tpl.add_partition(&topic, partition);
            }
        }
        // 2. 调底层 pause / resume
        match op {
            FlowOp::Pause => consumer.pause(&tpl).context("pause partitions")?,
            FlowOp::Resume => consumer.resume(&tpl).context("resume partitions")?,
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("spawn_blocking flow_control")
    .map_err(ConsumerError::Backend)?
    .map_err(ConsumerError::Backend)?;
    debug!(?sub, op = ?op, "flow_control ok");
    Ok(())
}

/// 纯状态机：检查是否应该转入 paused 状态。
///
/// 返回 `true` 表示**恰好**转入 paused（false→true），调用方应触发 rdkafka pause；
/// 返回 `false` 表示水位未到 / 已经 paused，无需动作。
///
/// 仅在单测里直接调用——production 走 `maybe_pause`，那里同时管条数与字节维度。
#[cfg(test)]
fn try_transition_to_paused(
    buf_len: usize,
    pause_at: usize,
    paused: &AtomicBool,
    pause_total: &AtomicU64,
) -> bool {
    if pause_at == 0 || buf_len < pause_at {
        return false;
    }
    if paused.swap(true, Ordering::SeqCst) {
        return false; // 已经 paused
    }
    pause_total.fetch_add(1, Ordering::Relaxed);
    true
}

/// 纯状态机：检查是否应该转出 paused 状态。
///
/// 返回 `true` 表示**恰好**转出 paused（true→false），调用方应触发 rdkafka resume。
#[cfg(test)]
fn try_transition_to_resumed(
    buf_len: usize,
    resume_at: usize,
    paused: &AtomicBool,
    resume_total: &AtomicU64,
) -> bool {
    if buf_len > resume_at {
        return false;
    }
    if !paused.swap(false, Ordering::SeqCst) {
        return false; // 本来就没 paused
    }
    resume_total.fetch_add(1, Ordering::Relaxed);
    true
}

/// 单条消息占多少 byte（payload + headers）。供 buffer_bytes 计量用。
fn message_bytes(m: &ConsumerMessage) -> usize {
    m.key.len()
        + m.value.len()
        + m
            .headers
            .iter()
            .map(|(n, v)| n.len() + v.len())
            .sum::<usize>()
}

/// 自动 pause：条数 OR 字节任一超阈值即触发；任一阈值为 0 视为该维度禁用。
fn maybe_pause(
    state: &Arc<SubscriptionState>,
    buf_len: usize,
    pause_at: usize,
    buf_bytes: usize,
    pause_at_bytes: usize,
    sub: SubscriptionId,
) {
    let count_trip = pause_at > 0 && buf_len >= pause_at;
    let bytes_trip = pause_at_bytes > 0 && buf_bytes >= pause_at_bytes;
    if !count_trip && !bytes_trip {
        return;
    }
    if state.paused.swap(true, Ordering::SeqCst) {
        return; // 已 paused
    }
    state.pause_total.fetch_add(1, Ordering::Relaxed);
    let consumer = state.consumer.clone();
    tokio::task::spawn_blocking(move || {
        let assignment = match consumer.assignment() {
            Ok(a) => a,
            Err(e) => {
                warn!(?sub, error = ?e, "auto-pause: get assignment failed");
                return;
            }
        };
        if let Err(e) = consumer.pause(&assignment) {
            warn!(?sub, error = ?e, "auto-pause: consumer.pause failed");
        } else {
            info!(?sub, buf_len, buf_bytes, count_trip, bytes_trip,
                  "auto-pause: 自动暂停 fetcher");
        }
    });
}

/// 自动 resume：条数 AND 字节都降到低水位以下才触发（保守，防抖）。
fn maybe_resume(
    state: &Arc<SubscriptionState>,
    buf_len: usize,
    resume_at: usize,
    buf_bytes: usize,
    resume_at_bytes: usize,
    sub: SubscriptionId,
) {
    if buf_len > resume_at || buf_bytes > resume_at_bytes {
        return;
    }
    if !state.paused.swap(false, Ordering::SeqCst) {
        return; // 本就没 paused
    }
    state.resume_total.fetch_add(1, Ordering::Relaxed);
    let consumer = state.consumer.clone();
    tokio::task::spawn_blocking(move || {
        let assignment = match consumer.assignment() {
            Ok(a) => a,
            Err(e) => {
                warn!(?sub, error = ?e, "auto-resume: get assignment failed");
                return;
            }
        };
        if let Err(e) = consumer.resume(&assignment) {
            warn!(?sub, error = ?e, "auto-resume: consumer.resume failed");
        } else {
            info!(?sub, buf_len, buf_bytes, "auto-resume: 恢复 fetcher");
        }
    });
}

/// 标记 subscription 已终结，唤醒等待中的 poll。
fn mark_terminated(state: &Arc<SubscriptionState>, reason: String) {
    *state
        .last_error
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(reason);
    state.terminated.store(true, Ordering::Release);
    state.notify.notify_waiters();
}

/// 后台 stream 拉取循环。
///
/// 用 `Weak<SubscriptionState>` 避免循环引用——当外部 `Arc` 全部 drop 后，
/// 这里 upgrade 失败、自动退出。
///
/// P0 #3 修：
/// - Err 路径加指数退避，避免致命错（authn / 协议不兼容）形成 hot loop；
/// - None / 不可恢复错路径设 `terminated=true`，让 poll 立刻给 PHP 回错。
async fn stream_loop(
    consumer: Arc<StreamConsumer<RebalanceCtx>>,
    state: std::sync::Weak<SubscriptionState>,
    buffer_capacity: usize,
    pause_at: usize,
    buffer_bytes_capacity: usize,
    pause_at_bytes: usize,
    sub_id: SubscriptionId,
) {
    let mut stream = consumer.stream();
    // 指数退避：50ms → 100 → 200 → ... → 5s 封顶；成功一次重置
    let mut backoff_ms: u64 = 50;
    const BACKOFF_MAX_MS: u64 = 5_000;
    loop {
        let msg = match stream.next().await {
            Some(Ok(m)) => {
                backoff_ms = 50; // 成功一次复位
                m
            }
            Some(Err(e)) => {
                let kind = format!("{e}");
                let is_fatal = matches!(
                    e,
                    rdkafka::error::KafkaError::AdminOpCreation(_)
                        | rdkafka::error::KafkaError::ClientCreation(_)
                        | rdkafka::error::KafkaError::ClientConfig(_, _, _, _)
                );
                warn!(?sub_id, error = %kind, is_fatal, "kafka stream error");
                if is_fatal {
                    if let Some(s) = state.upgrade() {
                        mark_terminated(&s, format!("fatal: {kind}"));
                    }
                    return;
                }
                // 非致命：退避后再继续，避免 hot loop
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                continue;
            }
            None => {
                info!(?sub_id, "kafka stream ended, marking subscription terminated");
                if let Some(s) = state.upgrade() {
                    mark_terminated(&s, "stream ended".into());
                }
                return;
            }
        };

        let state = match state.upgrade() {
            Some(s) => s,
            None => return, // 外部已 drop
        };

        let headers = msg
            .headers()
            .map(|hdrs| {
                let mut out = Vec::with_capacity(hdrs.count());
                for i in 0..hdrs.count() {
                    let h = hdrs.get(i);
                    out.push((
                        h.key.to_string(),
                        h.value
                            .map(|v| Bytes::copy_from_slice(v))
                            .unwrap_or_default(),
                    ));
                }
                out
            })
            .unwrap_or_default();
        debug!(
            topic = %msg.topic(),
            offset = msg.offset(),
            headers_count = headers.len(),
            "consumer received message"
        );

        let cm = ConsumerMessage {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
            key: msg
                .key()
                .map(|k| Bytes::copy_from_slice(k))
                .unwrap_or_default(),
            value: msg
                .payload()
                .map(|v| Bytes::copy_from_slice(v))
                .unwrap_or_default(),
            headers,
        };

        let cm_bytes = message_bytes(&cm);
        let mut buf = state.buffer.lock().await;
        // 硬上限检查：条数 OR 字节任一超就丢最旧（兜底，正常 auto-pause 先发力）。
        let mut overflow_reason: Option<&'static str> = None;
        if buf.len() >= buffer_capacity {
            overflow_reason = Some("count");
        } else if buffer_bytes_capacity > 0
            && state.buffer_bytes.load(Ordering::Relaxed) + cm_bytes > buffer_bytes_capacity
        {
            overflow_reason = Some("bytes");
        }
        if let Some(reason) = overflow_reason {
            if let Some(dropped) = buf.pop_front() {
                let dropped_bytes = message_bytes(&dropped);
                state
                    .buffer_bytes
                    .fetch_sub(dropped_bytes, Ordering::Relaxed);
                state
                    .messages_dropped_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            error!(
                buffer_capacity,
                buffer_bytes_capacity,
                ?sub_id,
                reason,
                "consumer buffer overflow（pause_at 应配得更小），dropping oldest"
            );
        }
        buf.push_back(cm);
        state.buffer_bytes.fetch_add(cm_bytes, Ordering::Relaxed);
        let buf_len = buf.len();
        let buf_bytes = state.buffer_bytes.load(Ordering::Relaxed);
        drop(buf);
        state.notify.notify_waiters();
        // 自动 pause 检查（低开销：单次原子比较即可短路）
        maybe_pause(&state, buf_len, pause_at, buf_bytes, pause_at_bytes, sub_id);
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::*;

    #[test]
    fn pause_does_not_trigger_below_threshold() {
        let paused = AtomicBool::new(false);
        let total = AtomicU64::new(0);
        assert!(!try_transition_to_paused(99, 100, &paused, &total));
        assert!(!paused.load(Ordering::Relaxed));
        assert_eq!(total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn pause_triggers_at_threshold() {
        let paused = AtomicBool::new(false);
        let total = AtomicU64::new(0);
        assert!(try_transition_to_paused(100, 100, &paused, &total));
        assert!(paused.load(Ordering::Relaxed));
        assert_eq!(total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn pause_idempotent_when_already_paused() {
        let paused = AtomicBool::new(true);
        let total = AtomicU64::new(1);
        assert!(!try_transition_to_paused(200, 100, &paused, &total));
        // counter 不增长
        assert_eq!(total.load(Ordering::Relaxed), 1);
        assert!(paused.load(Ordering::Relaxed));
    }

    #[test]
    fn pause_zero_threshold_disables() {
        let paused = AtomicBool::new(false);
        let total = AtomicU64::new(0);
        // pause_at == 0 视为禁用
        assert!(!try_transition_to_paused(usize::MAX, 0, &paused, &total));
        assert!(!paused.load(Ordering::Relaxed));
        assert_eq!(total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn resume_only_when_currently_paused() {
        let paused = AtomicBool::new(false);
        let total = AtomicU64::new(0);
        // 没 paused 时 resume 不该触发，即使水位很低
        assert!(!try_transition_to_resumed(0, 100, &paused, &total));
        assert_eq!(total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn resume_triggers_at_low_watermark() {
        let paused = AtomicBool::new(true);
        let total = AtomicU64::new(0);
        assert!(try_transition_to_resumed(50, 100, &paused, &total));
        assert!(!paused.load(Ordering::Relaxed));
        assert_eq!(total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn resume_does_not_trigger_above_low_watermark() {
        let paused = AtomicBool::new(true);
        let total = AtomicU64::new(0);
        // buf=150 > resume_at=100 → 不该 resume
        assert!(!try_transition_to_resumed(150, 100, &paused, &total));
        assert!(paused.load(Ordering::Relaxed));
        assert_eq!(total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn full_hysteresis_cycle() {
        // 模拟一个 push → drain 循环，验证 paused 状态机迁移
        let paused = AtomicBool::new(false);
        let pause_total = AtomicU64::new(0);
        let resume_total = AtomicU64::new(0);

        // 起始：buf=0，不 pause
        assert!(!try_transition_to_paused(0, 80, &paused, &pause_total));

        // 推到 80（pause_at），触发 pause
        assert!(try_transition_to_paused(80, 80, &paused, &pause_total));
        // 继续推到 90，不重复触发
        assert!(!try_transition_to_paused(90, 80, &paused, &pause_total));

        // drain 到 50（> resume_at=20），不 resume
        assert!(!try_transition_to_resumed(50, 20, &paused, &resume_total));

        // drain 到 20，触发 resume
        assert!(try_transition_to_resumed(20, 20, &paused, &resume_total));

        // 已 resume，再次低水位不重复
        assert!(!try_transition_to_resumed(0, 20, &paused, &resume_total));

        assert_eq!(pause_total.load(Ordering::Relaxed), 1);
        assert_eq!(resume_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn default_config_reads_env() {
        // 干净环境下走默认值
        unsafe {
            std::env::remove_var("HI_KAFKA_CONSUMER_BUFFER_CAPACITY");
            std::env::remove_var("HI_KAFKA_CONSUMER_PAUSE_AT");
            std::env::remove_var("HI_KAFKA_CONSUMER_RESUME_AT");
        }
        let cfg = KafkaConsumerConfig::default();
        assert_eq!(cfg.buffer_capacity, 10_000);
        assert_eq!(cfg.pause_at, 8_000);
        assert_eq!(cfg.resume_at, 2_000);
    }
}
