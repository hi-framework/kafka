//! Kafka 消费者抽象。
//!
//! Phase 2.15：trait + LoggingConsumer（内存模拟，无 broker 也能跑）。
//! Phase 2.16：实接 rdkafka `StreamConsumer`。

use hi_kafka_proto::{ConsumerMessage, OffsetSpec, PartitionSpec, RebalanceEvent, SubscribeReq};
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Opaque consumer group metadata 句柄。具体实现下沉到 backend：
/// - `KafkaConsumer` 装 `rdkafka::consumer::ConsumerGroupMetadata`
/// - `LoggingConsumer` 返回错误（dry-run 不支持事务）
///
/// 用 `Box<dyn Any + Send + Sync>` 而不把 rdkafka 类型抬到 trait 签名，
/// 让 `LoggingProducer` 也能编译通过。
pub type GroupMetadataHandle = Box<dyn Any + Send + Sync>;

#[cfg(feature = "kafka")]
mod real;

/// 订阅 ID（由 worker 分配，全进程唯一）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

impl SubscriptionId {
    fn next() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        Self(SEQ.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("subscription {0:?} not found")]
    NotFound(SubscriptionId),

    #[error("cluster '{0}' not configured")]
    UnknownCluster(String),

    #[error("backend: {0}")]
    Backend(#[from] anyhow::Error),
}

#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    /// 创建一个订阅。返回的 subscription_id 用于后续 poll/commit/unsubscribe。
    async fn subscribe(&self, req: SubscribeReq) -> Result<SubscriptionId, ConsumerError>;

    /// 拉一批消息。`timeout_ms == 0` 视为非阻塞快照；否则 long-poll 等待。
    async fn poll(
        &self,
        sub: SubscriptionId,
        max_messages: u32,
        timeout_ms: u32,
    ) -> Result<Vec<ConsumerMessage>, ConsumerError>;

    /// 同步提交 offset（commit all currently held positions）。
    async fn commit(&self, sub: SubscriptionId) -> Result<(), ConsumerError>;

    /// 退订并释放资源。
    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), ConsumerError>;

    /// 拉取 rebalance 事件队列。最多返回 `max_events` 条；队列空时返回空 Vec。
    /// 默认实现返回空（LoggingConsumer 等不触发 rebalance 的 backend）。
    async fn fetch_rebalance_events(
        &self,
        _sub: SubscriptionId,
        _max_events: u32,
    ) -> Result<Vec<RebalanceEvent>, ConsumerError> {
        Ok(vec![])
    }

    /// 按 offset 显式 seek 到指定 (topic, partition, offset) 列表。
    /// 必须在订阅已建立且 partition 已被分配后调用。
    async fn seek_by_offset(
        &self,
        _sub: SubscriptionId,
        _targets: Vec<OffsetSpec>,
    ) -> Result<(), ConsumerError> {
        Ok(())
    }

    /// 按 timestamp seek。`partitions` 为空时应用到当前 assignment 的所有分区。
    /// 返回每分区被 seek 到的 offset（用于业务可观测）。
    async fn seek_by_timestamp(
        &self,
        _sub: SubscriptionId,
        _timestamp_ms: i64,
        _partitions: Vec<PartitionSpec>,
    ) -> Result<(), ConsumerError> {
        Ok(())
    }

    /// 暂停一组 `(topic, partition)` 的 fetch，不丢分区分配、不触发 rebalance。
    /// `partitions` 为空 → 暂停当前 assignment 的全部分区。
    async fn pause(
        &self,
        _sub: SubscriptionId,
        _partitions: Vec<(String, i32)>,
    ) -> Result<(), ConsumerError> {
        Ok(())
    }

    /// 恢复被 `pause` 暂停的分区。从上次 fetch 位置继续，不会重复消费。
    /// `partitions` 为空 → 恢复当前 assignment 的全部分区。
    async fn resume(
        &self,
        _sub: SubscriptionId,
        _partitions: Vec<(String, i32)>,
    ) -> Result<(), ConsumerError> {
        Ok(())
    }

    /// 获取该 subscription 对应 consumer 的 group metadata 句柄。
    /// 用于 `send_offsets_to_transaction`：producer 需要 consumer 的 group_metadata
    /// 才能把 offset 提交进当前事务（KIP-447）。
    ///
    /// 默认实现返回不支持错误；`KafkaConsumer` override 后返回 rdkafka 的
    /// `ConsumerGroupMetadata`，包在 `Box<dyn Any>` 里跨 trait 传递。
    async fn group_metadata(
        &self,
        _sub: SubscriptionId,
    ) -> Result<GroupMetadataHandle, ConsumerError> {
        Err(ConsumerError::Backend(anyhow::anyhow!(
            "group_metadata not supported by this consumer backend"
        )))
    }

    /// 最近 `within` 内有过活动（poll）的订阅数。worker idle 自退判定用。
    ///
    /// **为何带时间窗**：只统计「近期还在 poll」的订阅来阻止自退。否则
    /// owner 进程已死、却没 `unsubscribe` 的泄漏订阅（其 stream_loop 仍在后台
    /// 长跑、持 group 成员资格）会让计数恒 >0，worker 永远 idle 不掉、孤儿
    /// 残留。窗口由 server 传入（取 `idle_timeout`）：活跃 consumer 持续 poll →
    /// 始终在窗口内 → 受保护；泄漏订阅 `within` 内无 poll → 不再计数 → 放行自退。
    /// 默认 0（LoggingConsumer 等无真实订阅的后端）。
    fn active_subscriptions(&self, _within: Duration) -> usize {
        0
    }
}

pub type ConsumerHandle = Arc<dyn Consumer>;

// ============================================================================
// LoggingConsumer：内存模拟，dry-run 用
// ============================================================================

/// 内存模拟消费者：每次 subscribe 都成功，poll 总是返回 0 条（或可注入 fake 消息）。
pub struct LoggingConsumer {
    /// 测试可注入的固定回放消息（按 subscription 复用）
    canned_messages: Mutex<Vec<ConsumerMessage>>,
}

impl LoggingConsumer {
    pub fn new() -> Self {
        Self {
            canned_messages: Mutex::new(Vec::new()),
        }
    }

    /// 测试辅助：注入一批 fake 消息，poll 时会按序返回（取完即空）。
    #[allow(dead_code)]
    pub async fn inject(&self, msgs: Vec<ConsumerMessage>) {
        self.canned_messages.lock().await.extend(msgs);
    }
}

impl Default for LoggingConsumer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Consumer for LoggingConsumer {
    async fn subscribe(&self, req: SubscribeReq) -> Result<SubscriptionId, ConsumerError> {
        let id = SubscriptionId::next();
        info!(
            ?id,
            cluster = %req.cluster,
            group = %req.group_id,
            topics = ?req.topics,
            "[LoggingConsumer] subscribed (dry-run)"
        );
        Ok(id)
    }

    async fn poll(
        &self,
        sub: SubscriptionId,
        max_messages: u32,
        _timeout_ms: u32,
    ) -> Result<Vec<ConsumerMessage>, ConsumerError> {
        let mut buf = self.canned_messages.lock().await;
        let take = (max_messages as usize).min(buf.len());
        let out: Vec<_> = buf.drain(..take).collect();
        debug!(?sub, returned = out.len(), "[LoggingConsumer] poll");
        Ok(out)
    }

    async fn commit(&self, sub: SubscriptionId) -> Result<(), ConsumerError> {
        debug!(?sub, "[LoggingConsumer] commit (no-op)");
        Ok(())
    }

    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), ConsumerError> {
        info!(?sub, "[LoggingConsumer] unsubscribed");
        Ok(())
    }
}

pub fn logging() -> ConsumerHandle {
    Arc::new(LoggingConsumer::new())
}

#[cfg(feature = "kafka")]
pub use real::{BackpressureStats, KafkaConsumer, KafkaConsumerConfig};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn fake_req() -> SubscribeReq {
        SubscribeReq {
            cluster: "default".into(),
            group_id: "g1".into(),
            topics: vec!["t".into()],
            config: vec![],
        }
    }

    #[tokio::test]
    async fn test_subscribe_assigns_unique_id() {
        let c = LoggingConsumer::new();
        let a = c.subscribe(fake_req()).await.unwrap();
        let b = c.subscribe(fake_req()).await.unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn test_poll_returns_injected_messages() {
        let c = LoggingConsumer::new();
        c.inject(vec![ConsumerMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp_ms: 1,
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            headers: vec![],
        }])
        .await;
        let sub = c.subscribe(fake_req()).await.unwrap();
        let r = c.poll(sub, 10, 0).await.unwrap();
        assert_eq!(r.len(), 1);
    }

    #[tokio::test]
    async fn test_poll_respects_max_messages() {
        let c = LoggingConsumer::new();
        c.inject(vec![
            ConsumerMessage {
                topic: "t".into(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1,
                key: Bytes::new(),
                value: Bytes::new(),
                headers: vec![],
            };
            5
        ])
        .await;
        let sub = c.subscribe(fake_req()).await.unwrap();
        let r = c.poll(sub, 2, 0).await.unwrap();
        assert_eq!(r.len(), 2);
    }
}
