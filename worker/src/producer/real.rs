//! 真实 rdkafka 生产者。仅在 `kafka` feature 开启时编译。

use crate::cluster::{ClusterRegistryHandle, OAuthTokenSlot};
use crate::consumer::GroupMetadataHandle;
use crate::producer::Producer;
use anyhow::Context;
use hi_kafka_proto::{DeliveryAck, DeliveryErr, OffsetCommit, ProduceFnf, ProduceResp};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header as RdHeader, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::ClientConfig;
use rdkafka::Offset;

/// Producer 端的自定义 ClientContext，只为提供 OAuth token 回调。
/// 不持有任何 producer 相关状态——FutureProducer 自己包装我们这个 ctx
/// 来挂 delivery callback，这里只补 oauth 一项。
struct ProducerOAuthCtx {
    cluster: String,
    oauth_slot: OAuthTokenSlot,
}

impl ClientContext for ProducerOAuthCtx {
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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::{debug, info, warn};

/// FutureProducer 必须以 cluster 名 + token slot 为 ctx 创建，
/// 类型别名让 ClusterEntry / Producer impl 引用更简洁。
type OAuthFutureProducer = FutureProducer<ProducerOAuthCtx>;

/// 单 cluster 的状态：producer + 事务互斥锁。
struct ClusterEntry {
    producer: Arc<OAuthFutureProducer>,
    /// 是否启用事务（cluster 配置含 `transactional.id`）
    transactional: bool,
    /// 事务操作串行（同集群同时仅一个 active tx）
    txn_lock: Arc<Mutex<()>>,
    /// 创建时记录的 ClusterRegistry 版本号；register 覆盖后会比对，stale 触发 rebuild
    config_version: u64,
}

/// 每个 cluster 一个 OnceCell + 当前 config_version。
/// OnceCell 保证并发 produce_fnf/ack 只触发一次 `create_with_context` + `init_transactions`。
/// 当 ClusterRegistry 的 version 比 cell 里的 entry.config_version 新 → 把 cell 整体替换。
struct ClusterCell {
    cell: OnceCell<Arc<ClusterEntry>>,
}

impl ClusterCell {
    fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }
}

pub struct KafkaProducer {
    registry: ClusterRegistryHandle,
    /// 每个 cluster 名一个 cell。外层 RwLock 仅保护 HashMap 的 entry-set 操作，
    /// 真正的 producer 初始化由内层 OnceCell 串行（不阻塞其它 cluster）。
    producers: RwLock<HashMap<String, Arc<ClusterCell>>>,
}

impl KafkaProducer {
    pub fn new(registry: ClusterRegistryHandle) -> Self {
        Self {
            registry,
            producers: RwLock::new(HashMap::new()),
        }
    }

    /// 拿到或建一个 cluster cell（外层 RwLock，快路径走读锁）。
    async fn cell_for(&self, cluster: &str) -> Arc<ClusterCell> {
        if let Some(c) = self.producers.read().await.get(cluster) {
            return c.clone();
        }
        let mut guard = self.producers.write().await;
        guard
            .entry(cluster.to_string())
            .or_insert_with(|| Arc::new(ClusterCell::new()))
            .clone()
    }

    /// P1 #6 修：用 tokio `OnceCell` per cluster 保证并发 produce 时只会触发
    /// 一次 `create_with_context + init_transactions`。原先 RwLock 双检查在
    /// 中间 await 期间释放，并发能撞两次 init_transactions，事务 id fencing 出错。
    ///
    /// P1 #7：拿到 cell 后再校验 ClusterRegistry 当前 version；如果 entry 是
    /// 老 config 编出来的，丢掉重建（覆盖了 broker 地址也能即时生效）。
    async fn entry_for(&self, cluster: &str) -> anyhow::Result<Arc<ClusterEntry>> {
        loop {
            let cell = self.cell_for(cluster).await;

            // OnceCell 串行化初始化；失败不缓存，下一次重试
            let entry = cell
                .cell
                .get_or_try_init(|| async { self.build_entry(cluster).await })
                .await?
                .clone();

            // 校验 version：若 registry 比 entry 更新 → 已被 register 覆盖，
            // 把这个 cluster 的 cell 整体换掉重建
            let (_cfg, current_version) = self
                .registry
                .get_with_version(cluster)
                .await
                .with_context(|| format!("cluster '{cluster}' not registered"))?;
            if entry.config_version == current_version {
                return Ok(entry);
            }
            warn!(
                %cluster,
                entry_version = entry.config_version,
                current_version,
                "producer cache stale (config overwritten), rebuilding"
            );
            // 替换 cell，下一轮 loop 重新初始化
            let mut guard = self.producers.write().await;
            guard.insert(cluster.to_string(), Arc::new(ClusterCell::new()));
        }
    }

    /// 真正构建 ClusterEntry —— 由 OnceCell 串行化调用，同集群内只跑一次。
    async fn build_entry(&self, cluster: &str) -> anyhow::Result<Arc<ClusterEntry>> {
        let (cluster_cfg, config_version) = self
            .registry
            .get_with_version(cluster)
            .await
            .with_context(|| {
                format!("cluster '{cluster}' not registered (call registerCluster first)")
            })?;

        let transactional = cluster_cfg.contains_key("transactional.id");

        let mut cfg = ClientConfig::new();
        // 默认压缩 lz4：所有 broker 版本（≥0.8.2）都支持，速度/压缩比平衡好。
        // 业务侧若显式给了 `compression.codec` / `compression.type` 走自己的值。
        if !cluster_cfg.contains_key("compression.codec")
            && !cluster_cfg.contains_key("compression.type")
        {
            cfg.set("compression.codec", "lz4");
        }
        for (k, v) in &cluster_cfg {
            cfg.set(k, v);
        }

        let oauth_slot = self
            .registry
            .token_slot_for(cluster)
            .await
            .with_context(|| format!("token slot for cluster '{cluster}'"))?;
        let ctx = ProducerOAuthCtx {
            cluster: cluster.to_string(),
            oauth_slot,
        };
        let producer: OAuthFutureProducer = cfg
            .create_with_context(ctx)
            .with_context(|| format!("create FutureProducer for cluster '{cluster}'"))?;

        // 事务模式：必须 init_transactions（阻塞 rdkafka 调用 → spawn_blocking）
        if transactional {
            info!(%cluster, "init_transactions starting");
            let p = producer.clone();
            tokio::task::spawn_blocking(move || p.init_transactions(Duration::from_secs(30)))
                .await
                .context("spawn_blocking init_transactions")?
                .with_context(|| format!("init_transactions for cluster '{cluster}'"))?;
            info!(%cluster, "init_transactions ok");
        }

        Ok(Arc::new(ClusterEntry {
            producer: Arc::new(producer),
            transactional,
            txn_lock: Arc::new(Mutex::new(())),
            config_version,
        }))
    }

    /// 返回 producer 句柄。供 produce_fnf / produce_ack 直接用。
    async fn producer_for(&self, cluster: &str) -> anyhow::Result<Arc<OAuthFutureProducer>> {
        Ok(self.entry_for(cluster).await?.producer.clone())
    }
}

#[async_trait::async_trait]
impl Producer for KafkaProducer {
    async fn produce_fnf(&self, msg: ProduceFnf) -> anyhow::Result<()> {
        let producer = self.producer_for(&msg.cluster).await?;
        let value_slice: &[u8] = msg.value.as_ref();
        let key_slice: &[u8] = msg.key.as_ref();
        let mut record = FutureRecord::to(&msg.topic)
            .payload(value_slice)
            .key(key_slice);

        if msg.partition >= 0 {
            record = record.partition(msg.partition);
        }
        if msg.timestamp_ms >= 0 {
            record = record.timestamp(msg.timestamp_ms);
        }

        let headers = build_headers(&msg.headers);
        if let Some(h) = &headers {
            record = record.headers(h.clone());
        }

        // FNF 路径：enqueue timeout = 0 表示不阻塞 caller，队列满立即返回 QueueFull。
        // 这是 fire-and-forget 的正确语义——业务说「丢进去就行」，不关心 broker ack；
        // 但**队列满本身要让业务感知**（已经在 is_retryable 里标了 QueueFull = 可重试），
        // 否则丢消息也没人知道。
        match producer.send(record, Duration::from_secs(0)).await {
            Ok(_) => {
                debug!(cluster = %msg.cluster, topic = %msg.topic, "PRODUCE_FNF delivered");
                Ok(())
            }
            Err((err, _msg)) => {
                warn!(cluster = %msg.cluster, topic = %msg.topic, error = ?err, "PRODUCE_FNF delivery failed");
                Err(anyhow::anyhow!("rdkafka send: {err}"))
            }
        }
    }

    async fn flush(&self, timeout: Duration) -> anyhow::Result<()> {
        // 只 flush 已经初始化好的 cell（OnceCell::get 不触发 init）
        let producers: Vec<Arc<OAuthFutureProducer>> = {
            let guard = self.producers.read().await;
            guard
                .values()
                .filter_map(|cell| cell.cell.get().map(|e| e.producer.clone()))
                .collect()
        };
        for p in producers {
            let p_clone = p.clone();
            let result = tokio::task::spawn_blocking(move || p_clone.flush(timeout))
                .await
                .context("spawn_blocking flush")?;
            if let Err(e) = result {
                warn!(error = ?e, "rdkafka flush failed");
            }
        }
        Ok(())
    }

    async fn begin_transaction(&self, cluster: &str) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            anyhow::bail!("cluster '{cluster}' has no transactional.id; cannot begin transaction");
        }
        let _guard = entry.txn_lock.lock().await;
        let p = entry.producer.clone();
        tokio::task::spawn_blocking(move || p.begin_transaction())
            .await
            .context("spawn_blocking begin_transaction")?
            .with_context(|| format!("begin_transaction for cluster '{cluster}'"))?;
        debug!(%cluster, "begin_transaction ok");
        Ok(())
    }

    async fn commit_transaction(&self, cluster: &str) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            anyhow::bail!("cluster '{cluster}' is not transactional");
        }
        let p = entry.producer.clone();
        tokio::task::spawn_blocking(move || p.commit_transaction(Duration::from_secs(30)))
            .await
            .context("spawn_blocking commit_transaction")?
            .with_context(|| format!("commit_transaction for cluster '{cluster}'"))?;
        debug!(%cluster, "commit_transaction ok");
        Ok(())
    }

    async fn abort_transaction(&self, cluster: &str) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            anyhow::bail!("cluster '{cluster}' is not transactional");
        }
        let p = entry.producer.clone();
        tokio::task::spawn_blocking(move || p.abort_transaction(Duration::from_secs(30)))
            .await
            .context("spawn_blocking abort_transaction")?
            .with_context(|| format!("abort_transaction for cluster '{cluster}'"))?;
        debug!(%cluster, "abort_transaction ok");
        Ok(())
    }

    async fn send_offsets_to_transaction(
        &self,
        cluster: &str,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
        metadata: GroupMetadataHandle,
    ) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            anyhow::bail!(
                "cluster '{cluster}' has no transactional.id; send_offsets_to_transaction requires it"
            );
        }
        // 句柄解包：必须是 KafkaConsumer 装进去的 ConsumerGroupMetadata
        let cgm: Box<ConsumerGroupMetadata> = metadata.downcast().map_err(|_| {
            anyhow::anyhow!(
                "group metadata handle is not a rdkafka ConsumerGroupMetadata; \
                 consumer/producer backend mismatch"
            )
        })?;
        let cgm: ConsumerGroupMetadata = *cgm;

        // 构造 TopicPartitionList
        let mut tpl = TopicPartitionList::new();
        for (topic, partition, offset) in &offsets {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .with_context(|| format!("add {topic}:{partition} offset={offset} to tpl"))?;
        }

        let offsets_count = offsets.len();
        let p = entry.producer.clone();
        let group_id_owned = group_id.to_string();
        let cluster_owned = cluster.to_string();

        // send_offsets_to_transaction 同集群同事务串行
        let _guard = entry.txn_lock.lock().await;

        tokio::task::spawn_blocking(move || {
            p.send_offsets_to_transaction(&tpl, &cgm, Duration::from_secs(30))
        })
        .await
        .context("spawn_blocking send_offsets_to_transaction")?
        .with_context(|| {
            format!(
                "send_offsets_to_transaction cluster='{cluster_owned}' group='{group_id_owned}' \
                 offsets={offsets_count}"
            )
        })?;
        info!(%cluster, %group_id, offsets_count, "send_offsets_to_transaction ok");
        Ok(())
    }

    async fn produce_ack(&self, msg: ProduceFnf) -> anyhow::Result<ProduceResp> {
        let producer = self.producer_for(&msg.cluster).await?;
        let value_slice: &[u8] = msg.value.as_ref();
        let key_slice: &[u8] = msg.key.as_ref();
        debug!(
            cluster = %msg.cluster,
            topic = %msg.topic,
            headers_count = msg.headers.len(),
            "PRODUCE_REQ inbound"
        );
        let mut record = FutureRecord::to(&msg.topic)
            .payload(value_slice)
            .key(key_slice);

        if msg.partition >= 0 {
            record = record.partition(msg.partition);
        }
        if msg.timestamp_ms >= 0 {
            record = record.timestamp(msg.timestamp_ms);
        }

        let headers = build_headers(&msg.headers);
        if let Some(h) = &headers {
            record = record.headers(h.clone());
        }

        // P0 #2 修正：`producer.send(record, queue_timeout)` 的第二参数是
        // **enqueue 队列满时的等待时长**，不是「整体超时」。给 0 表示队列满
        // 立即返回 QueueFull，不阻塞 IPC roundtrip。.await 本身等的是 broker
        // delivery report（受 librdkafka 的 message.timeout.ms 控制，默认 5min）。
        // 注：原注释「0 表示永不超时」是反的，已修。
        match producer.send(record, Duration::from_secs(0)).await {
            Ok(delivery) => {
                let partition = delivery.partition;
                let offset = delivery.offset;
                debug!(
                    cluster = %msg.cluster,
                    topic = %msg.topic,
                    partition,
                    offset,
                    "PRODUCE_REQ delivered"
                );
                Ok(ProduceResp::Ok(DeliveryAck { partition, offset }))
            }
            Err((err, _msg)) => {
                warn!(
                    cluster = %msg.cluster,
                    topic = %msg.topic,
                    error = ?err,
                    "PRODUCE_REQ delivery failed"
                );
                Ok(ProduceResp::Err(kafka_error_to_resp(&err)))
            }
        }
    }
}

fn kafka_error_to_resp(err: &KafkaError) -> DeliveryErr {
    let (code, retryable) = match err {
        KafkaError::MessageProduction(rd_err) => (rd_err_code_to_u16(rd_err), is_retryable(rd_err)),
        _ => (u16::MAX, false),
    };
    DeliveryErr {
        code,
        message: err.to_string(),
        retryable,
    }
}

/// 把 IPC 传来的 headers 转成 rdkafka `OwnedHeaders`。
/// 返回 `None` 表示无 headers，避免对 rdkafka 发空 headers 段。
fn build_headers(headers: &[hi_kafka_proto::MessageHeader]) -> Option<OwnedHeaders> {
    if headers.is_empty() {
        return None;
    }
    let mut owned = OwnedHeaders::new_with_capacity(headers.len());
    for (name, value) in headers {
        owned = owned.insert(RdHeader {
            key: name,
            value: Some(value.as_ref()),
        });
    }
    Some(owned)
}

fn rd_err_code_to_u16(code: &RDKafkaErrorCode) -> u16 {
    // RDKafkaErrorCode 是 #[repr(i32)] 的枚举，对应 librdkafka rd_kafka_resp_err_t。
    // 大多数 broker 侧错误码是正数；负数（client-side）截断为 u16::MAX。
    let raw: i32 = *code as i32;
    if (0..=u16::MAX as i32).contains(&raw) {
        raw as u16
    } else {
        u16::MAX
    }
}

fn is_retryable(code: &RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::*;
    matches!(
        code,
        RequestTimedOut
            | NetworkException
            | LeaderNotAvailable
            | NotLeaderForPartition
            | BrokerNotAvailable
            | ReplicaNotAvailable
            | KafkaStorageError
            // P0 #2：QueueFull = rdkafka 内部发送队列满，绝对应该退避重试，
            // 不是永久失败。enqueue timeout 一到就报 QueueFull，业务侧重试即可。
            | QueueFull
            // 临时不可用类（NetworkException 已覆盖大部分）的补充：
            | RebalanceInProgress         // 群组正在 rebalance，稍后再来
            | UnknownTopicOrPartition     // 可能是 metadata 还没刷新到所有分区
            | ConcurrentTransactions // 事务客户端互相阻塞，重试可以放行
    )
}
