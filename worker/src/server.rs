use crate::cluster::{ClusterRegistryHandle, StoredOAuthToken};
use crate::consumer::{ConsumerError, ConsumerHandle, SubscriptionId};
use crate::error::WorkerError;
use crate::metrics::Metrics;
use crate::producer::ProducerHandle;
use crate::shutdown::ShutdownHandle;
use anyhow::Context;
use bytes::BytesMut;
use futures::FutureExt;
use hi_kafka_proto::{
    codec, encode_frame, CommitReq, CommitResp, DeliveryAck, ErrorKind, ErrorResp, FrameType,
    HelloReq, HelloResp,
    PauseResumeOp, PauseResumeReq, PauseResumeResp, PollRebalanceReq, PollRebalanceResp, PollReq,
    PollResp, ProduceFnf, ProduceReq, ProduceResp, RegisterClusterReq, RegisterClusterResp,
    SeekReq, SeekResp, SendOffsetsReq, SendOffsetsResp, SetOAuthBearerTokenReq,
    SetOAuthBearerTokenResp, SubscribeReq, SubscribeResp, TxnOp, TxnReq, TxnResp, UnsubscribeReq,
    HEADER_LEN, PROTOCOL_MAJOR,
};
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

pub struct Server {
    listener: UnixListener,
    producer: ProducerHandle,
    consumer: ConsumerHandle,
    registry: ClusterRegistryHandle,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
    /// 无任何连接且持续空闲超过此时长 → worker 自动 drain 退出（解决主进程退出后
    /// worker 残留）。`Duration::ZERO` = 禁用（常驻，旧行为）。
    idle_timeout: Duration,
}

impl Server {
    pub async fn bind(socket: &Path) -> anyhow::Result<Self> {
        let registry = crate::cluster::ClusterRegistry::new();
        Self::bind_with(
            socket,
            crate::producer::logging(),
            crate::consumer::logging(),
            registry,
            crate::shutdown::ShutdownState::new(),
            Metrics::new(),
            Duration::ZERO,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn bind_with(
        socket: &Path,
        producer: ProducerHandle,
        consumer: ConsumerHandle,
        registry: ClusterRegistryHandle,
        shutdown: ShutdownHandle,
        metrics: Arc<Metrics>,
        idle_timeout: Duration,
    ) -> anyhow::Result<Self> {
        if socket.exists() {
            std::fs::remove_file(socket).context("remove stale socket")?;
        }
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent).context("create socket parent dir")?;
        }
        let listener = UnixListener::bind(socket).context("bind unix listener")?;
        info!(socket = %socket.display(), "listening");
        Ok(Self {
            listener,
            producer,
            consumer,
            registry,
            shutdown,
            metrics,
            idle_timeout,
        })
    }

    /// 主循环：accept → spawn connection task。task 句柄进 [`JoinSet`]，
    /// `drain_timeout` 决定 SIGTERM 后等已 ack 但尚未写回响应的连接的最大时间。
    ///
    /// 修复 P0 #1：原实现的 `tokio::select!{ server.run() vs signal }` 在
    /// 信号到来时直接 drop server future，detach 的 connection task 在 main
    /// 返回后被 runtime abort，导致已 ack 但未 PRODUCE_RESP 的 cid 永久丢失。
    /// 现在改为：信号方通过 `shutdown` 通知，server.run 自己 drain，等所有
    /// in-flight connection 把响应写回（超时强制 abort）才返回。
    pub async fn run(self, drain_timeout: Duration) -> anyhow::Result<()> {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let idle_timeout = self.idle_timeout;
        let mut last_active = tokio::time::Instant::now();
        // idle 检查间隔：idle_timeout 的 1/4，夹在 [1s, 30s]。禁用时给个无害的 30s。
        let check_interval = if idle_timeout.is_zero() {
            Duration::from_secs(30)
        } else {
            (idle_timeout / 4).clamp(Duration::from_secs(1), Duration::from_secs(30))
        };
        let mut idle_ticker = tokio::time::interval(check_interval);
        idle_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.shutdown.wait_draining() => {
                    info!(
                        in_flight = tasks.len(),
                        ?drain_timeout,
                        "server: drain signaled, waiting for in-flight connection tasks"
                    );
                    drain_tasks(&mut tasks, drain_timeout).await;
                    info!("server: drain complete");
                    return Ok(());
                }
                res = self.listener.accept() => {
                    last_active = tokio::time::Instant::now();
                    match res {
                        Ok((stream, _addr)) => {
                            debug!("client connected");
                            Metrics::inc(&self.metrics.ipc_connections_total);
                            let producer = self.producer.clone();
                            let consumer = self.consumer.clone();
                            let registry = self.registry.clone();
                            let shutdown = self.shutdown.clone();
                            let metrics = self.metrics.clone();
                            tasks.spawn(async move {
                                if let Err(e) = handle_connection(stream, producer, consumer, registry, shutdown, metrics).await {
                                    warn!(error = ?e, "connection ended with error");
                                }
                            });
                            // 机会主义清理已结束的 task，避免 JoinSet 无限增长
                            reap_finished(&mut tasks);
                        }
                        Err(e) => {
                            error!(error = ?e, "accept failed");
                        }
                    }
                }
                _ = idle_ticker.tick(), if !idle_timeout.is_zero() => {
                    // 清掉已结束的 connection task，才能准确判断"无连接"。
                    reap_finished(&mut tasks);
                    if tasks.is_empty() {
                        // 没有任何活跃 / idle 连接（客户端连接池里的连接对应一个阻塞在
                        // read 的 task；全部断开才会走到这里）。持续空闲超时则自退。
                        if last_active.elapsed() >= idle_timeout {
                            info!(?idle_timeout, "worker idle (no connections), self-terminating");
                            self.shutdown.start_draining();
                        }
                    } else {
                        // 还有 in-flight 连接 → 不算空闲，刷新计时基准。
                        last_active = tokio::time::Instant::now();
                    }
                }
            }
        }
    }
}

/// drain：等所有 in-flight connection task 自然结束，超时则全部 abort
/// 避免 SIGTERM 之后 hi-kafka 客户端永远等不到已 ack 的 PRODUCE_RESP
async fn drain_tasks(tasks: &mut JoinSet<()>, drain_timeout: Duration) {
    if tasks.is_empty() {
        return;
    }
    let deadline = tokio::time::Instant::now() + drain_timeout;
    loop {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(res)) => {
                if let Err(e) = res {
                    warn!(error = ?e, "connection task error during drain");
                }
            }
            Ok(None) => return, // 全部结束
            Err(_) => {
                warn!(
                    remaining = tasks.len(),
                    "drain timeout reached, aborting remaining connection tasks"
                );
                tasks.abort_all();
                // 等 abort 真正完成（一般立即）
                while tasks.join_next().await.is_some() {}
                return;
            }
        }
    }
}

/// P3 #8：按帧类型做"业务期望"的 payload 上限。比协议级 MAX_PAYLOAD_LEN
/// 严格，避免 16 MiB × N 连接 OOM。返回 `Err` 则连接立即关。
fn check_per_frame_limit(kind: FrameType, payload_len: u32) -> anyhow::Result<()> {
    // 单条业务消息（key + value + headers）上限：4 MiB
    // ——Kafka broker 默认 message.max.bytes 1 MiB，留 4× 余量给 headers / 大 value
    const PRODUCE_MAX: u32 = 4 * 1024 * 1024;
    // poll 一批消息回 PHP：上限 8 MiB（多条聚合）
    const POLL_RESP_MAX: u32 = 8 * 1024 * 1024;
    // 控制帧（注册集群、订阅、提交、事务、OAuth token 等）：64 KiB 足矣
    const CONTROL_MAX: u32 = 64 * 1024;

    let limit = match kind {
        FrameType::ProduceFnf | FrameType::ProduceReq | FrameType::ProduceResp => PRODUCE_MAX,
        FrameType::PollResp => POLL_RESP_MAX,
        // 其余全部按控制帧
        _ => CONTROL_MAX,
    };
    if payload_len > limit {
        anyhow::bail!(
            "frame payload {} exceeds per-type limit {} for {:?}",
            payload_len,
            limit,
            kind
        );
    }
    Ok(())
}

/// 机会主义把已结束的 task 从 JoinSet 拿出来；不阻塞。
fn reap_finished(tasks: &mut JoinSet<()>) {
    while let Some(res) = tasks.try_join_next() {
        if let Err(e) = res {
            warn!(error = ?e, "connection task ended with error");
        }
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    producer: ProducerHandle,
    consumer: ConsumerHandle,
    registry: ClusterRegistryHandle,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let mut header_buf = [0u8; HEADER_LEN];
    let mut payload_buf = BytesMut::new();
    // F: 第一帧必须是 HELLO 且 PROTOCOL_MAJOR 与 worker 自身一致；
    // 否则关连接（让客户端 read 端 EOF → pool 视为 connect 失败，重试）
    let mut handshaked = false;

    loop {
        if let Err(e) = stream.read_exact(&mut header_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("client disconnected");
                return Ok(());
            }
            return Err(e.into());
        }

        let header = codec::decode_header(&header_buf).context("decode header")?;
        Metrics::inc(&metrics.ipc_frames_total);
        // P3 #8：codec::decode_header 已检 MAX_PAYLOAD_LEN（16 MiB 协议级硬上限）。
        // 这里再按 frame 类型加一层"实际业务上限"，让恶意/错配的客户端无法
        // 在没有恶意单帧的前提下 16 MiB × N 个连接打爆 worker 内存。
        // 业务侧 ProduceReq 消息单条 > 4 MiB 是反模式（Kafka 默认 message.max.bytes 1 MiB），
        // RegisterCluster/OAuth token 等控制帧更应该几 KiB 就够。
        if let Err(e) = check_per_frame_limit(header.kind, header.payload_len) {
            warn!(cid = header.cid, kind = ?header.kind, payload_len = header.payload_len,
                  "rejecting oversized frame: {e}");
            // 把后续字节读掉以保持 stream 对齐，然后 return Err 让 connection close
            let mut sink = vec![0u8; header.payload_len as usize];
            let _ = stream.read_exact(&mut sink).await;
            return Err(e);
        }
        payload_buf.clear();
        payload_buf.resize(header.payload_len as usize, 0);
        stream
            .read_exact(&mut payload_buf)
            .await
            .context("read payload")?;

        // F: 握手——在 dispatch 业务帧前先完成
        if !handshaked {
            if header.kind != FrameType::Hello {
                warn!(
                    cid = header.cid,
                    kind = ?header.kind,
                    "first frame is not HELLO; closing connection"
                );
                anyhow::bail!("handshake required, got {:?}", header.kind);
            }
            let hello = HelloReq::decode(&payload_buf).context("decode HELLO payload")?;
            if hello.major != PROTOCOL_MAJOR {
                warn!(
                    client_major = hello.major,
                    server_major = PROTOCOL_MAJOR,
                    "HELLO major mismatch; closing connection"
                );
                anyhow::bail!(
                    "PROTOCOL_MAJOR mismatch: client {} vs server {}",
                    hello.major,
                    PROTOCOL_MAJOR
                );
            }
            let resp = HelloResp {
                major: PROTOCOL_MAJOR,
            };
            let mut resp_buf = BytesMut::new();
            resp.encode(&mut resp_buf).context("encode HELLO resp")?;
            write_frame(&mut stream, FrameType::Hello, header.cid, &resp_buf).await?;
            handshaked = true;
            debug!(cid = header.cid, "HELLO handshake ok");
            continue;
        }

        dispatch(
            &mut stream,
            header.kind,
            header.cid,
            &payload_buf,
            &producer,
            &consumer,
            &registry,
            &shutdown,
            &metrics,
        )
        .await?;
    }
}

/// I: 外层 panic 兜底。每帧 dispatch 整体 catch_unwind，panic 时返回 Err
/// 让 `handle_connection` 关连接（客户端见 EOF → with_retry 触发自愈）。
///
/// `handle_produce_req` 内部已有更精细的 catch_unwind + fallback ProduceResp::Err
/// （retryable=true 配合 idempotence 安全去重），那层会先 catch，外层只对其它
/// 10 个 handler 的 panic 生效——这些路径上 panic 多发生在协议解码或 worker
/// 内部数据结构上，broker 状态不变；at-least-once 语义下客户端重试是安全的。
async fn dispatch(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    consumer: &ConsumerHandle,
    registry: &ClusterRegistryHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    let result = AssertUnwindSafe(dispatch_inner(
        stream, kind, cid, payload, producer, consumer, registry, shutdown, metrics,
    ))
    .catch_unwind()
    .await;
    match result {
        Ok(r) => r,
        Err(panic) => {
            let msg = describe_panic(&panic);
            error!(cid, ?kind, panic = %msg, "dispatch panicked, closing connection");
            Err(anyhow::anyhow!("dispatch panic [{kind:?}]: {msg}"))
        }
    }
}

async fn dispatch_inner(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    consumer: &ConsumerHandle,
    registry: &ClusterRegistryHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    match kind {
        FrameType::Hello => {
            // 握手已经在 handle_connection 第一帧时处理完。如果握手后又收到
            // HELLO，说明客户端实现有 bug——记 warn 但不强行关连接（兼容容错）。
            warn!(cid, "spurious HELLO after handshake; ignoring");
        }
        FrameType::Ping => {
            debug!(cid, "PING received");
            write_frame(stream, FrameType::Pong, cid, &[]).await?;
        }
        FrameType::ProduceFnf => {
            handle_produce_fnf(stream, cid, payload, producer, shutdown, metrics).await?;
        }
        FrameType::ProduceReq => {
            handle_produce_req(stream, cid, payload, producer, shutdown, metrics).await?;
        }
        FrameType::SubscribeReq => {
            handle_subscribe(stream, cid, payload, consumer).await?;
        }
        FrameType::PollReq => {
            handle_poll(stream, cid, payload, consumer).await?;
        }
        FrameType::CommitReq => {
            handle_commit(stream, cid, payload, consumer).await?;
        }
        FrameType::Unsubscribe => {
            handle_unsubscribe(cid, payload, consumer).await;
        }
        FrameType::RegisterClusterReq => {
            handle_register_cluster(stream, cid, payload, registry).await?;
        }
        FrameType::TxnReq => {
            handle_txn(stream, cid, payload, producer).await?;
        }
        FrameType::PollRebalanceReq => {
            handle_poll_rebalance(stream, cid, payload, consumer).await?;
        }
        FrameType::SeekReq => {
            handle_seek(stream, cid, payload, consumer).await?;
        }
        FrameType::SendOffsetsReq => {
            handle_send_offsets(stream, cid, payload, producer, consumer).await?;
        }
        FrameType::PauseResumeReq => {
            handle_pause_resume(stream, cid, payload, consumer).await?;
        }
        FrameType::SetOAuthBearerTokenReq => {
            handle_set_oauth_token(stream, cid, payload, registry).await?;
        }
        other => {
            warn!(kind = ?other, cid, "frame type not yet implemented");
        }
    }
    Ok(())
}

async fn handle_subscribe(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match SubscribeReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    match consumer.subscribe(req).await {
        Ok(sub) => {
            let mut buf = BytesMut::new();
            SubscribeResp::Ok {
                subscription_id: sub.0,
            }
            .encode(&mut buf)
            .context("encode subscribe_resp")?;
            write_frame(stream, FrameType::SubscribeResp, cid, &buf).await
        }
        Err(e) => write_error_frame(stream, cid, consumer_err_to_resp(&e)).await,
    }
}

async fn handle_poll(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match PollReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let sub = SubscriptionId(req.subscription_id);
    match consumer.poll(sub, req.max_messages, req.timeout_ms).await {
        Ok(messages) => {
            let mut buf = BytesMut::new();
            PollResp::Ok { messages }
                .encode(&mut buf)
                .context("encode poll_resp")?;
            write_frame(stream, FrameType::PollResp, cid, &buf).await
        }
        Err(e) => write_error_frame(stream, cid, consumer_err_to_resp(&e)).await,
    }
}

async fn handle_commit(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match CommitReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let sub = SubscriptionId(req.subscription_id);
    match consumer.commit(sub).await {
        Ok(()) => {
            let mut buf = BytesMut::new();
            CommitResp::Ok.encode(&mut buf).context("encode commit_resp")?;
            write_frame(stream, FrameType::CommitResp, cid, &buf).await
        }
        Err(e) => write_error_frame(stream, cid, consumer_err_to_resp(&e)).await,
    }
}

async fn handle_seek(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match SeekReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let sub = SubscriptionId(req.subscription_id());
    let r = match req {
        SeekReq::ByOffset { targets, .. } => consumer.seek_by_offset(sub, targets).await,
        SeekReq::ByTimestamp {
            timestamp_ms,
            partitions,
            ..
        } => {
            consumer
                .seek_by_timestamp(sub, timestamp_ms, partitions)
                .await
        }
    };
    match r {
        Ok(()) => {
            let mut buf = BytesMut::new();
            SeekResp::Ok.encode(&mut buf).context("encode seek_resp")?;
            write_frame(stream, FrameType::SeekResp, cid, &buf).await
        }
        Err(e) => write_error_frame(stream, cid, consumer_err_to_resp(&e)).await,
    }
}

async fn handle_poll_rebalance(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match PollRebalanceReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let sub = SubscriptionId(req.subscription_id);
    match consumer.fetch_rebalance_events(sub, req.max_events).await {
        Ok(events) => {
            let mut buf = BytesMut::new();
            PollRebalanceResp::Ok { events }
                .encode(&mut buf)
                .context("encode poll_rebalance_resp")?;
            write_frame(stream, FrameType::PollRebalanceResp, cid, &buf).await
        }
        Err(e) => write_error_frame(stream, cid, consumer_err_to_resp(&e)).await,
    }
}

async fn handle_txn(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
) -> anyhow::Result<()> {
    let req = match TxnReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let r = match req.op {
        TxnOp::Begin => producer.begin_transaction(&req.cluster).await,
        TxnOp::Commit => producer.commit_transaction(&req.cluster).await,
        TxnOp::Abort => producer.abort_transaction(&req.cluster).await,
    };
    match r {
        Ok(()) => {
            info!(cluster = %req.cluster, op = ?req.op, "txn op ok");
            let mut buf = BytesMut::new();
            TxnResp::Ok.encode(&mut buf).context("encode txn_resp")?;
            write_frame(stream, FrameType::TxnResp, cid, &buf).await
        }
        Err(e) => {
            warn!(cluster = %req.cluster, op = ?req.op, error = ?e, "txn op failed");
            write_error_frame(stream, cid, WorkerError::from_anyhow(&e)).await
        }
    }
}

async fn handle_set_oauth_token(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    registry: &ClusterRegistryHandle,
) -> anyhow::Result<()> {
    let req = match SetOAuthBearerTokenReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let cluster = req.cluster.clone();
    let lifetime_ms = req.lifetime_ms;
    let token = StoredOAuthToken {
        token_value: req.token_value,
        lifetime_ms: req.lifetime_ms,
        principal_name: req.principal_name,
        extensions: req.extensions,
    };
    match registry.set_oauth_token(&cluster, token).await {
        Ok(()) => {
            info!(%cluster, lifetime_ms, "OAuth token updated");
            let mut buf = BytesMut::new();
            SetOAuthBearerTokenResp::Ok
                .encode(&mut buf)
                .context("encode set_oauth_token_resp")?;
            write_frame(stream, FrameType::SetOAuthBearerTokenResp, cid, &buf).await
        }
        Err(msg) => {
            warn!(%cluster, error = %msg, "set_oauth_token failed");
            // set_oauth_token 失败基本是目标 cluster 尚未注册
            write_error_frame(stream, cid, ErrorResp::new(ErrorKind::ClusterNotRegistered, msg))
                .await
        }
    }
}

async fn handle_pause_resume(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match PauseResumeReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let sub = SubscriptionId(req.subscription_id);
    let count = req.partitions.len();
    let r = match req.op {
        PauseResumeOp::Pause => consumer.pause(sub, req.partitions).await,
        PauseResumeOp::Resume => consumer.resume(sub, req.partitions).await,
    };
    match r {
        Ok(()) => {
            debug!(?sub, op = ?req.op, count, "pause/resume ok");
            let mut buf = BytesMut::new();
            PauseResumeResp::Ok
                .encode(&mut buf)
                .context("encode pause_resume_resp")?;
            write_frame(stream, FrameType::PauseResumeResp, cid, &buf).await
        }
        Err(e) => {
            warn!(?sub, op = ?req.op, error = ?e, "pause/resume failed");
            write_error_frame(stream, cid, consumer_err_to_resp(&e)).await
        }
    }
}

async fn handle_send_offsets(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    consumer: &ConsumerHandle,
) -> anyhow::Result<()> {
    let req = match SendOffsetsReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let sub = SubscriptionId(req.subscription_id);
    // 1) 拿 group_metadata（subscription 不存在 → SUBSCRIPTION_NOT_FOUND，触发自愈）
    let metadata = match consumer.group_metadata(sub).await {
        Ok(m) => m,
        Err(e) => return write_error_frame(stream, cid, consumer_err_to_resp(&e)).await,
    };
    // 2) 调 producer 把 offsets 提交进当前事务
    match producer
        .send_offsets_to_transaction(&req.producer_cluster, &req.group_id, req.offsets, metadata)
        .await
    {
        Ok(()) => {
            info!(
                cluster = %req.producer_cluster,
                group_id = %req.group_id,
                ?sub,
                "send_offsets_to_transaction ok"
            );
            let mut buf = BytesMut::new();
            SendOffsetsResp::Ok
                .encode(&mut buf)
                .context("encode send_offsets_resp")?;
            write_frame(stream, FrameType::SendOffsetsResp, cid, &buf).await
        }
        Err(e) => {
            warn!(
                cluster = %req.producer_cluster,
                group_id = %req.group_id,
                ?sub,
                error = ?e,
                "send_offsets_to_transaction failed"
            );
            write_error_frame(stream, cid, WorkerError::from_anyhow(&e)).await
        }
    }
}

async fn handle_register_cluster(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    registry: &ClusterRegistryHandle,
) -> anyhow::Result<()> {
    let req = match RegisterClusterReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let cluster = req.cluster.clone();
    let config: std::collections::HashMap<_, _> = req.config.into_iter().collect();
    if !config.contains_key("bootstrap.servers") {
        return write_error_frame(
            stream,
            cid,
            ErrorResp::new(
                ErrorKind::InvalidArgument,
                format!("cluster '{cluster}': missing required key 'bootstrap.servers'"),
            ),
        )
        .await;
    }
    let added = registry.register(cluster.clone(), config).await;
    info!(%cluster, added, "cluster registered");
    let mut buf = BytesMut::new();
    RegisterClusterResp::Ok
        .encode(&mut buf)
        .context("encode register_cluster_resp")?;
    write_frame(stream, FrameType::RegisterClusterResp, cid, &buf).await
}

async fn handle_unsubscribe(cid: u64, payload: &[u8], consumer: &ConsumerHandle) {
    match UnsubscribeReq::decode(payload) {
        Ok(req) => {
            let sub = SubscriptionId(req.subscription_id);
            if let Err(e) = consumer.unsubscribe(sub).await {
                warn!(cid, ?sub, error = ?e, "unsubscribe failed");
            }
        }
        Err(e) => {
            warn!(cid, error = ?e, "unsubscribe payload decode failed");
        }
    }
}

/// P2 #9：panic guard 包裹 produce_ack + encode + write_frame。
///
/// 真实风险：produce_ack 已 ack broker（消息已 commit）后，在 encode/write 之间
/// 发生 panic → connection drop → PHP 客户端 EOF → 重试 → 重复 produce。
/// 我们 catch panic 后**尽力**写一个 server-error frame（retryable=true 让客户端
/// 重试，配合 librdkafka enable.idempotence=true 可去重）；
/// 写 frame 本身也可能 panic，那只能让客户端见 EOF 然后重试，
/// 至少日志里留下了完整 panic backtrace 便于事后定位。
async fn handle_produce_req(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    let result = AssertUnwindSafe(handle_produce_req_inner(
        stream, cid, payload, producer, shutdown, metrics,
    ))
    .catch_unwind()
    .await;
    match result {
        Ok(r) => r,
        Err(panic) => {
            let msg = describe_panic(&panic);
            error!(cid, panic = %msg, "handle_produce_req panicked, attempting fallback error frame");
            Metrics::inc(&metrics.produce_resp_err_total);
            // retryable=true：配合 librdkafka enable.idempotence 安全去重
            let err_resp = ErrorResp {
                kind: ErrorKind::Internal,
                retryable: true,
                native_code: 0,
                message: format!("server panic: {msg}"),
            };
            let _ = write_error_frame(stream, cid, err_resp).await;
            Err(anyhow::anyhow!("panic in handle_produce_req: {msg}"))
        }
    }
}

/// PRODUCE_FNF：fire-and-forget，但**分层**回错——cluster 不存在 / 参数非法 /
/// 本地 enqueue 失败等「同步可知」的前置错误回结构化 [`ErrorResp`] 给业务；
/// enqueue 成功则回轻量 ack（不等 broker delivery，故 partition/offset = -1）。
/// 真正的 broker 异步投递结果仍不保证。
async fn handle_produce_fnf(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    Metrics::inc(&metrics.produce_fnf_total);
    if shutdown.is_draining() {
        Metrics::inc(&metrics.frames_dropped_draining_total);
        Metrics::inc(&metrics.produce_fnf_failed_total);
        return write_error_frame(
            stream,
            cid,
            ErrorResp::new(ErrorKind::WorkerDraining, "worker is draining"),
        )
        .await;
    }
    let msg = match ProduceFnf::decode(payload) {
        Ok(m) => m,
        Err(e) => {
            Metrics::inc(&metrics.produce_fnf_failed_total);
            warn!(cid, error = ?e, "PRODUCE_FNF payload decode failed");
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let cluster = msg.cluster.clone();
    let topic = msg.topic.clone();
    match producer.produce_fnf(msg).await {
        Ok(()) => {
            let resp = ProduceResp::Ok(DeliveryAck {
                partition: -1,
                offset: -1,
            });
            let mut buf = BytesMut::new();
            resp.encode(&mut buf).context("encode fnf ack")?;
            write_frame(stream, FrameType::ProduceResp, cid, &buf).await
        }
        Err(e) => {
            Metrics::inc(&metrics.produce_fnf_failed_total);
            warn!(cid, %cluster, %topic, error = ?e, "PRODUCE_FNF enqueue failed");
            write_error_frame(stream, cid, WorkerError::from_anyhow(&e)).await
        }
    }
}

fn describe_panic(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

async fn handle_produce_req_inner(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    Metrics::inc(&metrics.produce_req_total);
    if shutdown.is_draining() {
        Metrics::inc(&metrics.frames_dropped_draining_total);
        Metrics::inc(&metrics.produce_resp_err_total);
        return write_error_frame(
            stream,
            cid,
            ErrorResp::new(ErrorKind::WorkerDraining, "worker is draining"),
        )
        .await;
    }
    let msg = match ProduceReq::decode(payload) {
        Ok(m) => m,
        Err(e) => {
            warn!(cid, error = ?e, "PRODUCE_REQ payload decode failed");
            Metrics::inc(&metrics.produce_resp_err_total);
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    debug!(cid, cluster = %msg.cluster, topic = %msg.topic, "PRODUCE_REQ received");
    match producer.produce_ack(msg).await {
        Ok(resp) => {
            Metrics::inc(&metrics.produce_resp_ok_total);
            let mut resp_payload = BytesMut::new();
            resp.encode(&mut resp_payload)
                .context("encode PRODUCE_RESP")?;
            write_frame(stream, FrameType::ProduceResp, cid, &resp_payload).await
        }
        Err(e) => {
            warn!(cid, error = ?e, "produce_ack failed");
            Metrics::inc(&metrics.produce_resp_err_total);
            write_error_frame(stream, cid, WorkerError::from_anyhow(&e)).await
        }
    }
}

async fn write_frame(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    encode_frame(kind, cid, payload, &mut buf).context("encode frame")?;
    stream.write_all(&buf).await.context("write frame")?;
    stream.flush().await.context("flush frame")?;
    Ok(())
}

/// 把一个 [`ErrorResp`] 编成 `FrameType::Error` 帧写回客户端——
/// 所有 handler 的失败统一走这里，让 PHP 侧拿到结构化 kind。
async fn write_error_frame(
    stream: &mut UnixStream,
    cid: u64,
    err: ErrorResp,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    err.encode(&mut buf).context("encode error resp")?;
    write_frame(stream, FrameType::Error, cid, &buf).await
}

/// 把 [`ConsumerError`] 映射成结构化 [`ErrorResp`]。
/// 关键：`NotFound → SUBSCRIPTION_NOT_FOUND`，让扩展端自愈从字符串匹配升级为 kind 判定。
fn consumer_err_to_resp(e: &ConsumerError) -> ErrorResp {
    match e {
        ConsumerError::NotFound(_) => {
            ErrorResp::new(ErrorKind::SubscriptionNotFound, e.to_string())
        }
        ConsumerError::UnknownCluster(_) => {
            ErrorResp::new(ErrorKind::ClusterNotRegistered, e.to_string())
        }
        ConsumerError::Backend(err) => WorkerError::from_anyhow(err),
    }
}
