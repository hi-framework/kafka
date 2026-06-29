//! IPC 客户端：编帧、连 worker、读写。
//!
//! 把 `lib.rs` 和 `client.rs` 共享的 IPC 逻辑抽出来，避免重复。
//! Phase 2.9 起所有出向连接走 [`pool`] 复用。
//! Phase 2.12 起 worker 就绪状态由 [`worker_health`] 缓存。

use crate::cluster_replay::{self, OAuthToken};
use crate::pool;
use crate::spawn;
use crate::worker_health::{self, EnsureOutcome};
use bytes::BytesMut;
use hi_kafka_proto::{
    codec, encode_frame, CommitReq, CommitResp, ConsumerMessage, FrameType, OffsetCommit,
    OffsetSpec, PartitionSpec, PauseResumeOp, PauseResumeReq, PauseResumeResp, PollRebalanceReq,
    PollRebalanceResp, PollReq, PollResp, ProduceFnf, ProduceResp, RebalanceEvent,
    RegisterClusterReq, RegisterClusterResp, SeekReq, SeekResp, SendOffsetsReq, SendOffsetsResp,
    SetOAuthBearerTokenReq, SetOAuthBearerTokenResp, SubscribeReq, SubscribeResp, TxnOp, TxnReq,
    TxnResp, UnsubscribeReq, HEADER_LEN,
};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// === 重试统计（供 hi_kafka_retry_stats 暴露） ============================

static RETRY_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static RETRY_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static RETRY_FAILURES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
pub struct RetryStats {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
}

pub fn retry_stats() -> RetryStats {
    RetryStats {
        attempts: RETRY_ATTEMPTS.load(Ordering::Relaxed),
        successes: RETRY_SUCCESSES.load(Ordering::Relaxed),
        failures: RETRY_FAILURES.load(Ordering::Relaxed),
    }
}

static NEXT_CID: AtomicU64 = AtomicU64::new(1);

pub fn next_cid() -> u64 {
    NEXT_CID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("ensure_worker: {0}")]
    EnsureWorker(spawn::SpawnError),

    #[error("encode: {0}")]
    Encode(String),

    #[error("pool: {0}")]
    Pool(#[from] pool::PoolError),

    #[error("io: {0}")]
    Io(std::io::Error),

    #[error("decode resp: {0}")]
    DecodeResp(String),

    #[error("unexpected resp frame type: {0:?}")]
    UnexpectedKind(FrameType),

    #[error("cid mismatch: sent {sent}, got {got}")]
    CidMismatch { sent: u64, got: u64 },

    #[error("server error: {0}")]
    Server(String),
}

impl From<std::io::Error> for IpcError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// 公共前置：保证 worker 就绪。命中缓存时零开销；
/// L: 若本次是新 spawn 出的 worker（cache miss），自动 replay 已知 cluster + token
/// 让"业务调用 → worker 死 → 透明重连"链路覆盖完整状态。
///
/// pub(crate) 以便 PHP `ensureWorker()` 入口（lib.rs / client.rs）也走这条路径——
/// 业务上"显式 ensureWorker" 等价于"worker 一定 alive 且所有 cluster 已注册"。
pub(crate) fn ensure(socket: &str) -> Result<(), IpcError> {
    let outcome = worker_health::ensure(socket).map_err(IpcError::EnsureWorker)?;
    if outcome == EnsureOutcome::JustSpawned {
        replay_clusters(socket)?;
    }
    Ok(())
}

/// 把 cluster_replay 里的所有 cluster 注册重发到（可能是新起的）worker。
/// 失败会立即返回——因为重放发生在 ensure 调用栈内，让上层拿到清晰错误。
fn replay_clusters(socket: &str) -> Result<(), IpcError> {
    let snap = cluster_replay::snapshot(socket);
    if snap.is_empty() {
        return Ok(());
    }
    tracing::debug!(
        socket,
        count = snap.len(),
        "replaying cluster registrations to fresh worker"
    );
    let timeout = Duration::from_secs(5);
    for (cluster, entry) in snap {
        if !entry.config.is_empty() {
            register_cluster_once(socket, &cluster, entry.config, timeout)?;
        }
        if let Some(t) = entry.oauth_token {
            set_oauth_bearer_token_once(
                socket,
                &cluster,
                &t.token_value,
                t.lifetime_ms,
                &t.principal_name,
                t.extensions,
                timeout,
            )?;
        }
    }
    Ok(())
}

/// 判定一个错误是否暗示 worker 已死，需要让缓存失效以触发下次重启。
///
/// - `Pool(connect refused)` / `Io(BrokenPipe/ConnectionReset)`：worker 八成挂了
/// - `Encode` / `DecodeResp` / `CidMismatch`：协议层问题，worker 可能还活着
fn should_invalidate(e: &IpcError) -> bool {
    use std::io::ErrorKind;
    match e {
        IpcError::EnsureWorker(_) => false, // ensure 本身失败：缓存还没建立
        IpcError::Pool(_) => true,          // 拿不到连接，多半 worker 挂了
        IpcError::Io(io_err) => matches!(
            io_err.kind(),
            ErrorKind::BrokenPipe
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionAborted
                | ErrorKind::UnexpectedEof
        ),
        IpcError::Encode(_) | IpcError::DecodeResp(_) => false,
        IpcError::UnexpectedKind(_) | IpcError::CidMismatch { .. } => false,
        IpcError::Server(_) => false, // 业务层错误，worker 还活着
    }
}

/// 通用「带一次自动重试」包装。
///
/// 第一次失败时：
/// 1. 如果错误暗示 worker 已死（[`should_invalidate`]），让 health 缓存失效
/// 2. 重试一次——此时 `ensure_worker` 会重新探测，发现 worker 不响应就重新 fork 一个
/// 3. 第二次仍失败：返回该错误，不再重试，避免循环
///
/// 重试只针对「worker 死」类错误。业务错误（CidMismatch、Server 错误等）一次返回。
fn with_retry<F, T>(socket: &str, mut op: F) -> Result<T, IpcError>
where
    F: FnMut() -> Result<T, IpcError>,
{
    match op() {
        Ok(v) => Ok(v),
        Err(e) if should_invalidate(&e) => {
            worker_health::invalidate(socket);
            RETRY_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            match op() {
                Ok(v) => {
                    RETRY_SUCCESSES.fetch_add(1, Ordering::Relaxed);
                    Ok(v)
                }
                Err(e2) => {
                    RETRY_FAILURES.fetch_add(1, Ordering::Relaxed);
                    // 第二次仍失败：进一步 invalidate，避免下次 fast-path 直接送进死 worker
                    if should_invalidate(&e2) {
                        worker_health::invalidate(socket);
                    }
                    Err(e2)
                }
            }
        }
        Err(e) => Err(e),
    }
}

/// 通用：write 帧，遇 IO 错就 poison 连接，让池下次重建。
fn write_frame(
    conn: &mut pool::PooledConn,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    write_timeout: Option<Duration>,
) -> Result<(), IpcError> {
    let mut frame = BytesMut::new();
    encode_frame(kind, cid, payload, &mut frame).map_err(|e| IpcError::Encode(e.to_string()))?;
    let stream = conn.stream_mut();
    stream.set_write_timeout(write_timeout)?;
    if let Err(e) = stream.write_all(&frame).and_then(|_| stream.flush()) {
        conn.poison();
        return Err(e.into());
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ProduceOptions {
    pub headers: Vec<(String, bytes::Bytes)>,
    /// `-1` = 由 librdkafka 的 partitioner（key hash）决定。
    /// 显式正整数 = 强制写入该分区。
    pub partition: i32,
    /// `-1` = librdkafka 用当前时间戳。显式正整数 = 消息时间戳（毫秒）。
    pub timestamp_ms: i64,
}
// 故意不 `derive(Default)`：默认 0 会被 rdkafka 当作「写第 0 分区 + 时间戳 0」
// 而不是「auto」，是语义陷阱。要构造就显式给三个字段。

fn encode_produce_payload(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    opts: &ProduceOptions,
) -> Result<BytesMut, IpcError> {
    encode_produce_payload_bin(cluster, topic, key.as_bytes(), value.as_bytes(), opts)
}

fn encode_produce_payload_bin(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: &ProduceOptions,
) -> Result<BytesMut, IpcError> {
    let msg = ProduceFnf {
        cluster: cluster.to_string(),
        topic: topic.to_string(),
        key: bytes::Bytes::copy_from_slice(key),
        value: bytes::Bytes::copy_from_slice(value),
        partition: opts.partition,
        timestamp_ms: opts.timestamp_ms,
        headers: opts.headers.clone(),
    };
    let mut payload = BytesMut::new();
    msg.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    Ok(payload)
}

pub fn produce_fnf(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    opts: ProduceOptions,
) -> Result<(), IpcError> {
    with_retry(socket, || {
        produce_fnf_inner(socket, cluster, topic, key, value, &opts)
    })
}

fn produce_fnf_inner(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    opts: &ProduceOptions,
) -> Result<(), IpcError> {
    ensure(socket)?;
    let payload = encode_produce_payload(cluster, topic, key, value, opts)?;
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;
    write_frame(
        &mut conn,
        FrameType::ProduceFnf,
        0,
        &payload,
        Some(Duration::from_secs(1)),
    )
}

pub fn produce_sync(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    opts: ProduceOptions,
    timeout: Duration,
) -> Result<ProduceResp, IpcError> {
    with_retry(socket, || {
        produce_sync_inner(socket, cluster, topic, key, value, &opts, timeout)
    })
}

/// Binary-safe 变体：key/value 接受任意字节。供 `produceFnfBin` / `produceSyncBin` 用。
pub fn produce_fnf_bin(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: ProduceOptions,
) -> Result<(), IpcError> {
    with_retry(socket, || {
        produce_fnf_bin_inner(socket, cluster, topic, key, value, &opts)
    })
}

fn produce_fnf_bin_inner(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: &ProduceOptions,
) -> Result<(), IpcError> {
    ensure(socket)?;
    let payload = encode_produce_payload_bin(cluster, topic, key, value, opts)?;
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;
    write_frame(
        &mut conn,
        FrameType::ProduceFnf,
        0,
        &payload,
        Some(Duration::from_secs(1)),
    )
}

pub fn produce_sync_bin(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: ProduceOptions,
    timeout: Duration,
) -> Result<ProduceResp, IpcError> {
    with_retry(socket, || {
        produce_sync_bin_inner(socket, cluster, topic, key, value, &opts, timeout)
    })
}

fn produce_sync_bin_inner(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: &ProduceOptions,
    timeout: Duration,
) -> Result<ProduceResp, IpcError> {
    ensure(socket)?;
    let payload = encode_produce_payload_bin(cluster, topic, key, value, opts)?;
    let cid = next_cid();
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;

    write_frame(
        &mut conn,
        FrameType::ProduceReq,
        cid,
        &payload,
        Some(timeout),
    )?;

    let stream = conn.stream_mut();
    stream.set_read_timeout(Some(timeout))?;

    let mut header = [0u8; HEADER_LEN];
    if let Err(e) = stream.read_exact(&mut header) {
        conn.poison();
        return Err(e.into());
    }
    let h = match codec::decode_header(&header) {
        Ok(h) => h,
        Err(e) => {
            conn.poison();
            return Err(IpcError::DecodeResp(e.to_string()));
        }
    };
    if h.kind != FrameType::ProduceResp {
        conn.poison();
        return Err(IpcError::UnexpectedKind(h.kind));
    }
    if h.cid != cid {
        conn.poison();
        return Err(IpcError::CidMismatch {
            sent: cid,
            got: h.cid,
        });
    }

    let mut resp_payload = vec![0u8; h.payload_len as usize];
    if let Err(e) = conn.stream_mut().read_exact(&mut resp_payload) {
        conn.poison();
        return Err(e.into());
    }
    ProduceResp::decode(&resp_payload).map_err(|e| IpcError::DecodeResp(e.to_string()))
}

fn produce_sync_inner(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    opts: &ProduceOptions,
    timeout: Duration,
) -> Result<ProduceResp, IpcError> {
    ensure(socket)?;
    let payload = encode_produce_payload(cluster, topic, key, value, opts)?;
    let cid = next_cid();
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;

    write_frame(
        &mut conn,
        FrameType::ProduceReq,
        cid,
        &payload,
        Some(timeout),
    )?;

    let stream = conn.stream_mut();
    stream.set_read_timeout(Some(timeout))?;

    let mut header = [0u8; HEADER_LEN];
    if let Err(e) = stream.read_exact(&mut header) {
        conn.poison();
        return Err(e.into());
    }
    let h = match codec::decode_header(&header) {
        Ok(h) => h,
        Err(e) => {
            conn.poison();
            return Err(IpcError::DecodeResp(e.to_string()));
        }
    };
    if h.kind != FrameType::ProduceResp {
        conn.poison();
        return Err(IpcError::UnexpectedKind(h.kind));
    }
    if h.cid != cid {
        conn.poison();
        return Err(IpcError::CidMismatch {
            sent: cid,
            got: h.cid,
        });
    }

    let mut resp_payload = vec![0u8; h.payload_len as usize];
    if let Err(e) = conn.stream_mut().read_exact(&mut resp_payload) {
        conn.poison();
        return Err(e.into());
    }
    ProduceResp::decode(&resp_payload).map_err(|e| IpcError::DecodeResp(e.to_string()))
}

// ============================================================================
// Consumer 操作（subscribe / poll / commit / unsubscribe）
// ============================================================================

/// 通用 req/resp 流程：写一帧、读 13B header（按 cid 校验）、读 payload，返回 payload bytes。
///
/// K: 自动 with_retry 包装——所有走 round_trip 的 RPC（subscribe / poll / commit /
/// register_cluster / seek / pause_resume / txn / send_offsets / oauth / poll_rebalance）
/// 在 worker 失联时自动重试一次。第二次仍失败才返回错误。重试只针对 "worker 死"
/// 类错误（see `should_invalidate`）；业务错误（CidMismatch / Server）一次返回。
fn round_trip(
    socket: &str,
    req_kind: FrameType,
    expected_resp_kind: FrameType,
    req_payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, IpcError> {
    with_retry(socket, || {
        round_trip_once(socket, req_kind, expected_resp_kind, req_payload, timeout)
    })
}

/// round_trip 的单次执行体。retry-friendly：失败不副作用，全部清理由 PooledConn::Drop 处理。
fn round_trip_once(
    socket: &str,
    req_kind: FrameType,
    expected_resp_kind: FrameType,
    req_payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, IpcError> {
    ensure(socket)?;
    let cid = next_cid();
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;

    write_frame(&mut conn, req_kind, cid, req_payload, Some(timeout))?;

    let stream = conn.stream_mut();
    stream.set_read_timeout(Some(timeout))?;

    let mut header = [0u8; HEADER_LEN];
    if let Err(e) = stream.read_exact(&mut header) {
        conn.poison();
        return Err(e.into());
    }
    let h = match codec::decode_header(&header) {
        Ok(h) => h,
        Err(e) => {
            conn.poison();
            return Err(IpcError::DecodeResp(e.to_string()));
        }
    };
    if h.kind != expected_resp_kind {
        conn.poison();
        return Err(IpcError::UnexpectedKind(h.kind));
    }
    if h.cid != cid {
        conn.poison();
        return Err(IpcError::CidMismatch {
            sent: cid,
            got: h.cid,
        });
    }

    let mut payload = vec![0u8; h.payload_len as usize];
    if let Err(e) = conn.stream_mut().read_exact(&mut payload) {
        conn.poison();
        return Err(e.into());
    }
    Ok(payload)
}

pub fn subscribe(
    socket: &str,
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Vec<(String, String)>,
    timeout: Duration,
) -> Result<u64, IpcError> {
    let req = SubscribeReq {
        cluster: cluster.to_string(),
        group_id: group_id.to_string(),
        topics,
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;

    let resp_bytes = round_trip(
        socket,
        FrameType::SubscribeReq,
        FrameType::SubscribeResp,
        &payload,
        timeout,
    )?;
    match SubscribeResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        SubscribeResp::Ok { subscription_id } => Ok(subscription_id),
        SubscribeResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn poll(
    socket: &str,
    subscription_id: u64,
    max_messages: u32,
    timeout_ms: u32,
) -> Result<Vec<ConsumerMessage>, IpcError> {
    let req = PollReq {
        subscription_id,
        max_messages,
        timeout_ms,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    // IPC 超时 = poll timeout + 安全裕度
    let io_timeout = Duration::from_millis(timeout_ms as u64) + Duration::from_secs(2);
    let resp_bytes = round_trip(
        socket,
        FrameType::PollReq,
        FrameType::PollResp,
        &payload,
        io_timeout,
    )?;
    match PollResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        PollResp::Ok { messages } => Ok(messages),
        PollResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn commit(socket: &str, subscription_id: u64, timeout: Duration) -> Result<(), IpcError> {
    let req = CommitReq { subscription_id };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip(
        socket,
        FrameType::CommitReq,
        FrameType::CommitResp,
        &payload,
        timeout,
    )?;
    match CommitResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        CommitResp::Ok => Ok(()),
        CommitResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn register_cluster(
    socket: &str,
    cluster: &str,
    config: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    // 修：with_retry 必须包业务入口——L 拆 once 版本时漏写
    with_retry(socket, || {
        register_cluster_once(socket, cluster, config.clone(), timeout)
    })?;
    // L: 成功后入扩展端 replay 缓存——下次 worker 死亡 + spawn 时透明重放
    cluster_replay::record_cluster(socket, cluster, config);
    Ok(())
}

/// register_cluster 的"裸"版本——不更新 replay 缓存，绕开 ensure 的 replay 死循环。
/// `ensure` 内部 replay 路径 和 `register_cluster` 公共入口都调它。
/// round_trip_once 不会触发 with_retry，避免和外层 retry 嵌套。
fn register_cluster_once(
    socket: &str,
    cluster: &str,
    config: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = RegisterClusterReq {
        cluster: cluster.to_string(),
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip_once(
        socket,
        FrameType::RegisterClusterReq,
        FrameType::RegisterClusterResp,
        &payload,
        timeout,
    )?;
    match RegisterClusterResp::decode(&resp_bytes)
        .map_err(|e| IpcError::DecodeResp(e.to_string()))?
    {
        RegisterClusterResp::Ok => Ok(()),
        RegisterClusterResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn seek_by_offset(
    socket: &str,
    subscription_id: u64,
    targets: Vec<OffsetSpec>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = SeekReq::ByOffset {
        subscription_id,
        targets,
    };
    seek_round_trip(socket, req, timeout)
}

pub fn seek_by_timestamp(
    socket: &str,
    subscription_id: u64,
    timestamp_ms: i64,
    partitions: Vec<PartitionSpec>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = SeekReq::ByTimestamp {
        subscription_id,
        timestamp_ms,
        partitions,
    };
    seek_round_trip(socket, req, timeout)
}

fn seek_round_trip(socket: &str, req: SeekReq, timeout: Duration) -> Result<(), IpcError> {
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip(
        socket,
        FrameType::SeekReq,
        FrameType::SeekResp,
        &payload,
        timeout,
    )?;
    match SeekResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        SeekResp::Ok => Ok(()),
        SeekResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn poll_rebalance(
    socket: &str,
    subscription_id: u64,
    max_events: u32,
    timeout: Duration,
) -> Result<Vec<RebalanceEvent>, IpcError> {
    let req = PollRebalanceReq {
        subscription_id,
        max_events,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip(
        socket,
        FrameType::PollRebalanceReq,
        FrameType::PollRebalanceResp,
        &payload,
        timeout,
    )?;
    match PollRebalanceResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        PollRebalanceResp::Ok { events } => Ok(events),
        PollRebalanceResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn set_oauth_bearer_token(
    socket: &str,
    cluster: &str,
    token_value: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    with_retry(socket, || {
        set_oauth_bearer_token_once(
            socket,
            cluster,
            token_value,
            lifetime_ms,
            principal_name,
            extensions.clone(),
            timeout,
        )
    })?;
    cluster_replay::record_oauth_token(
        socket,
        cluster,
        OAuthToken {
            token_value: token_value.to_string(),
            lifetime_ms,
            principal_name: principal_name.to_string(),
            extensions,
        },
    );
    Ok(())
}

fn set_oauth_bearer_token_once(
    socket: &str,
    cluster: &str,
    token_value: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = SetOAuthBearerTokenReq {
        cluster: cluster.to_string(),
        token_value: token_value.to_string(),
        lifetime_ms,
        principal_name: principal_name.to_string(),
        extensions,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip_once(
        socket,
        FrameType::SetOAuthBearerTokenReq,
        FrameType::SetOAuthBearerTokenResp,
        &payload,
        timeout,
    )?;
    match SetOAuthBearerTokenResp::decode(&resp_bytes)
        .map_err(|e| IpcError::DecodeResp(e.to_string()))?
    {
        SetOAuthBearerTokenResp::Ok => Ok(()),
        SetOAuthBearerTokenResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn pause_resume(
    socket: &str,
    subscription_id: u64,
    op: PauseResumeOp,
    partitions: Vec<(String, i32)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = PauseResumeReq {
        subscription_id,
        op,
        partitions,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip(
        socket,
        FrameType::PauseResumeReq,
        FrameType::PauseResumeResp,
        &payload,
        timeout,
    )?;
    match PauseResumeResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        PauseResumeResp::Ok => Ok(()),
        PauseResumeResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn send_offsets_to_transaction(
    socket: &str,
    producer_cluster: &str,
    subscription_id: u64,
    group_id: &str,
    offsets: Vec<OffsetCommit>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = SendOffsetsReq {
        producer_cluster: producer_cluster.to_string(),
        subscription_id,
        group_id: group_id.to_string(),
        offsets,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip(
        socket,
        FrameType::SendOffsetsReq,
        FrameType::SendOffsetsResp,
        &payload,
        timeout,
    )?;
    match SendOffsetsResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        SendOffsetsResp::Ok => Ok(()),
        SendOffsetsResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn txn(socket: &str, cluster: &str, op: TxnOp, timeout: Duration) -> Result<(), IpcError> {
    let req = TxnReq {
        cluster: cluster.to_string(),
        op,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip(
        socket,
        FrameType::TxnReq,
        FrameType::TxnResp,
        &payload,
        timeout,
    )?;
    match TxnResp::decode(&resp_bytes).map_err(|e| IpcError::DecodeResp(e.to_string()))? {
        TxnResp::Ok => Ok(()),
        TxnResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn unsubscribe(socket: &str, subscription_id: u64) -> Result<(), IpcError> {
    // K: 与 round_trip 对称——unsubscribe 是 fire-and-forget 但仍走 with_retry，
    // 这样 worker 重启后第一次发被关连接也能透明恢复
    with_retry(socket, || unsubscribe_once(socket, subscription_id))
}

fn unsubscribe_once(socket: &str, subscription_id: u64) -> Result<(), IpcError> {
    ensure(socket)?;
    let req = UnsubscribeReq { subscription_id };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;
    write_frame(
        &mut conn,
        FrameType::Unsubscribe,
        0,
        &payload,
        Some(Duration::from_secs(1)),
    )
}
