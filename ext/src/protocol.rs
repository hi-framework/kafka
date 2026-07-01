//! 协议编解码原语（非 PHP 入口）。
//!
//! 把字节级编/解码与 `#[php_function]` 入口解耦：本文件提供纯 Rust 函数，
//! lib.rs 里的 `#[php_function]` 调用它们做翻译。

use bytes::BytesMut;
use hi_kafka_proto::{
    codec, encode_frame, CommitReq, CommitResp, ConsumerMessage, ErrorResp, FrameType, HelloReq,
    HelloResp,
    OffsetCommit, OffsetSpec, PartitionSpec, PauseResumeOp, PauseResumeReq, PauseResumeResp,
    PayloadError, PollRebalanceReq, PollRebalanceResp, PollReq, PollResp, ProduceFnf, ProduceResp,
    RebalanceEvent, RegisterClusterReq, RegisterClusterResp, SeekReq, SeekResp, SendOffsetsReq,
    SendOffsetsResp, SetOAuthBearerTokenReq, SetOAuthBearerTokenResp, SubscribeReq, SubscribeResp,
    TxnOp, TxnReq, TxnResp, UnsubscribeReq, HEADER_LEN, PROTOCOL_MAJOR,
};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CID: AtomicU64 = AtomicU64::new(1);

pub fn next_cid() -> u64 {
    NEXT_CID.fetch_add(1, Ordering::Relaxed)
}

pub fn header_len() -> usize {
    HEADER_LEN
}

/// `Error` 帧解析结果（供 PHP 协程 driver 用）。
pub struct ParsedError {
    pub kind: u16,
    pub kind_name: &'static str,
    pub retryable: bool,
    pub native_code: i32,
    pub message: String,
}

/// 解析完整 `Error` 帧（13B header + `ErrorResp` payload）。
pub fn parse_error_frame(bytes: &[u8]) -> anyhow::Result<ParsedError> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("error frame too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let h = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode error frame header: {e}"))?;
    if h.kind != FrameType::Error {
        anyhow::bail!("expected Error frame, got {:?}", h.kind);
    }
    let need = HEADER_LEN + h.payload_len as usize;
    if bytes.len() < need {
        anyhow::bail!("error frame truncated: {} < {}", bytes.len(), need);
    }
    let er = ErrorResp::decode(&bytes[HEADER_LEN..need])
        .map_err(|e| anyhow::anyhow!("decode ErrorResp: {e}"))?;
    Ok(ParsedError {
        kind: er.kind.as_u16(),
        kind_name: er.kind.as_str(),
        retryable: er.retryable,
        native_code: er.native_code,
        message: er.message,
    })
}

// === HELLO 握手 =============================================================

/// 编一帧 HELLO 请求（payload = `[u8 PROTOCOL_MAJOR]`，cid=0）。
/// 任意新建 UDS 连接的第一帧必须是它。
pub fn build_hello_frame() -> anyhow::Result<Vec<u8>> {
    let mut payload = BytesMut::new();
    HelloReq {
        major: PROTOCOL_MAJOR,
    }
    .encode(&mut payload)
    .map_err(|e| anyhow::anyhow!("encode HELLO payload: {e}"))?;
    let mut frame = BytesMut::new();
    encode_frame(FrameType::Hello, 0, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode HELLO frame: {e}"))?;
    Ok(frame.to_vec())
}

/// 解析 HELLO RESP 完整帧（13B header + 1B payload），校验 server major。
/// 不匹配的版本返回 Err。
pub fn parse_hello_resp(bytes: &[u8]) -> anyhow::Result<()> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("HELLO RESP too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let header = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode HELLO RESP header: {e}"))?;
    if header.kind != FrameType::Hello {
        anyhow::bail!(
            "expected HELLO RESP, got {:?} (cid={})",
            header.kind,
            header.cid
        );
    }
    let need = HEADER_LEN + header.payload_len as usize;
    if bytes.len() < need {
        anyhow::bail!("HELLO RESP payload truncated: {} < {}", bytes.len(), need);
    }
    let resp = HelloResp::decode(&bytes[HEADER_LEN..need])
        .map_err(|e| anyhow::anyhow!("decode HELLO RESP: {e}"))?;
    if resp.major != PROTOCOL_MAJOR {
        anyhow::bail!(
            "PROTOCOL_MAJOR mismatch: client {} vs server {}",
            PROTOCOL_MAJOR,
            resp.major
        );
    }
    Ok(())
}

pub fn build_fnf_frame(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> anyhow::Result<Vec<u8>> {
    let payload = build_payload(cluster, topic, key, value, headers, partition, timestamp_ms)?;
    let mut buf = BytesMut::new();
    encode_frame(FrameType::ProduceFnf, 0, &payload, &mut buf)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok(buf.to_vec())
}

pub fn build_req_frame(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let payload = build_payload(cluster, topic, key, value, headers, partition, timestamp_ms)?;
    let cid = next_cid();
    let mut buf = BytesMut::new();
    encode_frame(FrameType::ProduceReq, cid, &payload, &mut buf)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, buf.to_vec()))
}

#[derive(Debug)]
pub enum ParsedFrame {
    Resp {
        cid: u64,
        resp: ProduceResp,
    },
    Other {
        #[allow(dead_code)]
        kind: FrameType,
        #[allow(dead_code)]
        cid: u64,
        #[allow(dead_code)]
        payload_len: u32,
    },
}

pub fn parse_resp_frame(bytes: &[u8]) -> anyhow::Result<ParsedFrame> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("frame too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let header = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let need = HEADER_LEN + header.payload_len as usize;
    if bytes.len() < need {
        anyhow::bail!("payload truncated: {} < {}", bytes.len(), need);
    }
    let payload = &bytes[HEADER_LEN..need];
    match header.kind {
        FrameType::ProduceResp => {
            let resp =
                ProduceResp::decode(payload).map_err(|e| anyhow::anyhow!("decode resp: {e}"))?;
            Ok(ParsedFrame::Resp {
                cid: header.cid,
                resp,
            })
        }
        other => Ok(ParsedFrame::Other {
            kind: other,
            cid: header.cid,
            payload_len: header.payload_len,
        }),
    }
}

pub struct ParsedHeader {
    pub kind_byte: u8,
    pub cid: u64,
    pub payload_len: u32,
}

pub fn parse_header_only(bytes: &[u8]) -> anyhow::Result<ParsedHeader> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("header too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let h = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    Ok(ParsedHeader {
        kind_byte: h.kind as u8,
        cid: h.cid,
        payload_len: h.payload_len,
    })
}

fn build_payload(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> Result<BytesMut, PayloadError> {
    let msg = ProduceFnf {
        cluster: cluster.to_string(),
        topic: topic.to_string(),
        key: bytes::Bytes::copy_from_slice(key),
        value: bytes::Bytes::copy_from_slice(value),
        partition,
        timestamp_ms,
        headers,
    };
    let mut buf = BytesMut::new();
    msg.encode(&mut buf)?;
    Ok(buf)
}

// ============================================================================
// Consumer 协议原语
// ============================================================================

pub fn build_subscribe_frame(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = SubscribeReq {
        cluster: cluster.to_string(),
        group_id: group_id.to_string(),
        topics,
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode SubscribeReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::SubscribeReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

pub fn build_poll_frame(
    subscription_id: u64,
    max_messages: u32,
    timeout_ms: u32,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = PollReq {
        subscription_id,
        max_messages,
        timeout_ms,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode PollReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::PollReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

pub fn build_commit_frame(subscription_id: u64) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = CommitReq { subscription_id };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode CommitReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::CommitReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

/// Goodbye 是 fire-and-forget（无 RESP、无 payload），cid 固定为 0。
/// PHP 进程退出（MSHUTDOWN）时发给所用过的 worker——见 `lifecycle`。
pub fn build_goodbye_frame() -> anyhow::Result<Vec<u8>> {
    let mut frame = BytesMut::new();
    encode_frame(FrameType::Goodbye, 0, &[], &mut frame)
        .map_err(|e| anyhow::anyhow!("encode Goodbye frame: {e}"))?;
    Ok(frame.to_vec())
}

/// Unsubscribe 是 fire-and-forget（无 RESP），cid 固定为 0。
pub fn build_unsubscribe_frame(subscription_id: u64) -> anyhow::Result<Vec<u8>> {
    let req = UnsubscribeReq { subscription_id };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode UnsubscribeReq: {e}"))?;
    let mut frame = BytesMut::new();
    encode_frame(FrameType::Unsubscribe, 0, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok(frame.to_vec())
}

pub fn build_register_cluster_frame(
    cluster: &str,
    config: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = RegisterClusterReq {
        cluster: cluster.to_string(),
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode RegisterClusterReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::RegisterClusterReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

// === Phase 3.x REQ encoders（给 Swoole/Swow driver 用） =====================

/// 编一帧 PAUSE_RESUME_REQ。`op` 为 0 (Pause) 或 1 (Resume)。
/// `partitions` 空数组 = 应用到当前 assignment 全部分区。
pub fn build_pause_resume_frame(
    subscription_id: u64,
    op: PauseResumeOp,
    partitions: Vec<(String, i32)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = PauseResumeReq {
        subscription_id,
        op,
        partitions,
    };
    encode_req(FrameType::PauseResumeReq, |b| req.encode(b))
}

/// 编一帧 SEEK_REQ（按 offset 模式）。
pub fn build_seek_by_offset_frame(
    subscription_id: u64,
    targets: Vec<OffsetSpec>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = SeekReq::ByOffset {
        subscription_id,
        targets,
    };
    encode_req(FrameType::SeekReq, |b| req.encode(b))
}

/// 编一帧 SEEK_REQ（按 timestamp 模式）。`partitions` 空 = 当前 assignment 全部。
pub fn build_seek_by_timestamp_frame(
    subscription_id: u64,
    timestamp_ms: i64,
    partitions: Vec<PartitionSpec>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = SeekReq::ByTimestamp {
        subscription_id,
        timestamp_ms,
        partitions,
    };
    encode_req(FrameType::SeekReq, |b| req.encode(b))
}

/// 编一帧 TXN_REQ。`op` 为 0 (Begin) / 1 (Commit) / 2 (Abort)。
pub fn build_txn_frame(cluster: &str, op: TxnOp) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = TxnReq {
        cluster: cluster.to_string(),
        op,
    };
    encode_req(FrameType::TxnReq, |b| req.encode(b))
}

/// 编一帧 SEND_OFFSETS_REQ（EOS stream 用，必须在 BEGIN/COMMIT 之间调）。
pub fn build_send_offsets_frame(
    producer_cluster: &str,
    subscription_id: u64,
    group_id: &str,
    offsets: Vec<OffsetCommit>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = SendOffsetsReq {
        producer_cluster: producer_cluster.to_string(),
        subscription_id,
        group_id: group_id.to_string(),
        offsets,
    };
    encode_req(FrameType::SendOffsetsReq, |b| req.encode(b))
}

/// 编一帧 SET_OAUTH_BEARER_TOKEN_REQ。
pub fn build_set_oauth_token_frame(
    cluster: &str,
    token_value: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = SetOAuthBearerTokenReq {
        cluster: cluster.to_string(),
        token_value: token_value.to_string(),
        lifetime_ms,
        principal_name: principal_name.to_string(),
        extensions,
    };
    encode_req(FrameType::SetOAuthBearerTokenReq, |b| req.encode(b))
}

/// 编一帧 POLL_REBALANCE_REQ。
pub fn build_poll_rebalance_frame(
    subscription_id: u64,
    max_events: u32,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = PollRebalanceReq {
        subscription_id,
        max_events,
    };
    encode_req(FrameType::PollRebalanceReq, |b| req.encode(b))
}

/// 共用编帧 helper——分配 cid + payload encode + frame encode。
fn encode_req<F>(kind: FrameType, encode_payload: F) -> anyhow::Result<(u64, Vec<u8>)>
where
    F: FnOnce(&mut BytesMut) -> Result<(), PayloadError>,
{
    let mut payload = BytesMut::new();
    encode_payload(&mut payload).map_err(|e| anyhow::anyhow!("encode payload {kind:?}: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(kind, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame {kind:?}: {e}"))?;
    Ok((cid, frame.to_vec()))
}

#[derive(Debug)]
pub enum ConsumerResp {
    SubscribeOk {
        cid: u64,
        subscription_id: u64,
    },
    SubscribeErr {
        cid: u64,
        message: String,
    },
    PollOk {
        cid: u64,
        messages: Vec<ConsumerMessage>,
    },
    PollErr {
        cid: u64,
        message: String,
    },
    CommitOk {
        cid: u64,
    },
    CommitErr {
        cid: u64,
        message: String,
    },
    RegisterClusterOk {
        cid: u64,
    },
    RegisterClusterErr {
        cid: u64,
        message: String,
    },
    // Phase 3.x RESP
    PauseResumeOk {
        cid: u64,
    },
    PauseResumeErr {
        cid: u64,
        message: String,
    },
    SeekOk {
        cid: u64,
    },
    SeekErr {
        cid: u64,
        message: String,
    },
    TxnOk {
        cid: u64,
    },
    TxnErr {
        cid: u64,
        message: String,
    },
    SendOffsetsOk {
        cid: u64,
    },
    SendOffsetsErr {
        cid: u64,
        message: String,
    },
    SetOAuthTokenOk {
        cid: u64,
    },
    SetOAuthTokenErr {
        cid: u64,
        message: String,
    },
    PollRebalanceOk {
        cid: u64,
        events: Vec<RebalanceEvent>,
    },
    PollRebalanceErr {
        cid: u64,
        message: String,
    },
}

pub fn parse_consumer_resp_frame(bytes: &[u8]) -> anyhow::Result<ConsumerResp> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("frame too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let header = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let need = HEADER_LEN + header.payload_len as usize;
    if bytes.len() < need {
        anyhow::bail!("payload truncated: {} < {}", bytes.len(), need);
    }
    let payload = &bytes[HEADER_LEN..need];

    Ok(match header.kind {
        FrameType::SubscribeResp => match SubscribeResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode SubscribeResp: {e}"))?
        {
            SubscribeResp::Ok { subscription_id } => ConsumerResp::SubscribeOk {
                cid: header.cid,
                subscription_id,
            },
            SubscribeResp::Err { message } => ConsumerResp::SubscribeErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::PollResp => {
            match PollResp::decode(payload).map_err(|e| anyhow::anyhow!("decode PollResp: {e}"))? {
                PollResp::Ok { messages } => ConsumerResp::PollOk {
                    cid: header.cid,
                    messages,
                },
                PollResp::Err { message } => ConsumerResp::PollErr {
                    cid: header.cid,
                    message,
                },
            }
        }
        FrameType::CommitResp => match CommitResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode CommitResp: {e}"))?
        {
            CommitResp::Ok => ConsumerResp::CommitOk { cid: header.cid },
            CommitResp::Err { message } => ConsumerResp::CommitErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::RegisterClusterResp => match RegisterClusterResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode RegisterClusterResp: {e}"))?
        {
            RegisterClusterResp::Ok => ConsumerResp::RegisterClusterOk { cid: header.cid },
            RegisterClusterResp::Err { message } => ConsumerResp::RegisterClusterErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::PauseResumeResp => match PauseResumeResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode PauseResumeResp: {e}"))?
        {
            PauseResumeResp::Ok => ConsumerResp::PauseResumeOk { cid: header.cid },
            PauseResumeResp::Err { message } => ConsumerResp::PauseResumeErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::SeekResp => {
            match SeekResp::decode(payload).map_err(|e| anyhow::anyhow!("decode SeekResp: {e}"))? {
                SeekResp::Ok => ConsumerResp::SeekOk { cid: header.cid },
                SeekResp::Err { message } => ConsumerResp::SeekErr {
                    cid: header.cid,
                    message,
                },
            }
        }
        FrameType::TxnResp => {
            match TxnResp::decode(payload).map_err(|e| anyhow::anyhow!("decode TxnResp: {e}"))? {
                TxnResp::Ok => ConsumerResp::TxnOk { cid: header.cid },
                TxnResp::Err { message } => ConsumerResp::TxnErr {
                    cid: header.cid,
                    message,
                },
            }
        }
        FrameType::SendOffsetsResp => match SendOffsetsResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode SendOffsetsResp: {e}"))?
        {
            SendOffsetsResp::Ok => ConsumerResp::SendOffsetsOk { cid: header.cid },
            SendOffsetsResp::Err { message } => ConsumerResp::SendOffsetsErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::SetOAuthBearerTokenResp => match SetOAuthBearerTokenResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode SetOAuthBearerTokenResp: {e}"))?
        {
            SetOAuthBearerTokenResp::Ok => ConsumerResp::SetOAuthTokenOk { cid: header.cid },
            SetOAuthBearerTokenResp::Err { message } => ConsumerResp::SetOAuthTokenErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::PollRebalanceResp => match PollRebalanceResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode PollRebalanceResp: {e}"))?
        {
            PollRebalanceResp::Ok { events } => ConsumerResp::PollRebalanceOk {
                cid: header.cid,
                events,
            },
            PollRebalanceResp::Err { message } => ConsumerResp::PollRebalanceErr {
                cid: header.cid,
                message,
            },
        },
        other => anyhow::bail!("unexpected consumer frame kind: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hi_kafka_proto::{DeliveryAck, DeliveryErr, ErrorKind};
    use bytes::BufMut;

    // === HELLO / Goodbye / 通用 header ==================================

    #[test]
    fn test_fnf_frame_roundtrip_via_parse_header() {
        let bytes = build_fnf_frame("c", "t", b"k", b"v", vec![], -1, -1).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::ProduceFnf as u8);
        assert_eq!(h.cid, 0);
    }

    #[test]
    fn test_req_frame_assigns_monotonic_cid() {
        let (cid1, _) = build_req_frame("c", "t", b"k", b"v", vec![], -1, -1).unwrap();
        let (cid2, _) = build_req_frame("c", "t", b"k", b"v", vec![], -1, -1).unwrap();
        assert!(cid2 > cid1);
    }

    #[test]
    fn test_hello_frame_shape() {
        let bytes = build_hello_frame().unwrap();
        // 13B header + 1B payload (PROTOCOL_MAJOR)
        assert_eq!(bytes.len(), HEADER_LEN + 1);
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::Hello as u8);
        assert_eq!(h.cid, 0, "HELLO 帧 cid 固定为 0");
        assert_eq!(h.payload_len, 1);
    }

    #[test]
    fn test_hello_resp_ok() {
        // 构造一个 major=PROTOCOL_MAJOR 的完整 HELLO RESP 帧
        let bytes = encode_test_hello_resp(PROTOCOL_MAJOR);
        assert!(parse_hello_resp(&bytes).is_ok());
    }

    #[test]
    fn test_hello_resp_version_mismatch() {
        let wrong = PROTOCOL_MAJOR.wrapping_add(1);
        let bytes = encode_test_hello_resp(wrong);
        let err = parse_hello_resp(&bytes).unwrap_err().to_string();
        assert!(err.contains("PROTOCOL_MAJOR mismatch"));
    }

    #[test]
    fn test_hello_resp_wrong_kind() {
        // 用 Ping 帧充当 HELLO RESP 应该被拒
        let mut frame = BytesMut::new();
        encode_frame(FrameType::Ping, 0, &[], &mut frame).unwrap();
        let err = parse_hello_resp(&frame).unwrap_err().to_string();
        assert!(err.contains("expected HELLO RESP"));
    }

    #[test]
    fn test_hello_resp_too_short() {
        // 只给 12 字节，不足 header 长度
        let err = parse_hello_resp(&[0u8; 12]).unwrap_err().to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn test_goodbye_frame_shape() {
        let bytes = build_goodbye_frame().unwrap();
        assert_eq!(bytes.len(), HEADER_LEN, "Goodbye 无 payload");
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::Goodbye as u8);
        assert_eq!(h.cid, 0, "Goodbye 是 fire-and-forget，cid=0");
        assert_eq!(h.payload_len, 0);
    }

    #[test]
    fn test_unsubscribe_frame_shape_and_cid() {
        let bytes = build_unsubscribe_frame(42).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::Unsubscribe as u8);
        assert_eq!(h.cid, 0, "Unsubscribe 是 fire-and-forget，cid=0");
        // payload = UnsubscribeReq encoded (subscription_id = u64)
        assert_eq!(h.payload_len as usize, 8);
    }

    #[test]
    fn test_parse_header_too_short() {
        // 12 字节 < 13 字节 header
        let err = parse_header_only(&[0u8; 12])
            .map(|_| ())
            .err()
            .expect("should error")
            .to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn test_next_cid_monotonic_batch() {
        // 连续 100 次分配，严格单调
        let mut prev = next_cid();
        for _ in 0..100 {
            let c = next_cid();
            assert!(c > prev);
            prev = c;
        }
    }

    // === Consumer REQ 编帧 ==============================================

    #[test]
    fn test_subscribe_frame_header() {
        let (cid, bytes) =
            build_subscribe_frame("c", "g", vec!["t1".into(), "t2".into()], vec![]).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::SubscribeReq as u8);
        assert_eq!(h.cid, cid);
        assert!(h.cid > 0);
    }

    #[test]
    fn test_poll_frame_header() {
        let (cid, bytes) = build_poll_frame(7, 100, 500).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::PollReq as u8);
        assert_eq!(h.cid, cid);
    }

    #[test]
    fn test_commit_frame_header() {
        let (cid, bytes) = build_commit_frame(7).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::CommitReq as u8);
        assert_eq!(h.cid, cid);
    }

    #[test]
    fn test_register_cluster_frame_header() {
        let (cid, bytes) = build_register_cluster_frame(
            "main",
            vec![("bootstrap.servers".into(), "kafka:9092".into())],
        )
        .unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::RegisterClusterReq as u8);
        assert_eq!(h.cid, cid);
    }

    #[test]
    fn test_pause_resume_frame_pause_and_resume() {
        let (_, pause_bytes) = build_pause_resume_frame(
            1,
            PauseResumeOp::Pause,
            vec![("t".into(), 0), ("t".into(), 1)],
        )
        .unwrap();
        let (_, resume_bytes) =
            build_pause_resume_frame(1, PauseResumeOp::Resume, vec![]).unwrap();
        let ph = parse_header_only(&pause_bytes).unwrap();
        let rh = parse_header_only(&resume_bytes).unwrap();
        assert_eq!(ph.kind_byte, FrameType::PauseResumeReq as u8);
        assert_eq!(rh.kind_byte, FrameType::PauseResumeReq as u8);
        // Resume 空 partitions payload 应比 Pause 有 2 分区的短
        assert!(rh.payload_len < ph.payload_len);
    }

    #[test]
    fn test_seek_by_offset_frame() {
        let (_, bytes) = build_seek_by_offset_frame(1, vec![("t".into(), 0, 42)]).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::SeekReq as u8);
    }

    #[test]
    fn test_seek_by_timestamp_frame() {
        let (_, bytes) =
            build_seek_by_timestamp_frame(1, 1_700_000_000_000, vec![("t".into(), 0)]).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::SeekReq as u8);
    }

    #[test]
    fn test_txn_frame_variants() {
        for op in [TxnOp::Begin, TxnOp::Commit, TxnOp::Abort] {
            let (_, bytes) = build_txn_frame("main", op).unwrap();
            let h = parse_header_only(&bytes).unwrap();
            assert_eq!(h.kind_byte, FrameType::TxnReq as u8);
        }
    }

    #[test]
    fn test_send_offsets_frame_header() {
        let (_, bytes) =
            build_send_offsets_frame("prod", 42, "grp", vec![("t".into(), 0, 100)]).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::SendOffsetsReq as u8);
    }

    #[test]
    fn test_set_oauth_token_frame_header() {
        let (_, bytes) = build_set_oauth_token_frame(
            "main",
            "eyJ.token",
            60_000,
            "svc-account",
            vec![("scope".into(), "read".into())],
        )
        .unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::SetOAuthBearerTokenReq as u8);
    }

    #[test]
    fn test_poll_rebalance_frame_header() {
        let (_, bytes) = build_poll_rebalance_frame(1, 32).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::PollRebalanceReq as u8);
    }

    // === Producer / Error 响应解析 =======================================

    #[test]
    fn test_parse_resp_frame_produce_ok() {
        let cid = 123;
        let bytes = encode_test_produce_resp(
            cid,
            ProduceResp::Ok(DeliveryAck {
                partition: 2,
                offset: 999,
            }),
        );
        match parse_resp_frame(&bytes).unwrap() {
            ParsedFrame::Resp {
                cid: got_cid,
                resp: ProduceResp::Ok(ok),
            } => {
                assert_eq!(got_cid, cid);
                assert_eq!(ok.partition, 2);
                assert_eq!(ok.offset, 999);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_resp_frame_produce_err() {
        let cid = 7;
        let bytes = encode_test_produce_resp(
            cid,
            ProduceResp::Err(DeliveryErr {
                code: 42,
                message: "backend fail".into(),
                retryable: true,
            }),
        );
        match parse_resp_frame(&bytes).unwrap() {
            ParsedFrame::Resp {
                resp: ProduceResp::Err(e),
                ..
            } => {
                assert_eq!(e.code, 42);
                assert!(e.retryable);
                assert_eq!(e.message, "backend fail");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_resp_frame_other_kind_passthrough() {
        // 非 ProduceResp 帧应该走 Other 分支，不 panic
        let mut frame = BytesMut::new();
        encode_frame(FrameType::Ping, 555, &[], &mut frame).unwrap();
        match parse_resp_frame(&frame).unwrap() {
            ParsedFrame::Other { .. } => {}
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_error_frame_roundtrip() {
        let er = ErrorResp {
            kind: ErrorKind::ClusterNotRegistered,
            retryable: false,
            native_code: -1,
            message: "cluster 'nope' not registered".into(),
        };
        let mut payload = BytesMut::new();
        er.encode(&mut payload).unwrap();
        let mut frame = BytesMut::new();
        encode_frame(FrameType::Error, 99, &payload, &mut frame).unwrap();

        let parsed = parse_error_frame(&frame).unwrap();
        assert_eq!(parsed.kind, ErrorKind::ClusterNotRegistered.as_u16());
        assert_eq!(parsed.kind_name, ErrorKind::ClusterNotRegistered.as_str());
        assert!(!parsed.retryable);
        assert_eq!(parsed.native_code, -1);
        assert_eq!(parsed.message, "cluster 'nope' not registered");
    }

    #[test]
    fn test_parse_error_frame_rejects_non_error_kind() {
        let mut frame = BytesMut::new();
        encode_frame(FrameType::Ping, 0, &[], &mut frame).unwrap();
        // 用 map_err 转成 String，避开 ParsedError 未 impl Debug 的问题
        let err = parse_error_frame(&frame)
            .map(|_| ())
            .err()
            .expect("should error")
            .to_string();
        assert!(err.contains("expected Error frame"));
    }

    // === Consumer RESP 解析 =============================================

    #[test]
    fn test_parse_consumer_resp_subscribe_ok() {
        let bytes = encode_test_consumer_resp(
            FrameType::SubscribeResp,
            11,
            SubscribeResp::Ok {
                subscription_id: 777,
            },
        );
        match parse_consumer_resp_frame(&bytes).unwrap() {
            ConsumerResp::SubscribeOk {
                cid,
                subscription_id,
            } => {
                assert_eq!(cid, 11);
                assert_eq!(subscription_id, 777);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_consumer_resp_commit_err() {
        let bytes = encode_test_consumer_resp(
            FrameType::CommitResp,
            12,
            CommitResp::Err {
                message: "coord unavailable".into(),
            },
        );
        match parse_consumer_resp_frame(&bytes).unwrap() {
            ConsumerResp::CommitErr { cid, message } => {
                assert_eq!(cid, 12);
                assert_eq!(message, "coord unavailable");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_consumer_resp_txn_ok() {
        let bytes =
            encode_test_consumer_resp(FrameType::TxnResp, 13, TxnResp::Ok);
        match parse_consumer_resp_frame(&bytes).unwrap() {
            ConsumerResp::TxnOk { cid } => assert_eq!(cid, 13),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_consumer_resp_unexpected_kind() {
        // 用 Ping 冒充 consumer resp
        let mut frame = BytesMut::new();
        encode_frame(FrameType::Ping, 0, &[], &mut frame).unwrap();
        let err = parse_consumer_resp_frame(&frame).unwrap_err().to_string();
        assert!(err.contains("unexpected consumer frame kind"));
    }

    #[test]
    fn test_parse_consumer_resp_truncated_payload() {
        // 谎报 payload_len，实际字节不够
        let mut header = BytesMut::new();
        header.extend_from_slice(&100u32.to_be_bytes()); // payload_len = 100
        header.put_u8(FrameType::SubscribeResp as u8);
        header.extend_from_slice(&1u64.to_be_bytes()); // cid
        // 只带 5 字节 payload
        header.extend_from_slice(&[0u8; 5]);
        let err = parse_consumer_resp_frame(&header).unwrap_err().to_string();
        assert!(err.contains("truncated"));
    }

    // === 辅助编码函数 ====================================================
    //
    // 这些函数**只在测试内**——扩展运行时不需要往对端方向编响应帧，
    // 但为了 roundtrip 断言，我们在本地"模拟" worker 侧构造 resp。

    fn encode_test_hello_resp(major: u8) -> Vec<u8> {
        let mut payload = BytesMut::new();
        HelloResp { major }.encode(&mut payload).unwrap();
        let mut frame = BytesMut::new();
        encode_frame(FrameType::Hello, 0, &payload, &mut frame).unwrap();
        frame.to_vec()
    }

    fn encode_test_produce_resp(cid: u64, resp: ProduceResp) -> Vec<u8> {
        let mut payload = BytesMut::new();
        resp.encode(&mut payload).unwrap();
        let mut frame = BytesMut::new();
        encode_frame(FrameType::ProduceResp, cid, &payload, &mut frame).unwrap();
        frame.to_vec()
    }

    fn encode_test_consumer_resp<F>(kind: FrameType, cid: u64, resp: F) -> Vec<u8>
    where
        F: EncodeToBuf,
    {
        let mut payload = BytesMut::new();
        resp.encode_to(&mut payload);
        let mut frame = BytesMut::new();
        encode_frame(kind, cid, &payload, &mut frame).unwrap();
        frame.to_vec()
    }

    /// 让不同 Resp 类型共享编码入口（避免在测试里写 3 个重复 helper）。
    trait EncodeToBuf {
        fn encode_to(&self, buf: &mut BytesMut);
    }
    impl EncodeToBuf for SubscribeResp {
        fn encode_to(&self, buf: &mut BytesMut) {
            self.encode(buf).unwrap();
        }
    }
    impl EncodeToBuf for CommitResp {
        fn encode_to(&self, buf: &mut BytesMut) {
            self.encode(buf).unwrap();
        }
    }
    impl EncodeToBuf for TxnResp {
        fn encode_to(&self, buf: &mut BytesMut) {
            self.encode(buf).unwrap();
        }
    }
}
