//! Consumer 侧 payload 编解码。
//!
//! 所有长度均为大端。布局：
//!
//! ## SUBSCRIBE_REQ
//!
//! ```text
//! [u16 cluster_len][cluster]
//! [u16 group_id_len][group_id]
//! [u16 topics_count]
//!   [(u16 topic_len)(topic)] × topics_count
//! [u16 config_count]
//!   [(u16 key_len)(key)(u16 value_len)(value)] × config_count
//! ```
//!
//! ## SUBSCRIBE_RESP
//!
//! ```text
//! [u8 status]
//! ```
//!
//! - `status = 0x00`：`[u64 subscription_id]`
//! - `status = 0x01`：`[u16 msg_len][msg]`
//!
//! ## POLL_REQ
//!
//! ```text
//! [u64 subscription_id]
//! [u32 max_messages]
//! [u32 timeout_ms]
//! ```
//!
//! ## POLL_RESP
//!
//! ```text
//! [u8 status]
//! ```
//!
//! - `status = 0x00`：`[u32 message_count] [message]*`
//! - `status = 0x01`：`[u16 msg_len][msg]`
//!
//! 单条 message：
//!
//! ```text
//! [u16 topic_len][topic]
//! [i32 partition][i64 offset][i64 timestamp_ms]
//! [u32 key_len][key]
//! [u32 value_len][value]
//! [u32 headers_count]
//!   [(u16 name_len)(name)(u32 value_len)(value)] × headers_count
//! ```
//!
//! ## COMMIT_REQ
//!
//! ```text
//! [u64 subscription_id]
//! ```
//!
//! ## COMMIT_RESP
//!
//! 同 SUBSCRIBE_RESP 的 status 编码（subscription_id 字段被忽略，只用 ok/err）。
//!
//! ## UNSUBSCRIBE
//!
//! ```text
//! [u64 subscription_id]
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::payload::{read_headers, write_headers, MessageHeader, PayloadError};

const STATUS_OK: u8 = 0x00;
const STATUS_ERR: u8 = 0x01;

// ============================================================================
// SubscribeReq
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeReq {
    pub cluster: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub config: Vec<(String, String)>,
}

impl SubscribeReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.cluster, buf)?;
        write_str_u16(&self.group_id, buf)?;
        if self.topics.len() > u16::MAX as usize {
            return Err(PayloadError::FieldTooLarge(self.topics.len()));
        }
        buf.put_u16(self.topics.len() as u16);
        for t in &self.topics {
            write_str_u16(t, buf)?;
        }
        if self.config.len() > u16::MAX as usize {
            return Err(PayloadError::FieldTooLarge(self.config.len()));
        }
        buf.put_u16(self.config.len() as u16);
        for (k, v) in &self.config {
            write_str_u16(k, buf)?;
            write_str_u16(v, buf)?;
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str_u16(&mut buf, "cluster")?;
        let group_id = read_str_u16(&mut buf, "group_id")?;
        if buf.remaining() < 2 {
            return Err(PayloadError::Truncated);
        }
        let n = buf.get_u16() as usize;
        // P3: 解码侧 alloc 上限。即便上层有 MAX_PAYLOAD_LEN 总闸，预 alloc 大 Vec 仍能
        // 让短瞬内存激增。topics 实际几百够用；config 走 librdkafka 200+ 键的上限。
        let mut topics = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            topics.push(read_str_u16(&mut buf, "topic")?);
        }
        if buf.remaining() < 2 {
            return Err(PayloadError::Truncated);
        }
        let m = buf.get_u16() as usize;
        let mut config = Vec::with_capacity(m.min(256));
        for _ in 0..m {
            let k = read_str_u16(&mut buf, "cfg_key")?;
            let v = read_str_u16(&mut buf, "cfg_value")?;
            config.push((k, v));
        }
        Ok(Self {
            cluster,
            group_id,
            topics,
            config,
        })
    }
}

// ============================================================================
// SubscribeResp
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeResp {
    Ok { subscription_id: u64 },
    Err { message: String },
}

impl SubscribeResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok { subscription_id } => {
                buf.put_u8(STATUS_OK);
                buf.put_u64(*subscription_id);
            }
            Self::Err { message } => {
                buf.put_u8(STATUS_ERR);
                write_str_u16(message, buf)?;
            }
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        match buf.get_u8() {
            STATUS_OK => {
                if buf.remaining() < 8 {
                    return Err(PayloadError::Truncated);
                }
                Ok(Self::Ok {
                    subscription_id: buf.get_u64(),
                })
            }
            STATUS_ERR => Ok(Self::Err {
                message: read_str_u16(&mut buf, "message")?,
            }),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "subscribe_resp",
            }),
        }
    }
}

// ============================================================================
// PollReq
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollReq {
    pub subscription_id: u64,
    pub max_messages: u32,
    pub timeout_ms: u32,
}

impl PollReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        buf.put_u32(self.max_messages);
        buf.put_u32(self.timeout_ms);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 8 + 4 + 4 {
            return Err(PayloadError::Truncated);
        }
        Ok(Self {
            subscription_id: buf.get_u64(),
            max_messages: buf.get_u32(),
            timeout_ms: buf.get_u32(),
        })
    }
}

// ============================================================================
// ConsumerMessage
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<MessageHeader>,
}

impl ConsumerMessage {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.topic, buf)?;
        buf.put_i32(self.partition);
        buf.put_i64(self.offset);
        buf.put_i64(self.timestamp_ms);
        write_bytes_u32(&self.key, buf)?;
        write_bytes_u32(&self.value, buf)?;
        write_headers(&self.headers, buf)?;
        Ok(())
    }

    fn decode(buf: &mut &[u8]) -> Result<Self, PayloadError> {
        let topic = read_str_u16(buf, "topic")?;
        if buf.remaining() < 4 + 8 + 8 {
            return Err(PayloadError::Truncated);
        }
        let partition = buf.get_i32();
        let offset = buf.get_i64();
        let timestamp_ms = buf.get_i64();
        let key = read_bytes_u32(buf)?;
        let value = read_bytes_u32(buf)?;
        let headers = read_headers(buf)?;
        Ok(Self {
            topic,
            partition,
            offset,
            timestamp_ms,
            key,
            value,
            headers,
        })
    }
}

// ============================================================================
// PollResp
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollResp {
    Ok { messages: Vec<ConsumerMessage> },
    Err { message: String },
}

impl PollResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok { messages } => {
                buf.put_u8(STATUS_OK);
                if messages.len() > u32::MAX as usize {
                    return Err(PayloadError::FieldTooLarge(messages.len()));
                }
                buf.put_u32(messages.len() as u32);
                for m in messages {
                    m.encode(buf)?;
                }
            }
            Self::Err { message } => {
                buf.put_u8(STATUS_ERR);
                write_str_u16(message, buf)?;
            }
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        match buf.get_u8() {
            STATUS_OK => {
                if buf.remaining() < 4 {
                    return Err(PayloadError::Truncated);
                }
                let n = buf.get_u32() as usize;
                // P3: 防御解码侧 alloc DoS。单次 poll 批 64K 条已经足够任何业务。
                // 真要 64K+，分多次 poll 即可。
                let mut messages = Vec::with_capacity(n.min(65_536));
                for _ in 0..n {
                    messages.push(ConsumerMessage::decode(&mut buf)?);
                }
                Ok(Self::Ok { messages })
            }
            STATUS_ERR => Ok(Self::Err {
                message: read_str_u16(&mut buf, "message")?,
            }),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "poll_resp",
            }),
        }
    }
}

// ============================================================================
// CommitReq / CommitResp / Unsubscribe
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReq {
    pub subscription_id: u64,
}

impl CommitReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 8 {
            return Err(PayloadError::Truncated);
        }
        Ok(Self {
            subscription_id: buf.get_u64(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResp {
    Ok,
    Err { message: String },
}

impl CommitResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok => {
                buf.put_u8(STATUS_OK);
            }
            Self::Err { message } => {
                buf.put_u8(STATUS_ERR);
                write_str_u16(message, buf)?;
            }
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        match buf.get_u8() {
            STATUS_OK => Ok(Self::Ok),
            STATUS_ERR => Ok(Self::Err {
                message: read_str_u16(&mut buf, "message")?,
            }),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "commit_resp",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeReq {
    pub subscription_id: u64,
}

impl UnsubscribeReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 8 {
            return Err(PayloadError::Truncated);
        }
        Ok(Self {
            subscription_id: buf.get_u64(),
        })
    }
}

// ============================================================================
// RegisterCluster / RegisterClusterResp
// ============================================================================

/// 注册或覆盖一个 cluster。worker 端从 registry 拿配置惰性创建 librdkafka 客户端。
///
/// 布局：
/// ```text
/// [u16 cluster_len][cluster]
/// [u16 config_count]
///   [(u16 key_len)(key)(u16 value_len)(value)] × config_count
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterClusterReq {
    pub cluster: String,
    pub config: Vec<(String, String)>,
}

impl RegisterClusterReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.cluster, buf)?;
        if self.config.len() > u16::MAX as usize {
            return Err(PayloadError::FieldTooLarge(self.config.len()));
        }
        buf.put_u16(self.config.len() as u16);
        for (k, v) in &self.config {
            write_str_u16(k, buf)?;
            write_str_u16(v, buf)?;
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str_u16(&mut buf, "cluster")?;
        if buf.remaining() < 2 {
            return Err(PayloadError::Truncated);
        }
        let n = buf.get_u16() as usize;
        // P3: 与 SubscribeReq::decode 一致——librdkafka 200+ 键封顶。
        let mut config = Vec::with_capacity(n.min(256));
        for _ in 0..n {
            let k = read_str_u16(&mut buf, "cfg_key")?;
            let v = read_str_u16(&mut buf, "cfg_value")?;
            config.push((k, v));
        }
        Ok(Self { cluster, config })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterClusterResp {
    Ok,
    Err { message: String },
}

impl RegisterClusterResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok => buf.put_u8(STATUS_OK),
            Self::Err { message } => {
                buf.put_u8(STATUS_ERR);
                write_str_u16(message, buf)?;
            }
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        match buf.get_u8() {
            STATUS_OK => Ok(Self::Ok),
            STATUS_ERR => Ok(Self::Err {
                message: read_str_u16(&mut buf, "message")?,
            }),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "register_cluster_resp",
            }),
        }
    }
}

// ============================================================================
// 通用读写 helpers（与 payload.rs 中保持一致语义；复制以保持模块独立）
// ============================================================================

fn write_str_u16(s: &str, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if s.len() > u16::MAX as usize {
        return Err(PayloadError::FieldTooLarge(s.len()));
    }
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
    Ok(())
}

fn write_bytes_u32(b: &[u8], buf: &mut BytesMut) -> Result<(), PayloadError> {
    if b.len() > u32::MAX as usize {
        return Err(PayloadError::FieldTooLarge(b.len()));
    }
    buf.put_u32(b.len() as u32);
    buf.put_slice(b);
    Ok(())
}

fn read_str_u16(buf: &mut &[u8], field: &'static str) -> Result<String, PayloadError> {
    if buf.remaining() < 2 {
        return Err(PayloadError::Truncated);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(PayloadError::Truncated);
    }
    let bytes = buf.copy_to_bytes(len).to_vec();
    String::from_utf8(bytes).map_err(|_| PayloadError::InvalidUtf8 { field })
}

fn read_bytes_u32(buf: &mut &[u8]) -> Result<Bytes, PayloadError> {
    if buf.remaining() < 4 {
        return Err(PayloadError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(PayloadError::Truncated);
    }
    Ok(buf.copy_to_bytes(len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_req_roundtrip() {
        let orig = SubscribeReq {
            cluster: "default".into(),
            group_id: "g1".into(),
            topics: vec!["a".into(), "b".into()],
            config: vec![("auto.offset.reset".into(), "earliest".into())],
        };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(SubscribeReq::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_subscribe_resp_ok() {
        let orig = SubscribeResp::Ok {
            subscription_id: 42,
        };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(SubscribeResp::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_subscribe_resp_err() {
        let orig = SubscribeResp::Err {
            message: "broker down".into(),
        };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(SubscribeResp::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_poll_req_roundtrip() {
        let orig = PollReq {
            subscription_id: 7,
            max_messages: 100,
            timeout_ms: 1000,
        };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(PollReq::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_poll_resp_empty() {
        let orig = PollResp::Ok { messages: vec![] };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(PollResp::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_poll_resp_with_messages() {
        let orig = PollResp::Ok {
            messages: vec![
                ConsumerMessage {
                    topic: "orders".into(),
                    partition: 0,
                    offset: 100,
                    timestamp_ms: 1_700_000_000_000,
                    key: Bytes::from_static(b"k1"),
                    value: Bytes::from_static(b"v1"),
                    headers: vec![("traceparent".into(), Bytes::from_static(b"00-aaaa-bbbb-01"))],
                },
                ConsumerMessage {
                    topic: "orders".into(),
                    partition: 1,
                    offset: 200,
                    timestamp_ms: 1_700_000_001_000,
                    key: Bytes::from_static(b""),
                    value: Bytes::from_static(b"v2"),
                    headers: vec![],
                },
            ],
        };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(PollResp::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_commit_req_resp() {
        let req = CommitReq {
            subscription_id: 99,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf).unwrap();
        assert_eq!(CommitReq::decode(&buf).unwrap(), req);

        let resp = CommitResp::Ok;
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        assert_eq!(CommitResp::decode(&buf).unwrap(), resp);

        let resp = CommitResp::Err {
            message: "no offsets".into(),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        assert_eq!(CommitResp::decode(&buf).unwrap(), resp);
    }

    #[test]
    fn test_unsubscribe_roundtrip() {
        let orig = UnsubscribeReq { subscription_id: 1 };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(UnsubscribeReq::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_register_cluster_roundtrip() {
        let orig = RegisterClusterReq {
            cluster: "orders".into(),
            config: vec![
                (
                    "bootstrap.servers".into(),
                    "kafka-1:9092,kafka-2:9092".into(),
                ),
                ("compression.type".into(), "lz4".into()),
                ("sasl.mechanism".into(), "PLAIN".into()),
            ],
        };
        let mut buf = BytesMut::new();
        orig.encode(&mut buf).unwrap();
        assert_eq!(RegisterClusterReq::decode(&buf).unwrap(), orig);
    }

    #[test]
    fn test_register_cluster_resp_ok_err() {
        let mut buf = BytesMut::new();
        RegisterClusterResp::Ok.encode(&mut buf).unwrap();
        assert_eq!(
            RegisterClusterResp::decode(&buf).unwrap(),
            RegisterClusterResp::Ok
        );

        let err = RegisterClusterResp::Err {
            message: "missing bootstrap.servers".into(),
        };
        let mut buf = BytesMut::new();
        err.encode(&mut buf).unwrap();
        assert_eq!(RegisterClusterResp::decode(&buf).unwrap(), err);
    }
}
