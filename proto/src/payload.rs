//! MVP payload 编解码。
//!
//! 在引入 protobuf 之前用最小手写格式承载消息。所有长度均为大端。
//!
//! ## PRODUCE_FNF / PRODUCE_REQ v1
//!
//! ```text
//! [u16 cluster_len][cluster][u16 topic_len][topic]
//! [u32 key_len][key][u32 value_len][value]
//! [i32 partition][i64 timestamp_ms]
//! [u32 headers_count]
//!   [(u16 name_len)(name)(u32 value_len)(value)] × headers_count
//! ```
//!
//! 两种帧的 payload 二进制布局相同。区别只在 frame type 上（FNF 单向，REQ 期待 RESP）。
//!
//! - `partition`：`-1` = 让 librdkafka 用 partitioner 决定（典型 key hash），其它值
//!   指定写入分区（key 仍透传，但路由由用户控制）
//! - `timestamp_ms`：`-1` = librdkafka 用 CreateTime（系统当前时间）
//! - `headers`：见 Phase 3.1，空数组编码为 `[0u32]`
//!
//! ## PRODUCE_RESP v1
//!
//! ```text
//! [u8 status][rest...]
//! ```
//!
//! - `status = 0x00` (Ok)：`[i32 partition][i64 offset]`
//! - `status = 0x01` (Err)：`[u16 error_code][u16 msg_len][msg][u8 retryable]`

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error("payload truncated")]
    Truncated,

    #[error("field too large: {0}")]
    FieldTooLarge(usize),

    #[error("invalid utf-8 in {field}")]
    InvalidUtf8 { field: &'static str },

    #[error("invalid tag 0x{tag:02x} in {field}")]
    InvalidTag { tag: u8, field: &'static str },
}

/// Kafka 消息头：`(name, value)`。name 是 UTF-8 字符串，value 是任意字节序列。
pub type MessageHeader = (String, Bytes);

/// `-1` 表示 librdkafka 决定（auto）。其它非负值为显式指定。
pub const AUTO_PARTITION: i32 = -1;
pub const AUTO_TIMESTAMP: i64 = -1;

/// HELLO 帧 payload：仅 `[u8 major]`。
///
/// 协议握手语义：扩展端每条新建 UDS 连接的第一帧必须是 HELLO，并带上扩展
/// 自身编译的 `PROTOCOL_MAJOR`。worker 收到后校验 major 与自身一致，一致
/// 时回一个 HELLO 帧（payload 为 worker 的 server major），不一致直接关
/// 连接，扩展侧 read 端 EOF → 视为 connect 失败（pool 重试）。
///
/// 把握手挪到协议层显式实现而不是依赖默认行为，是为了：
/// - 升级 PROTOCOL_MAJOR 时双端不匹配能即时拒绝，不会出现「字段错位静默
///   解码出垃圾值」这种灾难
/// - worker 重启换不同 build / 不同 commit 时给客户端一次明确感知
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloReq {
    pub major: u8,
}

impl HelloReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u8(self.major);
        Ok(())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        Ok(Self { major: buf[0] })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloResp {
    pub major: u8,
}

impl HelloResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u8(self.major);
        Ok(())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        Ok(Self { major: buf[0] })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceFnf {
    pub cluster: String,
    pub topic: String,
    pub key: Bytes,
    pub value: Bytes,
    /// `-1` = auto（librdkafka partitioner，通常按 key hash）
    pub partition: i32,
    /// `-1` = auto（librdkafka 用系统当前 CreateTime）
    pub timestamp_ms: i64,
    pub headers: Vec<MessageHeader>,
}

impl Default for ProduceFnf {
    fn default() -> Self {
        Self {
            cluster: String::new(),
            topic: String::new(),
            key: Bytes::new(),
            value: Bytes::new(),
            partition: AUTO_PARTITION,
            timestamp_ms: AUTO_TIMESTAMP,
            headers: Vec::new(),
        }
    }
}

impl ProduceFnf {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.cluster, buf)?;
        write_str_u16(&self.topic, buf)?;
        write_bytes_u32(&self.key, buf)?;
        write_bytes_u32(&self.value, buf)?;
        buf.put_i32(self.partition);
        buf.put_i64(self.timestamp_ms);
        write_headers(&self.headers, buf)?;
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str_u16(&mut buf, "cluster")?;
        let topic = read_str_u16(&mut buf, "topic")?;
        let key = read_bytes_u32(&mut buf)?;
        let value = read_bytes_u32(&mut buf)?;
        if buf.remaining() < 4 + 8 {
            return Err(PayloadError::Truncated);
        }
        let partition = buf.get_i32();
        let timestamp_ms = buf.get_i64();
        let headers = read_headers(&mut buf)?;
        Ok(Self {
            cluster,
            topic,
            key,
            value,
            partition,
            timestamp_ms,
            headers,
        })
    }
}

pub(crate) fn write_headers(
    headers: &[MessageHeader],
    buf: &mut BytesMut,
) -> Result<(), PayloadError> {
    if headers.len() > u32::MAX as usize {
        return Err(PayloadError::FieldTooLarge(headers.len()));
    }
    buf.put_u32(headers.len() as u32);
    for (name, value) in headers {
        write_str_u16(name, buf)?;
        write_bytes_u32(value, buf)?;
    }
    Ok(())
}

pub(crate) fn read_headers(buf: &mut &[u8]) -> Result<Vec<MessageHeader>, PayloadError> {
    if buf.remaining() < 4 {
        return Err(PayloadError::Truncated);
    }
    let n = buf.get_u32() as usize;
    let mut headers = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let name = read_str_u16(buf, "header_name")?;
        let value = read_bytes_u32(buf)?;
        headers.push((name, value));
    }
    Ok(headers)
}

/// PRODUCE_REQ 与 PRODUCE_FNF payload 布局完全一致，仅帧类型不同。
/// 用类型别名表达，避免重复编解码代码。
pub type ProduceReq = ProduceFnf;

const RESP_OK: u8 = 0x00;
const RESP_ERR: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProduceResp {
    Ok(DeliveryAck),
    Err(DeliveryErr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAck {
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryErr {
    pub code: u16,
    pub message: String,
    pub retryable: bool,
}

impl ProduceResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok(ack) => {
                buf.put_u8(RESP_OK);
                buf.put_i32(ack.partition);
                buf.put_i64(ack.offset);
            }
            Self::Err(err) => {
                buf.put_u8(RESP_ERR);
                buf.put_u16(err.code);
                write_str_u16(&err.message, buf)?;
                buf.put_u8(if err.retryable { 1 } else { 0 });
            }
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.is_empty() {
            return Err(PayloadError::Truncated);
        }
        let tag = buf.get_u8();
        match tag {
            RESP_OK => {
                if buf.remaining() < 4 + 8 {
                    return Err(PayloadError::Truncated);
                }
                let partition = buf.get_i32();
                let offset = buf.get_i64();
                Ok(Self::Ok(DeliveryAck { partition, offset }))
            }
            RESP_ERR => {
                if buf.remaining() < 2 {
                    return Err(PayloadError::Truncated);
                }
                let code = buf.get_u16();
                let message = read_str_u16(&mut buf, "message")?;
                if buf.remaining() < 1 {
                    return Err(PayloadError::Truncated);
                }
                let retryable = buf.get_u8() != 0;
                Ok(Self::Err(DeliveryErr {
                    code,
                    message,
                    retryable,
                }))
            }
            other => Err(PayloadError::InvalidTag {
                tag: other,
                field: "produce_resp",
            }),
        }
    }
}

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
    fn test_produce_fnf_roundtrip() {
        let original = ProduceFnf {
            cluster: "default".into(),
            topic: "orders".into(),
            key: Bytes::from_static(b"user-42"),
            value: Bytes::from_static(b"{\"amount\":100}"),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        let decoded = ProduceFnf::decode(&buf).unwrap();
        assert_eq!(decoded, original);
        // 默认值是 -1（AUTO）
        assert_eq!(decoded.partition, -1);
        assert_eq!(decoded.timestamp_ms, -1);
    }

    #[test]
    fn test_produce_fnf_explicit_partition_and_timestamp() {
        let original = ProduceFnf {
            cluster: "default".into(),
            topic: "orders".into(),
            key: Bytes::from_static(b"user-42"),
            value: Bytes::from_static(b"v"),
            partition: 3,
            timestamp_ms: 1_700_000_000_000,
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(ProduceFnf::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_produce_fnf_with_headers() {
        let original = ProduceFnf {
            cluster: "default".into(),
            topic: "orders".into(),
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            headers: vec![
                (
                    "traceparent".into(),
                    Bytes::from_static(b"00-1234567890abcdef-fedcba0987654321-01"),
                ),
                ("source".into(), Bytes::from_static(b"web")),
                ("x-binary".into(), Bytes::from_static(&[0x00, 0xff, 0x42])),
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        let decoded = ProduceFnf::decode(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_truncated() {
        let err = ProduceFnf::decode(b"\x00").unwrap_err();
        assert!(matches!(err, PayloadError::Truncated));
    }

    #[test]
    fn test_empty_key_and_value() {
        let original = ProduceFnf {
            cluster: "c".into(),
            topic: "t".into(),
            key: Bytes::new(),
            value: Bytes::new(),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        let decoded = ProduceFnf::decode(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_produce_resp_ok_roundtrip() {
        let original = ProduceResp::Ok(DeliveryAck {
            partition: 7,
            offset: 12345,
        });
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(ProduceResp::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_produce_resp_err_roundtrip() {
        let original = ProduceResp::Err(DeliveryErr {
            code: 42,
            message: "broker not available".into(),
            retryable: true,
        });
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(ProduceResp::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_produce_resp_invalid_tag() {
        let buf = [0xFFu8];
        let err = ProduceResp::decode(&buf).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidTag { tag: 0xFF, .. }));
    }

    #[test]
    fn test_produce_resp_truncated() {
        // Ok tag but missing partition/offset
        let buf = [RESP_OK, 0x00];
        let err = ProduceResp::decode(&buf).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated));
    }
}
