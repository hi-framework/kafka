//! Seek 协议（Phase 3.11）。
//!
//! ## SeekReq
//!
//! ```text
//! [u64 subscription_id]
//! [u8 mode]                  // 0 = ByOffset, 1 = ByTimestamp
//! [...mode-specific data...]
//! ```
//!
//! ### ByOffset
//!
//! ```text
//! [u32 count]
//! [(u16 topic_len)(topic)(i32 partition)(i64 offset)] × count
//! ```
//!
//! ### ByTimestamp
//!
//! ```text
//! [i64 timestamp_ms]
//! [u32 count]                // 0 = 应用到当前 assignment 的所有分区
//! [(u16 topic_len)(topic)(i32 partition)] × count
//! ```
//!
//! ## SeekResp
//!
//! 同 TxnResp：`[u8 status]` + 可选 `[u16 msg_len][msg]`

use bytes::{Buf, BufMut, BytesMut};

use crate::payload::PayloadError;

const STATUS_OK: u8 = 0x00;
const STATUS_ERR: u8 = 0x01;

const MODE_BY_OFFSET: u8 = 0;
const MODE_BY_TIMESTAMP: u8 = 1;

/// `(topic, partition, offset)`
pub type OffsetSpec = (String, i32, i64);
/// `(topic, partition)`
pub type PartitionSpec = (String, i32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeekReq {
    ByOffset {
        subscription_id: u64,
        targets: Vec<OffsetSpec>,
    },
    ByTimestamp {
        subscription_id: u64,
        timestamp_ms: i64,
        /// 空 = 应用到当前 assignment 全部分区
        partitions: Vec<PartitionSpec>,
    },
}

impl SeekReq {
    pub fn subscription_id(&self) -> u64 {
        match self {
            Self::ByOffset {
                subscription_id, ..
            } => *subscription_id,
            Self::ByTimestamp {
                subscription_id, ..
            } => *subscription_id,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::ByOffset {
                subscription_id,
                targets,
            } => {
                buf.put_u64(*subscription_id);
                buf.put_u8(MODE_BY_OFFSET);
                if targets.len() > u32::MAX as usize {
                    return Err(PayloadError::FieldTooLarge(targets.len()));
                }
                buf.put_u32(targets.len() as u32);
                for (topic, partition, offset) in targets {
                    write_str_u16(topic, buf)?;
                    buf.put_i32(*partition);
                    buf.put_i64(*offset);
                }
            }
            Self::ByTimestamp {
                subscription_id,
                timestamp_ms,
                partitions,
            } => {
                buf.put_u64(*subscription_id);
                buf.put_u8(MODE_BY_TIMESTAMP);
                buf.put_i64(*timestamp_ms);
                if partitions.len() > u32::MAX as usize {
                    return Err(PayloadError::FieldTooLarge(partitions.len()));
                }
                buf.put_u32(partitions.len() as u32);
                for (topic, partition) in partitions {
                    write_str_u16(topic, buf)?;
                    buf.put_i32(*partition);
                }
            }
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 9 {
            return Err(PayloadError::Truncated);
        }
        let subscription_id = buf.get_u64();
        let mode = buf.get_u8();
        match mode {
            MODE_BY_OFFSET => {
                if buf.remaining() < 4 {
                    return Err(PayloadError::Truncated);
                }
                let n = buf.get_u32() as usize;
                let mut targets = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    let topic = read_str_u16(&mut buf, "topic")?;
                    if buf.remaining() < 12 {
                        return Err(PayloadError::Truncated);
                    }
                    let partition = buf.get_i32();
                    let offset = buf.get_i64();
                    targets.push((topic, partition, offset));
                }
                Ok(Self::ByOffset {
                    subscription_id,
                    targets,
                })
            }
            MODE_BY_TIMESTAMP => {
                if buf.remaining() < 12 {
                    return Err(PayloadError::Truncated);
                }
                let timestamp_ms = buf.get_i64();
                let n = buf.get_u32() as usize;
                let mut partitions = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    let topic = read_str_u16(&mut buf, "topic")?;
                    if buf.remaining() < 4 {
                        return Err(PayloadError::Truncated);
                    }
                    let partition = buf.get_i32();
                    partitions.push((topic, partition));
                }
                Ok(Self::ByTimestamp {
                    subscription_id,
                    timestamp_ms,
                    partitions,
                })
            }
            other => Err(PayloadError::InvalidTag {
                tag: other,
                field: "seek_mode",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeekResp {
    Ok,
    Err { message: String },
}

impl SeekResp {
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
                field: "seek_resp",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seek_by_offset_roundtrip() {
        let r = SeekReq::ByOffset {
            subscription_id: 7,
            targets: vec![("orders".into(), 0, 100), ("orders".into(), 1, 200)],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf).unwrap();
        assert_eq!(SeekReq::decode(&buf).unwrap(), r);
    }

    #[test]
    fn test_seek_by_timestamp_roundtrip() {
        let r = SeekReq::ByTimestamp {
            subscription_id: 7,
            timestamp_ms: 1_700_000_000_000,
            partitions: vec![("orders".into(), 0)],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf).unwrap();
        assert_eq!(SeekReq::decode(&buf).unwrap(), r);
    }

    #[test]
    fn test_seek_by_timestamp_all_assigned() {
        let r = SeekReq::ByTimestamp {
            subscription_id: 1,
            timestamp_ms: 0,
            partitions: vec![],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf).unwrap();
        assert_eq!(SeekReq::decode(&buf).unwrap(), r);
    }

    #[test]
    fn test_seek_resp_ok_err() {
        let mut buf = BytesMut::new();
        SeekResp::Ok.encode(&mut buf).unwrap();
        assert_eq!(SeekResp::decode(&buf).unwrap(), SeekResp::Ok);

        let err = SeekResp::Err {
            message: "no current assignment".into(),
        };
        let mut buf = BytesMut::new();
        err.encode(&mut buf).unwrap();
        assert_eq!(SeekResp::decode(&buf).unwrap(), err);
    }
}
