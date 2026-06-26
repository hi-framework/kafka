//! Rebalance 事件流（Phase 3.9）。
//!
//! Consumer group partition 重分配时，librdkafka 触发 pre/post rebalance 回调，
//! worker 把事件塞进 subscription 的事件队列；PHP 通过 `PollRebalanceReq` 拉取。
//!
//! ## PollRebalanceReq
//!
//! ```text
//! [u64 subscription_id]
//! [u32 max_events]
//! ```
//!
//! ## PollRebalanceResp
//!
//! ```text
//! [u8 status]
//! ```
//!
//! - `status = 0x00`：`[u32 event_count] [event]*`
//! - `status = 0x01`：`[u16 msg_len][msg]`
//!
//! 单个 event：
//!
//! ```text
//! [u8 kind]                 // 0 = Assign, 1 = Revoke, 2 = Error
//! [rest...]
//! ```
//!
//! - `kind=0/1`：`[u32 partitions_count] [(u16 topic_len)(topic)(i32 partition)] × partitions_count`
//! - `kind=2`：`[u16 msg_len][msg]`

use bytes::{Buf, BufMut, BytesMut};

use crate::payload::PayloadError;

const STATUS_OK: u8 = 0x00;
const STATUS_ERR: u8 = 0x01;

const EVENT_ASSIGN: u8 = 0;
const EVENT_REVOKE: u8 = 1;
const EVENT_ERROR: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollRebalanceReq {
    pub subscription_id: u64,
    pub max_events: u32,
}

impl PollRebalanceReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        buf.put_u32(self.max_events);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 12 {
            return Err(PayloadError::Truncated);
        }
        Ok(Self {
            subscription_id: buf.get_u64(),
            max_events: buf.get_u32(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceEvent {
    Assign { partitions: Vec<(String, i32)> },
    Revoke { partitions: Vec<(String, i32)> },
    Error { message: String },
}

impl RebalanceEvent {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Assign { partitions } => {
                buf.put_u8(EVENT_ASSIGN);
                write_partitions(partitions, buf)?;
            }
            Self::Revoke { partitions } => {
                buf.put_u8(EVENT_REVOKE);
                write_partitions(partitions, buf)?;
            }
            Self::Error { message } => {
                buf.put_u8(EVENT_ERROR);
                write_str_u16(message, buf)?;
            }
        }
        Ok(())
    }

    fn decode(buf: &mut &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 1 {
            return Err(PayloadError::Truncated);
        }
        let kind = buf.get_u8();
        match kind {
            EVENT_ASSIGN => Ok(Self::Assign {
                partitions: read_partitions(buf)?,
            }),
            EVENT_REVOKE => Ok(Self::Revoke {
                partitions: read_partitions(buf)?,
            }),
            EVENT_ERROR => Ok(Self::Error {
                message: read_str_u16(buf, "message")?,
            }),
            other => Err(PayloadError::InvalidTag {
                tag: other,
                field: "rebalance_event_kind",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollRebalanceResp {
    Ok { events: Vec<RebalanceEvent> },
    Err { message: String },
}

impl PollRebalanceResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok { events } => {
                buf.put_u8(STATUS_OK);
                if events.len() > u32::MAX as usize {
                    return Err(PayloadError::FieldTooLarge(events.len()));
                }
                buf.put_u32(events.len() as u32);
                for e in events {
                    e.encode(buf)?;
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
                let mut events = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    events.push(RebalanceEvent::decode(&mut buf)?);
                }
                Ok(Self::Ok { events })
            }
            STATUS_ERR => Ok(Self::Err {
                message: read_str_u16(&mut buf, "message")?,
            }),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "poll_rebalance_resp",
            }),
        }
    }
}

fn write_partitions(parts: &[(String, i32)], buf: &mut BytesMut) -> Result<(), PayloadError> {
    if parts.len() > u32::MAX as usize {
        return Err(PayloadError::FieldTooLarge(parts.len()));
    }
    buf.put_u32(parts.len() as u32);
    for (topic, partition) in parts {
        write_str_u16(topic, buf)?;
        buf.put_i32(*partition);
    }
    Ok(())
}

fn read_partitions(buf: &mut &[u8]) -> Result<Vec<(String, i32)>, PayloadError> {
    if buf.remaining() < 4 {
        return Err(PayloadError::Truncated);
    }
    let n = buf.get_u32() as usize;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let topic = read_str_u16(buf, "topic")?;
        if buf.remaining() < 4 {
            return Err(PayloadError::Truncated);
        }
        let partition = buf.get_i32();
        out.push((topic, partition));
    }
    Ok(out)
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
    fn test_poll_rebalance_req() {
        let r = PollRebalanceReq {
            subscription_id: 42,
            max_events: 100,
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf).unwrap();
        assert_eq!(PollRebalanceReq::decode(&buf).unwrap(), r);
    }

    #[test]
    fn test_poll_rebalance_resp_with_events() {
        let resp = PollRebalanceResp::Ok {
            events: vec![
                RebalanceEvent::Revoke {
                    partitions: vec![("orders".into(), 0), ("orders".into(), 1)],
                },
                RebalanceEvent::Assign {
                    partitions: vec![("orders".into(), 2)],
                },
                RebalanceEvent::Error {
                    message: "broker timed out".into(),
                },
            ],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        assert_eq!(PollRebalanceResp::decode(&buf).unwrap(), resp);
    }

    #[test]
    fn test_poll_rebalance_resp_empty() {
        let resp = PollRebalanceResp::Ok { events: vec![] };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).unwrap();
        assert_eq!(PollRebalanceResp::decode(&buf).unwrap(), resp);
    }
}
