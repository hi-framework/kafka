//! `send_offsets_to_transaction` 协议（Phase 3.13）。
//!
//! Exactly-once stream 处理：在 producer 事务内提交 consumer 的输入 offset，
//! 与 producer 写出的消息原子可见。崩溃恢复时要么"输出+offset"都在，要么都没有。
//!
//! ## SendOffsetsReq
//!
//! ```text
//! [u16 producer_cluster_len][producer_cluster]
//! [u64 subscription_id]            // consumer 的 sub_id，用于 group_metadata
//! [u16 group_id_len][group_id]
//! [u32 offsets_count]
//! 重复 offsets_count 次：
//!   [u16 topic_len][topic][i32 partition][i64 offset]
//! ```
//!
//! ## SendOffsetsResp
//!
//! ```text
//! [u8 status]
//! ```
//!
//! - `status = 0x00` (Ok)：无 payload
//! - `status = 0x01` (Err)：`[u16 msg_len][msg]`

use bytes::{Buf, BufMut, BytesMut};

use crate::payload::PayloadError;

const STATUS_OK: u8 = 0x00;
const STATUS_ERR: u8 = 0x01;

/// `(topic, partition, offset)` —— offset 是「下一条要读的位置」（last_consumed + 1）。
pub type OffsetCommit = (String, i32, i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendOffsetsReq {
    /// 启用 transactional.id 的 producer 所在 cluster
    pub producer_cluster: String,
    /// 提供 ConsumerGroupMetadata 的 consumer subscription_id
    pub subscription_id: u64,
    /// consumer 的 group.id（必须与 subscribe 时一致；worker 端会校验）
    pub group_id: String,
    /// 要原子提交的 offsets（每个 (topic, partition, next_offset)）
    pub offsets: Vec<OffsetCommit>,
}

impl SendOffsetsReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.producer_cluster, buf)?;
        buf.put_u64(self.subscription_id);
        write_str_u16(&self.group_id, buf)?;
        if self.offsets.len() > u32::MAX as usize {
            return Err(PayloadError::FieldTooLarge(self.offsets.len()));
        }
        buf.put_u32(self.offsets.len() as u32);
        for (topic, partition, offset) in &self.offsets {
            write_str_u16(topic, buf)?;
            buf.put_i32(*partition);
            buf.put_i64(*offset);
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let producer_cluster = read_str_u16(&mut buf, "producer_cluster")?;
        if buf.remaining() < 8 {
            return Err(PayloadError::Truncated);
        }
        let subscription_id = buf.get_u64();
        let group_id = read_str_u16(&mut buf, "group_id")?;
        if buf.remaining() < 4 {
            return Err(PayloadError::Truncated);
        }
        let count = buf.get_u32() as usize;
        // cap 预分配；同 pause_resume 的分区上限语义。
        let mut offsets = Vec::with_capacity(count.min(8192));
        for _ in 0..count {
            let topic = read_str_u16(&mut buf, "topic")?;
            if buf.remaining() < 12 {
                return Err(PayloadError::Truncated);
            }
            let partition = buf.get_i32();
            let offset = buf.get_i64();
            offsets.push((topic, partition, offset));
        }
        Ok(Self {
            producer_cluster,
            subscription_id,
            group_id,
            offsets,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOffsetsResp {
    Ok,
    Err { message: String },
}

impl SendOffsetsResp {
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
                field: "send_offsets_resp",
            }),
        }
    }
}

// 内部 helpers（与其他模块一致，避免跨模块 pub）
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
    fn test_req_roundtrip() {
        let original = SendOffsetsReq {
            producer_cluster: "txn".into(),
            subscription_id: 0xdeadbeef_cafebabe,
            group_id: "stream-consumer".into(),
            offsets: vec![
                ("input-a".into(), 0, 42),
                ("input-a".into(), 1, 100),
                ("input-b".into(), 0, 0),
            ],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(SendOffsetsReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_req_empty_offsets() {
        let original = SendOffsetsReq {
            producer_cluster: "txn".into(),
            subscription_id: 1,
            group_id: "g".into(),
            offsets: vec![],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(SendOffsetsReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_resp_ok_err() {
        let mut buf = BytesMut::new();
        SendOffsetsResp::Ok.encode(&mut buf).unwrap();
        assert_eq!(SendOffsetsResp::decode(&buf).unwrap(), SendOffsetsResp::Ok);

        let err = SendOffsetsResp::Err {
            message: "consumer subscription not found".into(),
        };
        let mut buf = BytesMut::new();
        err.encode(&mut buf).unwrap();
        assert_eq!(SendOffsetsResp::decode(&buf).unwrap(), err);
    }

    #[test]
    fn test_truncated() {
        let req = SendOffsetsReq {
            producer_cluster: "txn".into(),
            subscription_id: 1,
            group_id: "g".into(),
            offsets: vec![("t".into(), 0, 0)],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf).unwrap();
        let truncated = &buf[..buf.len() - 4];
        assert!(matches!(
            SendOffsetsReq::decode(truncated),
            Err(PayloadError::Truncated)
        ));
    }
}
