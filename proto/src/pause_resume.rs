//! Pause / resume per-partition flow control（Phase 3.14）。
//!
//! Pause 后 librdkafka 内部 fetcher 停止从该分区拉取，但 partition assignment 保留；
//! 不会触发 rebalance，consumer 心跳照常。Resume 即恢复 fetch，从上次位置继续。
//!
//! 典型场景：
//! - 下游写入慢 → pause 输入分区直到下游赶上（partition-level 背压）
//! - 某分区 schema 解析持续失败 → pause 让人工介入
//! - DLT 重放期间暂停主流
//!
//! ## PauseResumeReq
//!
//! ```text
//! [u64 subscription_id]
//! [u8 op]   // 0 = Pause, 1 = Resume
//! [u32 partitions_count]
//! 重复 partitions_count 次：
//!   [u16 topic_len][topic][i32 partition]
//! ```
//!
//! ## PauseResumeResp
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PauseResumeOp {
    Pause = 0,
    Resume = 1,
}

impl PauseResumeOp {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Pause,
            1 => Self::Resume,
            _ => return None,
        })
    }
}

// 复用 seek::PartitionSpec (= (String, i32))，不在本模块里重复定义类型别名。

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PauseResumeReq {
    pub subscription_id: u64,
    pub op: PauseResumeOp,
    /// (topic, partition) 列表。空数组合法 → 上层语义为「应用到当前 assignment 全部分区」。
    pub partitions: Vec<(String, i32)>,
}

impl PauseResumeReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        buf.put_u8(self.op as u8);
        if self.partitions.len() > u32::MAX as usize {
            return Err(PayloadError::FieldTooLarge(self.partitions.len()));
        }
        buf.put_u32(self.partitions.len() as u32);
        for (topic, partition) in &self.partitions {
            write_str_u16(topic, buf)?;
            buf.put_i32(*partition);
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if buf.remaining() < 9 {
            return Err(PayloadError::Truncated);
        }
        let subscription_id = buf.get_u64();
        let op_byte = buf.get_u8();
        let op = PauseResumeOp::from_u8(op_byte).ok_or(PayloadError::InvalidTag {
            tag: op_byte,
            field: "pause_resume_op",
        })?;
        if buf.remaining() < 4 {
            return Err(PayloadError::Truncated);
        }
        let count = buf.get_u32() as usize;
        // cap 预分配，u32::MAX 的恶意 count 会撑爆 with_capacity；
        // 单 subscription 分区数实际几千封顶，8192 留余量。
        let mut partitions = Vec::with_capacity(count.min(8192));
        for _ in 0..count {
            let topic = read_str_u16(&mut buf, "topic")?;
            if buf.remaining() < 4 {
                return Err(PayloadError::Truncated);
            }
            let partition = buf.get_i32();
            partitions.push((topic, partition));
        }
        Ok(Self {
            subscription_id,
            op,
            partitions,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PauseResumeResp {
    Ok,
    Err { message: String },
}

impl PauseResumeResp {
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
                field: "pause_resume_resp",
            }),
        }
    }
}

// helpers (与其它模块一致)
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
    fn test_pause_roundtrip() {
        let original = PauseResumeReq {
            subscription_id: 42,
            op: PauseResumeOp::Pause,
            partitions: vec![("topic-a".into(), 0), ("topic-a".into(), 1)],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(PauseResumeReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_resume_roundtrip() {
        let original = PauseResumeReq {
            subscription_id: 1,
            op: PauseResumeOp::Resume,
            partitions: vec![("t".into(), 0)],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(PauseResumeReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_empty_partitions() {
        let original = PauseResumeReq {
            subscription_id: 7,
            op: PauseResumeOp::Pause,
            partitions: vec![],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(PauseResumeReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_invalid_op() {
        let mut buf = BytesMut::new();
        buf.put_u64(1);
        buf.put_u8(99);
        buf.put_u32(0);
        let err = PauseResumeReq::decode(&buf).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidTag { tag: 99, .. }));
    }

    #[test]
    fn test_resp_ok_err() {
        let mut buf = BytesMut::new();
        PauseResumeResp::Ok.encode(&mut buf).unwrap();
        assert_eq!(PauseResumeResp::decode(&buf).unwrap(), PauseResumeResp::Ok);

        let err = PauseResumeResp::Err {
            message: "subscription not found".into(),
        };
        let mut buf = BytesMut::new();
        err.encode(&mut buf).unwrap();
        assert_eq!(PauseResumeResp::decode(&buf).unwrap(), err);
    }
}
