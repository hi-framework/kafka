//! 事务操作协议（Phase 3.7）。
//!
//! ## TxnReq
//!
//! ```text
//! [u16 cluster_len][cluster]
//! [u8 op]   // 0 = Begin, 1 = Commit, 2 = Abort
//! ```
//!
//! ## TxnResp
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
pub enum TxnOp {
    Begin = 0,
    Commit = 1,
    Abort = 2,
}

impl TxnOp {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Begin,
            1 => Self::Commit,
            2 => Self::Abort,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnReq {
    pub cluster: String,
    pub op: TxnOp,
}

impl TxnReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.cluster, buf)?;
        buf.put_u8(self.op as u8);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str_u16(&mut buf, "cluster")?;
        if buf.remaining() < 1 {
            return Err(PayloadError::Truncated);
        }
        let op_byte = buf.get_u8();
        let op = TxnOp::from_u8(op_byte).ok_or(PayloadError::InvalidTag {
            tag: op_byte,
            field: "txn_op",
        })?;
        Ok(Self { cluster, op })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnResp {
    Ok,
    Err { message: String },
}

impl TxnResp {
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
                field: "txn_resp",
            }),
        }
    }
}

// 内部 helpers —— 与 payload.rs 中的同名函数一致，避免跨模块 pub
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
    fn test_txn_req_roundtrip() {
        for op in [TxnOp::Begin, TxnOp::Commit, TxnOp::Abort] {
            let original = TxnReq {
                cluster: "main".into(),
                op,
            };
            let mut buf = BytesMut::new();
            original.encode(&mut buf).unwrap();
            assert_eq!(TxnReq::decode(&buf).unwrap(), original);
        }
    }

    #[test]
    fn test_txn_req_invalid_op() {
        let mut buf = BytesMut::new();
        // 假装手工构造一个非法 op
        buf.put_u16(4);
        buf.put_slice(b"main");
        buf.put_u8(99);
        let err = TxnReq::decode(&buf).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidTag { tag: 99, .. }));
    }

    #[test]
    fn test_txn_resp_ok_err() {
        let mut buf = BytesMut::new();
        TxnResp::Ok.encode(&mut buf).unwrap();
        assert_eq!(TxnResp::decode(&buf).unwrap(), TxnResp::Ok);

        let err = TxnResp::Err {
            message: "abort failed: ConcurrentTransactions".into(),
        };
        let mut buf = BytesMut::new();
        err.encode(&mut buf).unwrap();
        assert_eq!(TxnResp::decode(&buf).unwrap(), err);
    }
}
