//! 推送 SASL/OAUTHBEARER token（Phase 3.17）。
//!
//! ## 背景
//!
//! 云上 Kafka（Confluent Cloud、Aiven、MSK、阿里云/华为云）几乎都用 OIDC
//! 短时 token 鉴权。token 通常 1h 过期，必须周期刷新——librdkafka 自身不会
//! 主动去 STS/OIDC 取 token，需要应用通过 `oauthbearer_token_refresh_cb`
//! 喂给它。
//!
//! ## 推送模型而非回调模型
//!
//! 直接把 token 拉取逻辑放到 worker 进程里有两个麻烦：
//! 1. worker 不带 HTTP 客户端、不带证书/凭据（Cloud SDK），引入大量依赖
//! 2. 多语言、多 Cloud 的 token 源各不相同，烧死在 worker 里不灵活
//!
//! 所以我们采用 **PHP 推、worker 存** 的模型：
//! - PHP 端用业务自己的方式（HTTP、k8s secret、IAM SDK 等）拿到 token
//! - PHP 调 `setOAuthBearerToken(cluster, token, lifetimeMs, principal, extensions)`
//! - worker 把 token 存到 `ClusterRegistry` 的 per-cluster slot
//! - librdkafka 触发 `OAUTHBEARER_TOKEN_REFRESH` 时，worker 的 `ClientContext`
//!   回调直接读 slot 返回给 librdkafka
//!
//! PHP 侧的刷新策略由业务决定（cron / 监听 token 快过期事件等）。
//!
//! ## SetOAuthBearerTokenReq
//!
//! ```text
//! [u16 cluster_len][cluster]
//! [u32 token_len][token_value]
//! [i64 lifetime_ms]          // token 失效时间（unix epoch ms）
//! [u16 principal_len][principal_name]
//! [u32 ext_count]
//! 重复 ext_count 次：
//!   [u16 key_len][key][u16 val_len][val]
//! ```
//!
//! ## SetOAuthBearerTokenResp
//!
//! ```text
//! [u8 status]   // 0x00 Ok（无 payload），0x01 Err（[u16 msg_len][msg]）
//! ```

use bytes::{Buf, BufMut, BytesMut};

use crate::payload::PayloadError;

const STATUS_OK: u8 = 0x00;
const STATUS_ERR: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOAuthBearerTokenReq {
    pub cluster: String,
    pub token_value: String,
    /// Token 失效时间（unix epoch ms）。librdkafka 会按此值决定何时再次触发 refresh。
    pub lifetime_ms: i64,
    pub principal_name: String,
    /// SASL extension key=value 对。
    pub extensions: Vec<(String, String)>,
}

impl SetOAuthBearerTokenReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str_u16(&self.cluster, buf)?;
        write_str_u32(&self.token_value, buf)?;
        buf.put_i64(self.lifetime_ms);
        write_str_u16(&self.principal_name, buf)?;
        if self.extensions.len() > u32::MAX as usize {
            return Err(PayloadError::FieldTooLarge(self.extensions.len()));
        }
        buf.put_u32(self.extensions.len() as u32);
        for (k, v) in &self.extensions {
            write_str_u16(k, buf)?;
            write_str_u16(v, buf)?;
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str_u16(&mut buf, "cluster")?;
        let token_value = read_str_u32(&mut buf, "token_value")?;
        if buf.remaining() < 8 {
            return Err(PayloadError::Truncated);
        }
        let lifetime_ms = buf.get_i64();
        let principal_name = read_str_u16(&mut buf, "principal_name")?;
        if buf.remaining() < 4 {
            return Err(PayloadError::Truncated);
        }
        let ext_count = buf.get_u32() as usize;
        // P3: SASL extension 实际只用零星几个 kv，64 足够覆盖任何 mechanism。
        let mut extensions = Vec::with_capacity(ext_count.min(64));
        for _ in 0..ext_count {
            let k = read_str_u16(&mut buf, "ext_key")?;
            let v = read_str_u16(&mut buf, "ext_val")?;
            extensions.push((k, v));
        }
        Ok(Self {
            cluster,
            token_value,
            lifetime_ms,
            principal_name,
            extensions,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetOAuthBearerTokenResp {
    Ok,
    Err { message: String },
}

impl SetOAuthBearerTokenResp {
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
                field: "set_oauth_token_resp",
            }),
        }
    }
}

// helpers
fn write_str_u16(s: &str, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if s.len() > u16::MAX as usize {
        return Err(PayloadError::FieldTooLarge(s.len()));
    }
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
    Ok(())
}

fn write_str_u32(s: &str, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if s.len() > u32::MAX as usize {
        return Err(PayloadError::FieldTooLarge(s.len()));
    }
    buf.put_u32(s.len() as u32);
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

fn read_str_u32(buf: &mut &[u8], field: &'static str) -> Result<String, PayloadError> {
    if buf.remaining() < 4 {
        return Err(PayloadError::Truncated);
    }
    let len = buf.get_u32() as usize;
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
        let original = SetOAuthBearerTokenReq {
            cluster: "msk-prod".into(),
            token_value: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...".into(),
            lifetime_ms: 1_793_000_000_000,
            principal_name: "kafka-client@example.com".into(),
            extensions: vec![
                ("traceparent".into(), "00-abc-def-01".into()),
                ("tenant".into(), "tenant-42".into()),
            ],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(SetOAuthBearerTokenReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_req_no_extensions() {
        let original = SetOAuthBearerTokenReq {
            cluster: "c".into(),
            token_value: "tok".into(),
            lifetime_ms: 0,
            principal_name: "p".into(),
            extensions: vec![],
        };
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        assert_eq!(SetOAuthBearerTokenReq::decode(&buf).unwrap(), original);
    }

    #[test]
    fn test_resp_ok_err() {
        let mut buf = BytesMut::new();
        SetOAuthBearerTokenResp::Ok.encode(&mut buf).unwrap();
        assert_eq!(
            SetOAuthBearerTokenResp::decode(&buf).unwrap(),
            SetOAuthBearerTokenResp::Ok
        );

        let err = SetOAuthBearerTokenResp::Err {
            message: "cluster not registered".into(),
        };
        let mut buf = BytesMut::new();
        err.encode(&mut buf).unwrap();
        assert_eq!(SetOAuthBearerTokenResp::decode(&buf).unwrap(), err);
    }

    #[test]
    fn test_truncated() {
        let req = SetOAuthBearerTokenReq {
            cluster: "c".into(),
            token_value: "tok".into(),
            lifetime_ms: 0,
            principal_name: "p".into(),
            extensions: vec![("k".into(), "v".into())],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf).unwrap();
        let truncated = &buf[..buf.len() - 3];
        assert!(matches!(
            SetOAuthBearerTokenReq::decode(truncated),
            Err(PayloadError::Truncated)
        ));
    }
}
