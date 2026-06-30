//! worker 内部结构化错误。
//!
//! 各 handler / producer / consumer 失败时携带一个 [`ErrorKind`]，经
//! `anyhow::Error::new(WorkerError)` 在调用链上传递；server 出口用
//! [`WorkerError::from_anyhow`] 在错误链里找回它，统一编成
//! [`FrameType::Error`](hi_kafka_proto::FrameType::Error) 帧回 PHP——
//! 让业务侧拿到机器可读分类，而非 `str_contains(message)` 猜。

use hi_kafka_proto::{ErrorKind, ErrorResp};

#[derive(Debug, Clone)]
pub struct WorkerError {
    pub kind: ErrorKind,
    pub retryable: bool,
    /// 原生 librdkafka `rd_kafka_resp_err_t`；无则 0。
    pub native_code: i32,
    pub message: String,
}

impl WorkerError {
    /// 用 kind 的默认 retryable + 无 native_code 构造。
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            retryable: kind.default_retryable(),
            native_code: 0,
            message: message.into(),
        }
    }

    pub fn to_error_resp(&self) -> ErrorResp {
        ErrorResp {
            kind: self.kind,
            retryable: self.retryable,
            native_code: self.native_code,
            message: self.message.clone(),
        }
    }

    /// 从 anyhow 错误链里找回 `WorkerError`（穿透 `.context()` 包装）；
    /// 找不到则按 `Internal` 兜底，message 用整条链的 debug 串。
    pub fn from_anyhow(err: &anyhow::Error) -> ErrorResp {
        for cause in err.chain() {
            if let Some(we) = cause.downcast_ref::<WorkerError>() {
                return we.to_error_resp();
            }
        }
        ErrorResp::new(ErrorKind::Internal, format!("{err:#}"))
    }
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for WorkerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_anyhow_extracts_kind() {
        let e = anyhow::Error::new(WorkerError::new(ErrorKind::ClusterNotRegistered, "nope"));
        assert_eq!(
            WorkerError::from_anyhow(&e).kind,
            ErrorKind::ClusterNotRegistered
        );
    }

    #[test]
    fn test_from_anyhow_fallback_internal() {
        let e = anyhow::anyhow!("some random error");
        assert_eq!(WorkerError::from_anyhow(&e).kind, ErrorKind::Internal);
    }

    #[test]
    fn test_from_anyhow_through_context() {
        use anyhow::Context;
        let e = Err::<(), _>(WorkerError::new(ErrorKind::Timeout, "t"))
            .context("outer wrapping")
            .unwrap_err();
        // chain() 能穿透 context 找回底层 WorkerError
        assert_eq!(WorkerError::from_anyhow(&e).kind, ErrorKind::Timeout);
    }

    #[test]
    fn test_native_code_preserved() {
        let we = WorkerError {
            kind: ErrorKind::BrokerRetryable,
            retryable: true,
            native_code: -184,
            message: "queue full".into(),
        };
        let resp = we.to_error_resp();
        assert_eq!(resp.native_code, -184);
        assert!(resp.retryable);
    }
}
