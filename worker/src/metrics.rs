//! 极简 Prometheus 指标收集 + HTTP 暴露。
//!
//! Phase 2.7 MVP：手写 counter，无外部 metrics 框架。后续若需要 histogram/summary
//! 再切到 `prometheus` crate。
//!
//! HTTP 协议层只识别 `GET /metrics`，其余路径一律 404。无 Keep-Alive、无 TLS、
//! 不解析 header，只读到第一个 `\r\n` 就响应。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

#[derive(Debug, Default)]
pub struct Metrics {
    pub ipc_frames_total: AtomicU64,
    pub ipc_connections_total: AtomicU64,

    pub produce_fnf_total: AtomicU64,
    pub produce_fnf_failed_total: AtomicU64,

    pub produce_req_total: AtomicU64,
    pub produce_resp_ok_total: AtomicU64,
    pub produce_resp_err_total: AtomicU64,

    pub frames_dropped_draining_total: AtomicU64,

    // J: Consumer 背压指标——跨 subscription 求和（不带 label）。
    // per-subscription 粒度仍可通过 `KafkaConsumer::backpressure_stats()` API 在
    // 业务侧排障使用；这些 counter 是给 Prometheus 长期趋势监控的。
    pub consumer_pause_total: AtomicU64,
    pub consumer_resume_total: AtomicU64,
    pub consumer_messages_dropped_total: AtomicU64,
    pub consumer_stream_errors_total: AtomicU64,

    started_at: once_cell::sync::OnceCell<Instant>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let m = Self::default();
        let _ = m.started_at.set(Instant::now());
        Arc::new(m)
    }

    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at
            .get()
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0)
    }

    pub fn to_prometheus_text(&self) -> String {
        let mut s = String::with_capacity(1024);
        let pkg_ver = env!("CARGO_PKG_VERSION");

        macro_rules! emit_counter {
            ($name:literal, $help:literal, $field:ident) => {
                s.push_str(concat!("# HELP ", $name, " ", $help, "\n"));
                s.push_str(concat!("# TYPE ", $name, " counter\n"));
                s.push_str(&format!(
                    "{} {}\n",
                    $name,
                    self.$field.load(Ordering::Relaxed)
                ));
            };
        }

        s.push_str(&format!(
            "# HELP hi_kafka_worker_uptime_seconds Seconds since worker start\n\
             # TYPE hi_kafka_worker_uptime_seconds gauge\n\
             hi_kafka_worker_uptime_seconds {}\n",
            self.uptime_seconds()
        ));
        s.push_str(&format!(
            "# HELP hi_kafka_worker_info Build info\n\
             # TYPE hi_kafka_worker_info gauge\n\
             hi_kafka_worker_info{{version=\"{}\"}} 1\n",
            pkg_ver
        ));

        emit_counter!(
            "hi_kafka_ipc_frames_total",
            "Total IPC frames received from PHP",
            ipc_frames_total
        );
        emit_counter!(
            "hi_kafka_ipc_connections_total",
            "Total IPC connections accepted",
            ipc_connections_total
        );
        emit_counter!(
            "hi_kafka_produce_fnf_total",
            "Total PRODUCE_FNF frames received",
            produce_fnf_total
        );
        emit_counter!(
            "hi_kafka_produce_fnf_failed_total",
            "Total PRODUCE_FNF that failed at producer layer",
            produce_fnf_failed_total
        );
        emit_counter!(
            "hi_kafka_produce_req_total",
            "Total PRODUCE_REQ frames received",
            produce_req_total
        );
        emit_counter!(
            "hi_kafka_produce_resp_ok_total",
            "Total successful PRODUCE_RESP sent",
            produce_resp_ok_total
        );
        emit_counter!(
            "hi_kafka_produce_resp_err_total",
            "Total error PRODUCE_RESP sent",
            produce_resp_err_total
        );
        emit_counter!(
            "hi_kafka_frames_dropped_draining_total",
            "Total frames dropped because worker was draining",
            frames_dropped_draining_total
        );

        emit_counter!(
            "hi_kafka_consumer_pause_total",
            "Total auto-pause transitions triggered by backpressure (sum across subscriptions)",
            consumer_pause_total
        );
        emit_counter!(
            "hi_kafka_consumer_resume_total",
            "Total auto-resume transitions after backpressure relief (sum across subscriptions)",
            consumer_resume_total
        );
        emit_counter!(
            "hi_kafka_consumer_messages_dropped_total",
            "Total consumer messages dropped due to hard buffer overflow (sum across subscriptions)",
            consumer_messages_dropped_total
        );
        emit_counter!(
            "hi_kafka_consumer_stream_errors_total",
            "Total rdkafka stream errors fatal+non-fatal (sum across subscriptions)",
            consumer_stream_errors_total
        );

        s
    }
}

/// 启动指标 HTTP 服务。返回的 task 持续运行直到 listener 出错或 task 被 drop。
pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "metrics endpoint listening at /metrics");
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let m = metrics.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_request(stream, m).await {
                        debug!(error = ?e, "metrics request error");
                    }
                });
            }
            Err(e) => {
                error!(error = ?e, "metrics accept failed");
                return Err(e.into());
            }
        }
    }
}

async fn handle_request(mut stream: TcpStream, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    // 只读到第一个 \r\n 就够了
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await??;
    let line = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let first_line = line.split_once("\r\n").map(|x| x.0).unwrap_or(line);

    let (status, body, content_type) = if first_line.starts_with("GET /metrics") {
        let body = metrics.to_prometheus_text();
        ("200 OK", body, "text/plain; version=0.0.4; charset=utf-8")
    } else if first_line.starts_with("GET /healthz") {
        ("200 OK", "ok\n".to_string(), "text/plain")
    } else {
        ("404 Not Found", "not found\n".to_string(), "text/plain")
    };

    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_prometheus_text_contains_all_counters() {
        let m = Metrics::new();
        Metrics::inc(&m.produce_req_total);
        Metrics::inc(&m.produce_req_total);
        let text = m.to_prometheus_text();
        assert!(text.contains("hi_kafka_produce_req_total 2"));
        assert!(text.contains("hi_kafka_worker_uptime_seconds"));
        assert!(text.contains("hi_kafka_worker_info{version="));
        assert!(text.contains("# TYPE hi_kafka_produce_resp_ok_total counter"));
    }

    #[test]
    fn test_consumer_backpressure_counters_in_prometheus() {
        let m = Metrics::new();
        Metrics::inc(&m.consumer_pause_total);
        Metrics::inc(&m.consumer_pause_total);
        Metrics::inc(&m.consumer_resume_total);
        Metrics::inc(&m.consumer_messages_dropped_total);
        Metrics::inc(&m.consumer_stream_errors_total);
        Metrics::inc(&m.consumer_stream_errors_total);
        Metrics::inc(&m.consumer_stream_errors_total);
        let text = m.to_prometheus_text();
        assert!(text.contains("hi_kafka_consumer_pause_total 2"));
        assert!(text.contains("hi_kafka_consumer_resume_total 1"));
        assert!(text.contains("hi_kafka_consumer_messages_dropped_total 1"));
        assert!(text.contains("hi_kafka_consumer_stream_errors_total 3"));
        // 4 个 counter 都应有 # TYPE 元行
        for c in [
            "hi_kafka_consumer_pause_total",
            "hi_kafka_consumer_resume_total",
            "hi_kafka_consumer_messages_dropped_total",
            "hi_kafka_consumer_stream_errors_total",
        ] {
            assert!(
                text.contains(&format!("# TYPE {c} counter")),
                "missing TYPE for {c}"
            );
        }
    }

    #[test]
    fn test_counter_starts_at_zero() {
        let m = Metrics::new();
        let text = m.to_prometheus_text();
        assert!(text.contains("hi_kafka_ipc_frames_total 0"));
    }

    #[tokio::test]
    async fn test_http_serve_get_metrics() {
        use tokio::io::AsyncReadExt;
        let m = Metrics::new();
        Metrics::inc(&m.ipc_frames_total);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let m_clone = m.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_request(stream, m_clone).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let s = String::from_utf8(resp).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK"));
        assert!(s.contains("hi_kafka_ipc_frames_total 1"));
    }
}
