use anyhow::Context;
use clap::Parser;
use hi_kafka_worker::{Metrics, cluster::ClusterRegistry, metrics, server, shutdown::ShutdownState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "hi-kafka-worker", version)]
struct Cli {
    /// Unix socket 监听路径
    #[arg(long, env = "HI_KAFKA_SOCKET", default_value = "/tmp/hi-kafka.sock")]
    socket: PathBuf,

    /// 日志级别
    #[arg(long, env = "HI_KAFKA_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// 默认集群的 broker 列表（逗号分隔）。仅在编译时启用 `kafka` feature 才生效。
    /// 不指定则使用 LoggingProducer（dry-run，不投递）。
    #[arg(long, env = "HI_KAFKA_BROKERS")]
    brokers: Option<String>,

    /// 收到 SIGTERM/SIGINT 后 drain 在途消息的最大等待时间（毫秒）
    #[arg(long, env = "HI_KAFKA_DRAIN_TIMEOUT_MS", default_value_t = 10_000)]
    drain_timeout_ms: u64,

    /// Prometheus 指标 HTTP 端点地址。空字符串则禁用。
    #[arg(long, env = "HI_KAFKA_METRICS_ADDR", default_value = "127.0.0.1:9876")]
    metrics_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&cli.log_level).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(true)
        .init();

    info!(version = env!("CARGO_PKG_VERSION"), socket = %cli.socket.display(), "hi-kafka-worker starting");

    let mut sig_term = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut sig_int = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

    let registry = ClusterRegistry::new();
    if let Some(brokers) = cli.brokers.as_deref() {
        let mut cfg = std::collections::HashMap::new();
        cfg.insert("bootstrap.servers".to_string(), brokers.to_string());
        registry.register("default".to_string(), cfg).await;
        info!(%brokers, "preregistered 'default' cluster from --brokers");
    }
    let producer = build_producer(registry.clone());
    let consumer = build_consumer(registry.clone());
    let shutdown = ShutdownState::new();
    let m = Metrics::new();

    if !cli.metrics_addr.is_empty() {
        match cli.metrics_addr.parse::<SocketAddr>() {
            Ok(addr) => {
                let m_clone = m.clone();
                tokio::spawn(async move {
                    if let Err(e) = metrics::serve(addr, m_clone).await {
                        error!(error = ?e, "metrics endpoint exited");
                    }
                });
            }
            Err(e) => {
                error!(error = ?e, addr = %cli.metrics_addr, "invalid metrics-addr, skipping");
            }
        }
    }

    let server = server::Server::bind_with(
        &cli.socket,
        producer.clone(),
        consumer,
        registry,
        shutdown.clone(),
        m.clone(),
    )
    .await
    .context("bind UDS")?;

    let drain_timeout = Duration::from_millis(cli.drain_timeout_ms);

    // P0 #1：信号触发 shutdown，server.run 自己 drain 全部 in-flight connection task。
    // 不能用 `tokio::select!{ server.run() vs signal }`——会在信号到来时把 server.run
    // 当场 drop，spawn 出去的 connection task 后续被 runtime abort，已 ack 但未写回
    // PRODUCE_RESP 的 cid 永久丢失。
    let shutdown_for_sig = shutdown.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = sig_term.recv() => info!("SIGTERM received, draining..."),
            _ = sig_int.recv() => info!("SIGINT received, draining..."),
        }
        shutdown_for_sig.start_draining();
    });

    let started = std::time::Instant::now();
    if let Err(e) = server.run(drain_timeout).await {
        error!(error = ?e, "server exited with error");
    }

    // Server 已 drain 完所有 connection task（已 ack 的响应都写回了）。
    // 现在再 flush producer 内部队列，把 fnf 路径上还没 ack 的也送出去。
    let flush_started = std::time::Instant::now();
    if let Err(e) = producer.flush(drain_timeout).await {
        error!(error = ?e, "producer flush during shutdown failed");
    }
    info!(
        drain_ms = started.elapsed().as_millis() as u64,
        flush_ms = flush_started.elapsed().as_millis() as u64,
        "drain complete"
    );

    if let Err(e) = std::fs::remove_file(&cli.socket) {
        if e.kind() != std::io::ErrorKind::NotFound {
            error!(error = ?e, path = %cli.socket.display(), "failed to remove socket file");
        }
    }

    info!("hi-kafka-worker stopped");
    Ok(())
}

#[cfg(feature = "kafka")]
fn build_producer(registry: hi_kafka_worker::ClusterRegistryHandle) -> hi_kafka_worker::ProducerHandle {
    use hi_kafka_worker::producer::KafkaProducer;
    use std::sync::Arc;
    Arc::new(KafkaProducer::new(registry))
}

#[cfg(not(feature = "kafka"))]
fn build_producer(_registry: hi_kafka_worker::ClusterRegistryHandle) -> hi_kafka_worker::ProducerHandle {
    tracing::warn!("kafka feature disabled; using LoggingProducer");
    hi_kafka_worker::producer::logging()
}

#[cfg(feature = "kafka")]
fn build_consumer(registry: hi_kafka_worker::ClusterRegistryHandle) -> hi_kafka_worker::ConsumerHandle {
    use hi_kafka_worker::consumer::{KafkaConsumer, KafkaConsumerConfig};
    use std::sync::Arc;
    Arc::new(KafkaConsumer::new(registry, KafkaConsumerConfig::default()))
}

#[cfg(not(feature = "kafka"))]
fn build_consumer(_registry: hi_kafka_worker::ClusterRegistryHandle) -> hi_kafka_worker::ConsumerHandle {
    hi_kafka_worker::consumer::logging()
}
