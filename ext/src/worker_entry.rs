//! Worker 在 .so 内的入口点。
//!
//! 用 `libc::fork()` 拉起 worker 时，父进程立刻返回 PHP，
//! 子进程直接调 [`run_in_child`]，在同一个 .so 镜像内启动 tokio + server，
//! **不再 exec 外部二进制**。
//!
//! 子进程做的事：
//! 1. `setsid()` 脱离 PHP 会话
//! 2. 关 stdin/stdout/stderr（或重定向到日志文件）
//! 3. 重置 SIGTERM/SIGINT 默认行为
//! 4. 构造 tokio runtime
//! 5. 启动 `Server` 跑 IPC + producer + consumer
//! 6. 收到 SIGTERM 后 drain + flush + 退出
//!
//! **重要**：子进程**不要调任何 PHP 函数**——继承下来的 PHP 状态可能已经不一致。

use std::ffi::CString;
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
pub struct ChildConfig {
    pub socket: PathBuf,
    pub brokers: Option<String>,
    pub log_level: String,
    pub log_file: Option<PathBuf>,
    pub drain_timeout: Duration,
    pub metrics_addr: Option<std::net::SocketAddr>,
}

// `ChildConfig` 的构造由 `spawn.rs::to_child_config` 完成（从已解析的 `SpawnConfig`
// 字段转发）。原来这里有过一个 `from_env(socket)` 全部从环境变量直读的版本，
// 但 fork-in-MINIT 路径上 SpawnConfig 已经在父进程里解析过环境变量，没必要再读一遍，
// 已删除。

/// **不返回**——子进程在此处完成所有工作并 `_exit()`。
///
/// 必须确保进入此函数前是 fork 之后的子进程语境（即 `libc::fork()` 返回 0 的分支）。
///
/// # Safety
///
/// - 调用方负责确保已在子进程语境
/// - 调用方负责在 fork 后 setsid 之前不要调用任何 PHP 函数
pub unsafe fn run_in_child(config: ChildConfig) -> ! {
    // 1. 脱离 PHP 会话
    unsafe { libc::setsid() };

    // 2. 改进程名让 pgrep/ps 能识别（最长 15 字符 + null）
    #[cfg(target_os = "linux")]
    unsafe {
        let name = b"hi-kafka-worker\0";
        libc::prctl(libc::PR_SET_NAME, name.as_ptr() as libc::c_ulong);
        // 不设置 PR_SET_PDEATHSIG —— worker 应在父死后继续活，
        // 由后续 PHP 进程通过 UDS 复用，不被 PID 1 杀掉
    }

    // Y1: macOS 上 `prctl` 不存在。
    //
    // 我们走 `pthread_setname_np` 改主线程名——Activity Monitor / Instruments /
    // 调试器（lldb）能识别。**但 `ps -ef` 不行**：macOS kernel 给每个进程维护
    // 独立的 procinfo commandline buffer，与 user-space 的 `_NSGetArgv()` 完全
    // 隔离。覆盖 user-space argv[0] 字符串内存对 `ps -ef` 显示无效（实测验证）。
    // Darwin 有 `proc_setname_np` 私有 SPI 但 Apple 不保证 ABI 稳定。
    //
    // 实际效果：macOS 上 `ps -ef` 看 worker 仍像 PHP 进程；运维定位时**用
    // `pgrep -f /tmp/.*\.sock`** 按 socket 路径找，或用 `scripts/cleanup-ghosts.sh`
    // 一键清理。
    #[cfg(target_os = "macos")]
    unsafe {
        let new_name = b"hi-kafka-worker\0";
        libc::pthread_setname_np(new_name.as_ptr() as *const _);
    }

    // 3. 重定向 stdio
    redirect_stdio(config.log_file.as_deref());

    // 4. 重置信号处理器，避免继承 PHP 的奇怪 handlers
    for sig in &[libc::SIGTERM, libc::SIGINT, libc::SIGHUP, libc::SIGPIPE] {
        unsafe { libc::signal(*sig, libc::SIG_DFL) };
    }

    // 5. 初始化日志
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .try_init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        socket = %config.socket.display(),
        "hi-kafka worker (embedded in .so) starting"
    );

    // 6. 构造 tokio runtime
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .thread_name("hi-kafka")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = ?e, "build tokio runtime failed");
            unsafe { libc::_exit(1) };
        }
    };

    // 7. 跑 server
    let result = rt.block_on(run_server(config));

    if let Err(e) = result {
        error!(error = ?e, "worker exited with error");
        unsafe { libc::_exit(1) };
    }

    // 用 _exit 跳过 atexit handlers（可能继承自 PHP）
    unsafe { libc::_exit(0) };
}

async fn run_server(config: ChildConfig) -> anyhow::Result<()> {
    use hi_kafka_worker::{
        cluster::ClusterRegistry, metrics, server, shutdown::ShutdownState, Metrics,
    };

    let pid_file = pid_file_for(&config.socket);
    let _ = write_pid_file(&pid_file);

    let registry = ClusterRegistry::new();

    // 兼容：若旧式 brokers 存在（标准用法是 PHP 端 registerCluster），
    // 启动期把 'default' 集群预注册一份，方便老业务无缝迁移。
    if let Some(brokers) = config.brokers.as_deref() {
        let mut cfg = std::collections::HashMap::new();
        cfg.insert("bootstrap.servers".to_string(), brokers.to_string());
        registry.register("default".to_string(), cfg).await;
        info!(%brokers, "preregistered 'default' cluster from env");
    }

    let producer = build_producer(registry.clone());
    let shutdown = ShutdownState::new();
    let m = Metrics::new();
    let consumer = build_consumer(registry.clone(), m.clone());

    if let Some(addr) = config.metrics_addr {
        let m_clone = m.clone();
        tokio::spawn(async move {
            if let Err(e) = metrics::serve(addr, m_clone).await {
                error!(error = ?e, "metrics endpoint exited");
            }
        });
    }

    let mut sig_term = signal(SignalKind::terminate())?;
    let mut sig_int = signal(SignalKind::interrupt())?;

    // 内嵌 worker：无连接持续空闲超时则自退，解决主进程退出后 worker 残留。
    // 默认 5min；HI_KAFKA_IDLE_TIMEOUT_MS=0 可禁用（回到常驻）。
    let idle_timeout = std::env::var("HI_KAFKA_IDLE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(300));
    let server = server::Server::bind_with(
        &config.socket,
        producer.clone(),
        consumer,
        registry,
        shutdown.clone(),
        m,
        idle_timeout,
    )
    .await?;

    // P0 #1：同 main.rs，信号触发 shutdown，server.run 自己 drain。
    let shutdown_for_sig = shutdown.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = sig_term.recv() => info!("SIGTERM received, draining..."),
            _ = sig_int.recv() => info!("SIGINT received, draining..."),
        }
        shutdown_for_sig.start_draining();
    });

    if let Err(e) = server.run(config.drain_timeout).await {
        error!(error = ?e, "server exited with error");
    }
    if let Err(e) = producer.flush(config.drain_timeout).await {
        error!(error = ?e, "producer flush during shutdown failed");
    }

    let _ = std::fs::remove_file(&config.socket);
    let _ = std::fs::remove_file(pid_file_for(&config.socket));
    info!("hi-kafka worker (embedded) stopped");
    Ok(())
}

fn pid_file_for(socket: &std::path::Path) -> std::path::PathBuf {
    let mut p = socket.to_path_buf();
    let stem = p.file_name().map(|s| s.to_os_string()).unwrap_or_default();
    let mut name = stem;
    name.push(".pid");
    p.set_file_name(name);
    p
}

fn write_pid_file(path: &std::path::Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "{}", std::process::id())
}

// graceful_drain 已经废弃：server.run 现在自己负责 drain connection task，
// producer.flush 由调用方在 server.run 返回之后再做一次。

fn redirect_stdio(log_file: Option<&std::path::Path>) {
    // 关掉继承自 PHP 的 stdio
    let dev_null = match OpenOptions::new().read(true).write(true).open("/dev/null") {
        Ok(f) => f,
        Err(_) => return,
    };
    unsafe {
        libc::dup2(dev_null.as_raw_fd(), libc::STDIN_FILENO);
    }

    let out_fd = match log_file {
        Some(path) => match OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => Some(f),
            Err(_) => None,
        },
        None => None,
    };

    let target_fd = out_fd
        .as_ref()
        .map(|f| f.as_raw_fd())
        .unwrap_or_else(|| dev_null.as_raw_fd());

    unsafe {
        libc::dup2(target_fd, libc::STDOUT_FILENO);
        libc::dup2(target_fd, libc::STDERR_FILENO);
    }

    // 关闭其它从 PHP 继承的 fd（保守保留 0/1/2 和 socket 监听用的）
    // PHP-FPM 通常会自己处理；这里只清理 stdio。
    drop(dev_null);
    drop(out_fd);
}

#[cfg(feature = "kafka")]
fn build_producer(
    registry: hi_kafka_worker::ClusterRegistryHandle,
) -> hi_kafka_worker::ProducerHandle {
    use hi_kafka_worker::producer::KafkaProducer;
    use std::sync::Arc;
    Arc::new(KafkaProducer::new(registry))
}

#[cfg(not(feature = "kafka"))]
fn build_producer(
    _registry: hi_kafka_worker::ClusterRegistryHandle,
) -> hi_kafka_worker::ProducerHandle {
    hi_kafka_worker::producer::logging()
}

#[cfg(feature = "kafka")]
fn build_consumer(
    registry: hi_kafka_worker::ClusterRegistryHandle,
    metrics: std::sync::Arc<hi_kafka_worker::Metrics>,
) -> hi_kafka_worker::ConsumerHandle {
    use hi_kafka_worker::consumer::{KafkaConsumer, KafkaConsumerConfig};
    use std::sync::Arc;
    Arc::new(KafkaConsumer::new(registry, KafkaConsumerConfig::default()).with_metrics(metrics))
}

#[cfg(not(feature = "kafka"))]
fn build_consumer(
    _registry: hi_kafka_worker::ClusterRegistryHandle,
    _metrics: std::sync::Arc<hi_kafka_worker::Metrics>,
) -> hi_kafka_worker::ConsumerHandle {
    hi_kafka_worker::consumer::logging()
}

// 抑制 CString import 警告（保留供未来使用）
#[allow(dead_code)]
fn _unused_cstring(_s: &str) -> Option<CString> {
    None
}
