//! worker 生命周期自退 e2e（无需 Kafka，走 LoggingConsumer/Producer）。
//!
//! 覆盖两类机制：
//! 1. **主动退出**：客户端发 `Goodbye` 后关连接 → 若已是最后一个连接且无活跃订阅，
//!    worker 立即 drain（不等 idle 超时）。
//! 2. **共享 worker 安全**：收到 Goodbye 但仍有其它连接在用 → 绝不退。
//! 3. **idle 自退**：无 Goodbye、连接全断、持续空闲超过 idle_timeout → 自退。

use bytes::BytesMut;
use hi_kafka_proto::{encode_frame, FrameType, HelloReq, HEADER_LEN, PROTOCOL_MAJOR};
use hi_kafka_worker::{cluster::ClusterRegistry, shutdown::ShutdownState, Metrics, Server};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

static SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_socket() -> PathBuf {
    let pid = std::process::id();
    let seq = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hi-kafka-lifecycle-{pid}-{seq}-{ns}.sock"))
}

/// 起一个内存 worker，返回 (socket, 可观测的 shutdown handle)。
async fn start_worker(idle_timeout: Duration) -> (PathBuf, std::sync::Arc<ShutdownState>) {
    let socket = temp_socket();
    let shutdown = ShutdownState::new();
    let shutdown_obs = shutdown.clone();
    let server = Server::bind_with(
        &socket,
        hi_kafka_worker::producer::logging(),
        hi_kafka_worker::consumer::logging(),
        ClusterRegistry::new(),
        shutdown,
        Metrics::new(),
        idle_timeout,
    )
    .await
    .unwrap();
    tokio::spawn(server.run(Duration::from_secs(5)));
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "socket not ready");
    (socket, shutdown_obs)
}

async fn connect_handshake(socket: &PathBuf) -> UnixStream {
    let mut client = UnixStream::connect(socket).await.unwrap();
    let mut payload = BytesMut::new();
    HelloReq {
        major: PROTOCOL_MAJOR,
    }
    .encode(&mut payload)
    .unwrap();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::Hello, 0, &payload, &mut frame).unwrap();
    client.write_all(&frame).await.unwrap();
    client.flush().await.unwrap();
    let mut resp = vec![0u8; HEADER_LEN + 1];
    client.read_exact(&mut resp).await.unwrap();
    assert_eq!(resp[4], FrameType::Hello as u8);
    client
}

async fn send_goodbye(client: &mut UnixStream) {
    let mut frame = BytesMut::new();
    encode_frame(FrameType::Goodbye, 0, &[], &mut frame).unwrap();
    client.write_all(&frame).await.unwrap();
    client.flush().await.unwrap();
}

/// 轮询等待 draining 置位；最多 `timeout`。返回是否在期限内 drain。
async fn wait_draining(shutdown: &ShutdownState, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if shutdown.is_draining() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.is_draining()
}

/// Goodbye + 关连接（最后一个连接）→ 立即 drain。idle_timeout 设大，证明退出走的是
/// 主动路径而非 idle。
#[tokio::test]
async fn test_goodbye_triggers_active_exit_when_last_connection() {
    let (socket, shutdown) = start_worker(Duration::from_secs(60)).await;

    let mut client = connect_handshake(&socket).await;
    send_goodbye(&mut client).await;
    drop(client); // 关连接 → 最后一个连接关闭

    assert!(
        wait_draining(&shutdown, Duration::from_secs(3)).await,
        "worker 应在收到 Goodbye 且最后连接关闭后立即 drain（idle_timeout=60s 不可能触发）"
    );
    let _ = std::fs::remove_file(&socket);
}

/// 共享 worker 安全：收到 Goodbye 但仍有其它连接在用 → 不退；待其也关闭才退。
#[tokio::test]
async fn test_goodbye_keeps_worker_while_other_connection_alive() {
    let (socket, shutdown) = start_worker(Duration::from_secs(60)).await;

    let mut client_a = connect_handshake(&socket).await;
    let client_b = connect_handshake(&socket).await; // 保持存活

    send_goodbye(&mut client_a).await;
    drop(client_a); // A 告别并断开，但 B 仍在

    // B 还连着 → 绝不能退。
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        !shutdown.is_draining(),
        "仍有其它连接在用时，worker 不能因单个进程 Goodbye 而退出"
    );

    // B 也断开 → 此时才是最后一个连接，expedited 已置位 → drain。
    drop(client_b);
    assert!(
        wait_draining(&shutdown, Duration::from_secs(3)).await,
        "最后一个连接关闭后 worker 应 drain"
    );
    let _ = std::fs::remove_file(&socket);
}

/// 无 Goodbye：连接断开后持续空闲超过 idle_timeout → idle 自退。
#[tokio::test]
async fn test_idle_exit_without_goodbye() {
    let (socket, shutdown) = start_worker(Duration::from_millis(300)).await;

    let client = connect_handshake(&socket).await;
    drop(client); // 直接断开，不发 Goodbye

    assert!(
        wait_draining(&shutdown, Duration::from_secs(5)).await,
        "无连接持续空闲超过 idle_timeout 后应 idle 自退"
    );
    let _ = std::fs::remove_file(&socket);
}
