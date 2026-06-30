//! 验证 dispatch 层 panic guard：单帧 handler panic 后
//! 1. 当前连接被关闭（客户端 read EOF）
//! 2. worker 进程 / server 没挂
//! 3. 新连接能正常握手 + 接收业务帧

use async_trait::async_trait;
use bytes::BytesMut;
use hi_kafka_proto::{
    encode_frame, ConsumerMessage, FrameType, HelloReq, SubscribeReq, HEADER_LEN, PROTOCOL_MAJOR,
};
use hi_kafka_worker::{
    cluster::ClusterRegistry, shutdown::ShutdownState, Consumer, ConsumerError, ConsumerHandle,
    Metrics, Server, SubscriptionId,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
    std::env::temp_dir().join(format!("hi-kafka-panic-test-{pid}-{seq}-{ns}.sock"))
}

/// 故意 panic 的 Consumer——用于验证 dispatch 层的 catch_unwind。
struct PanicConsumer;

#[async_trait]
impl Consumer for PanicConsumer {
    async fn subscribe(&self, _req: SubscribeReq) -> Result<SubscriptionId, ConsumerError> {
        panic!("intentional panic from PanicConsumer::subscribe (testing dispatch guard)");
    }
    async fn poll(
        &self,
        _sub: SubscriptionId,
        _max: u32,
        _t: u32,
    ) -> Result<Vec<ConsumerMessage>, ConsumerError> {
        unreachable!()
    }
    async fn commit(&self, _sub: SubscriptionId) -> Result<(), ConsumerError> {
        unreachable!()
    }
    async fn unsubscribe(&self, _sub: SubscriptionId) -> Result<(), ConsumerError> {
        unreachable!()
    }
}

async fn handshake(client: &mut UnixStream) {
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
    assert_eq!(resp[HEADER_LEN], PROTOCOL_MAJOR);
}

#[tokio::test]
async fn test_dispatch_panic_closes_connection_not_worker() {
    let socket = temp_socket();
    let registry = ClusterRegistry::new();
    let producer = hi_kafka_worker::producer::logging();
    let consumer: ConsumerHandle = Arc::new(PanicConsumer);
    let shutdown = ShutdownState::new();
    let metrics = Metrics::new();

    let server = Server::bind_with(
        &socket,
        producer,
        consumer,
        registry,
        shutdown,
        metrics,
        Duration::ZERO,
    )
    .await
    .unwrap();
    let _handle = tokio::spawn(server.run(Duration::from_secs(5)));

    // 等 socket 就绪
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // === Step 1：连进去，握手，发 SubscribeReq → 期望 read EOF ===
    let mut client = UnixStream::connect(&socket).await.unwrap();
    handshake(&mut client).await;

    let req = SubscribeReq {
        cluster: "default".into(),
        group_id: "g".into(),
        topics: vec!["t".into()],
        config: vec![],
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload).unwrap();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::SubscribeReq, 42, &payload, &mut frame).unwrap();
    client.write_all(&frame).await.unwrap();
    client.flush().await.unwrap();

    // 应该 EOF（server 端 catch panic 后关连接）
    let mut buf = [0u8; HEADER_LEN];
    let n = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
        .await
        .expect("read should not hang");
    assert_eq!(n.unwrap(), 0, "server should close connection after panic");

    // === Step 2：新连一次 → 验证 worker 没挂，仍能握手 ===
    let mut client2 = UnixStream::connect(&socket).await.unwrap();
    handshake(&mut client2).await; // 不 panic 即说明 server 完好

    let _ = std::fs::remove_file(&socket);
}
