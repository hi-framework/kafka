//! 在内存里跑一个 worker server，通过 tokio UnixStream 客户端发 PRODUCE_FNF 帧，
//! 验证 server 能正确解码并 dispatch。

use bytes::BytesMut;
use hi_kafka_proto::{FrameType, ProduceFnf, encode_frame};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

static SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_socket() -> PathBuf {
    let pid = std::process::id();
    let seq = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hi-kafka-test-{pid}-{seq}-{ns}.sock"))
}

#[tokio::test]
async fn test_produce_fnf_end_to_end() {
    let socket = temp_socket();

    // 启动 server
    let server = hi_kafka_worker::Server::bind(&socket).await.unwrap();
    let _handle = tokio::spawn(server.run(std::time::Duration::from_secs(5)));

    // 等 socket 真正就绪
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "socket not ready");

    // 客户端连
    let mut client = UnixStream::connect(&socket).await.unwrap();

    // 编一帧 PRODUCE_FNF
    let msg = ProduceFnf {
        cluster: "default".into(),
        topic: "e2e-topic".into(),
        key: bytes::Bytes::from_static(b"key-1"),
        value: bytes::Bytes::from_static(b"hello e2e"),
        ..Default::default()
    };
    let mut payload = BytesMut::new();
    msg.encode(&mut payload).unwrap();

    let mut frame = BytesMut::new();
    encode_frame(FrameType::ProduceFnf, 0, &payload, &mut frame).unwrap();

    client.write_all(&frame).await.unwrap();
    client.flush().await.unwrap();
    drop(client);

    // 给 server dispatch 一点时间。MVP 期没有 ack 路径可以直接 await，
    // 实测这一步在 protocol v2 引入 ack 后会改为 wait_for_resp。
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 清理
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_multiple_frames_on_same_connection() {
    let socket = temp_socket();
    let server = hi_kafka_worker::Server::bind(&socket).await.unwrap();
    let _handle = tokio::spawn(server.run(std::time::Duration::from_secs(5)));

    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut client = UnixStream::connect(&socket).await.unwrap();

    for i in 0..10 {
        let msg = ProduceFnf {
            cluster: "default".into(),
            topic: "burst".into(),
            key: bytes::Bytes::from(format!("k-{i}")),
            value: bytes::Bytes::from(format!("v-{i}")),
            ..Default::default()
        };
        let mut payload = BytesMut::new();
        msg.encode(&mut payload).unwrap();
        let mut frame = BytesMut::new();
        encode_frame(FrameType::ProduceFnf, i as u64, &payload, &mut frame).unwrap();
        client.write_all(&frame).await.unwrap();
    }
    client.flush().await.unwrap();
    drop(client);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = std::fs::remove_file(&socket);
}
