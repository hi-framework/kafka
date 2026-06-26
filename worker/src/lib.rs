//! Hi-Kafka worker 库入口。
//!
//! 主二进制由 `src/main.rs` 编译为 `hi-kafka-worker`，
//! 同时 `src/lib.rs` 把核心模块作为库暴露给集成测试和未来的同进程嵌入用例。

pub mod cluster;
pub mod consumer;
pub mod metrics;
pub mod producer;
pub mod server;
pub mod shutdown;

pub use cluster::{ClusterConfig, ClusterRegistry, ClusterRegistryHandle};
pub use consumer::{Consumer, ConsumerHandle, ConsumerError, LoggingConsumer, SubscriptionId};
pub use metrics::Metrics;
pub use producer::{LoggingProducer, Producer, ProducerHandle};
pub use server::Server;
pub use shutdown::{ShutdownHandle, ShutdownState};
