//! Hi-Kafka IPC protocol v1.
//!
//! 帧格式：
//! ```text
//! 0       4       5      13                       N
//! +-------+-------+-------+------------------------+
//! | len   | type  | cid   |    payload            |
//! | (u32) | (u8)  | (u64) |                       |
//! +-------+-------+-------+------------------------+
//! ```
//!
//! - `len`: payload 长度（不含 header 自身的 13 字节），最大 16 MB
//! - `type`: 帧类型（[`FrameType`]）
//! - `cid`: correlation id，0 表示单向无应答帧
//! - `payload`: 帧体（按 type 解释）

pub mod codec;
pub mod consumer;
pub mod frame;
pub mod payload;
pub mod oauth;
pub mod pause_resume;
pub mod rebalance;
pub mod seek;
pub mod send_offsets;
pub mod txn;

pub use codec::{CodecError, Header, decode_header, encode_frame};
pub use consumer::{
    CommitReq, CommitResp, ConsumerMessage, PollReq, PollResp, RegisterClusterReq,
    RegisterClusterResp, SubscribeReq, SubscribeResp, UnsubscribeReq,
};
pub use frame::{Frame, FrameType, HEADER_LEN, MAX_PAYLOAD_LEN, PROTOCOL_MAJOR};
pub use payload::{
    AUTO_PARTITION, AUTO_TIMESTAMP, DeliveryAck, DeliveryErr, MessageHeader, PayloadError,
    ProduceFnf, ProduceReq, ProduceResp,
};
pub use rebalance::{PollRebalanceReq, PollRebalanceResp, RebalanceEvent};
pub use oauth::{SetOAuthBearerTokenReq, SetOAuthBearerTokenResp};
pub use pause_resume::{PauseResumeOp, PauseResumeReq, PauseResumeResp};
pub use seek::{OffsetSpec, PartitionSpec, SeekReq, SeekResp};
pub use send_offsets::{OffsetCommit, SendOffsetsReq, SendOffsetsResp};
pub use txn::{TxnOp, TxnReq, TxnResp};
