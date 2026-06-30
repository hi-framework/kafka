use bytes::Bytes;

/// 帧头长度：4 (len) + 1 (type) + 8 (cid)
pub const HEADER_LEN: usize = 13;

/// 单帧 payload 最大长度：16 MiB
pub const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

/// 协议版本（major）
pub const PROTOCOL_MAJOR: u8 = 1;

/// 帧类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// 握手帧。首次连接必须由 PHP 端发出。
    Hello = 0x01,
    /// 心跳请求
    Ping = 0x02,
    /// 心跳响应
    Pong = 0x03,
    /// Producer fire-and-forget（无应答）
    ProduceFnf = 0x10,
    /// Producer 异步带 ACK 请求
    ProduceReq = 0x11,
    /// Producer ACK 响应
    ProduceResp = 0x12,
    /// 订阅请求
    SubscribeReq = 0x20,
    /// 订阅响应
    SubscribeResp = 0x21,
    /// 轮询拉取消息
    PollReq = 0x22,
    /// 轮询返回消息批次
    PollResp = 0x23,
    /// 提交 offset
    CommitReq = 0x24,
    /// 提交 offset 响应
    CommitResp = 0x25,
    /// 退订
    Unsubscribe = 0x26,
    /// 注册/覆盖集群（cluster name + librdkafka 配置）
    RegisterClusterReq = 0x27,
    /// 注册响应
    RegisterClusterResp = 0x28,
    /// 事务操作请求（begin/commit/abort）
    TxnReq = 0x29,
    /// 事务响应
    TxnResp = 0x2A,
    /// 拉取 rebalance 事件队列
    PollRebalanceReq = 0x2B,
    /// rebalance 事件队列响应
    PollRebalanceResp = 0x2C,
    /// Seek 请求（按 offset / timestamp）
    SeekReq = 0x2D,
    /// Seek 响应
    SeekResp = 0x2E,
    /// 把 consumer offsets 提交到 producer 当前事务（exactly-once stream）
    SendOffsetsReq = 0x31,
    /// 上面操作的响应
    SendOffsetsResp = 0x32,
    /// Pause / Resume per-partition（细粒度背压）
    PauseResumeReq = 0x33,
    /// Pause / Resume 响应
    PauseResumeResp = 0x34,
    /// 推送 SASL/OAUTHBEARER token 给指定 cluster
    SetOAuthBearerTokenReq = 0x35,
    /// SetOAuthBearerToken 响应
    SetOAuthBearerTokenResp = 0x36,
    /// 流控（credit 续费）
    FlowControl = 0x30,
    /// 错误响应
    Error = 0x40,
    /// worker 即将停机通知
    ShutdownNotice = 0x41,
    /// 客户端告别（PHP 进程退出时主动发）：fire-and-forget，无 payload。
    /// worker 收到后若此连接关闭即成最后一个连接且无活跃订阅，则立即自退，
    /// 不必等 idle 超时。发送失败 / worker 已死 → 退化回 idle 自退。
    Goodbye = 0x42,
}

impl FrameType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0x01 => Self::Hello,
            0x02 => Self::Ping,
            0x03 => Self::Pong,
            0x10 => Self::ProduceFnf,
            0x11 => Self::ProduceReq,
            0x12 => Self::ProduceResp,
            0x20 => Self::SubscribeReq,
            0x21 => Self::SubscribeResp,
            0x22 => Self::PollReq,
            0x23 => Self::PollResp,
            0x24 => Self::CommitReq,
            0x25 => Self::CommitResp,
            0x26 => Self::Unsubscribe,
            0x27 => Self::RegisterClusterReq,
            0x28 => Self::RegisterClusterResp,
            0x29 => Self::TxnReq,
            0x2A => Self::TxnResp,
            0x2B => Self::PollRebalanceReq,
            0x2C => Self::PollRebalanceResp,
            0x2D => Self::SeekReq,
            0x2E => Self::SeekResp,
            0x31 => Self::SendOffsetsReq,
            0x32 => Self::SendOffsetsResp,
            0x33 => Self::PauseResumeReq,
            0x34 => Self::PauseResumeResp,
            0x35 => Self::SetOAuthBearerTokenReq,
            0x36 => Self::SetOAuthBearerTokenResp,
            0x30 => Self::FlowControl,
            0x40 => Self::Error,
            0x41 => Self::ShutdownNotice,
            0x42 => Self::Goodbye,
            _ => return None,
        })
    }
}

/// 已解码的帧
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameType,
    pub cid: u64,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(kind: FrameType, cid: u64, payload: impl Into<Bytes>) -> Self {
        Self {
            kind,
            cid,
            payload: payload.into(),
        }
    }
}
