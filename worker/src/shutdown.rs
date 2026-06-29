//! 优雅停机协调状态。
//!
//! - `draining = false`：正常运行
//! - `draining = true`：停止接受新连接 / 拒绝新的 PRODUCE_REQ/FNF
//!
//! 用 [`Notify`] 唤醒在 `accept` 上等的 server 主循环。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct ShutdownState {
    draining: AtomicBool,
    notify: Notify,
}

pub type ShutdownHandle = Arc<ShutdownState>;

impl ShutdownState {
    pub fn new() -> ShutdownHandle {
        Arc::new(Self::default())
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub fn start_draining(&self) {
        self.draining.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub async fn wait_draining(&self) {
        if self.is_draining() {
            return;
        }
        self.notify.notified().await;
    }
}
