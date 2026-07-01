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

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn test_new_starts_not_draining() {
        let s = ShutdownState::new();
        assert!(!s.is_draining(), "初始状态非 draining");
    }

    #[test]
    fn test_start_draining_flips_flag() {
        let s = ShutdownState::new();
        s.start_draining();
        assert!(s.is_draining(), "start_draining 后应 draining=true");
    }

    #[test]
    fn test_start_draining_idempotent() {
        let s = ShutdownState::new();
        s.start_draining();
        s.start_draining();
        s.start_draining();
        assert!(s.is_draining(), "多次 start_draining 幂等，仍为 true");
    }

    #[tokio::test]
    async fn test_wait_draining_returns_immediately_when_already_draining() {
        let s = ShutdownState::new();
        s.start_draining();
        // 已 draining → wait_draining 应零延迟返回
        let r = timeout(Duration::from_millis(50), s.wait_draining()).await;
        assert!(r.is_ok(), "已 draining 时 wait 应立即返回");
    }

    #[tokio::test]
    async fn test_wait_draining_wakes_on_start() {
        let s = ShutdownState::new();
        let s2 = s.clone();

        // 后台：50ms 后触发 draining
        let trigger = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            s2.start_draining();
        });

        // 主：wait 应被唤醒（总耗时 ~50ms，加宽到 500ms 兜底）
        let r = timeout(Duration::from_millis(500), s.wait_draining()).await;
        assert!(r.is_ok(), "start_draining 应唤醒 wait_draining");
        trigger.await.unwrap();
        assert!(s.is_draining());
    }

    #[tokio::test]
    async fn test_wait_draining_blocks_until_triggered() {
        let s = ShutdownState::new();
        // 未触发 draining → wait_draining 应超时
        let r = timeout(Duration::from_millis(50), s.wait_draining()).await;
        assert!(r.is_err(), "未 drain 时 wait 应一直阻塞（本例超时）");
        assert!(!s.is_draining());
    }

    #[tokio::test]
    async fn test_wait_draining_wakes_multiple_waiters() {
        let s = ShutdownState::new();

        let handles: Vec<_> = (0..5)
            .map(|_| {
                let s2 = s.clone();
                tokio::spawn(async move {
                    timeout(Duration::from_millis(500), s2.wait_draining()).await
                })
            })
            .collect();

        // 稍等，让所有 waiter 都进 notified()
        tokio::time::sleep(Duration::from_millis(10)).await;
        s.start_draining();

        // 所有 waiter 都应被唤醒
        for h in handles {
            let r = h.await.unwrap();
            assert!(r.is_ok(), "所有 waiter 都应被 start_draining 唤醒");
        }
    }
}
