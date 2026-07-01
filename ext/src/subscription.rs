//! 订阅虚拟 ID 注册表（扩展端 Consumer 自愈层）。
//!
//! 设计要点：
//!
//! - PHP 业务拿到的 `subscription_id` 是 **virtual_id**，全进程单调
//! - 注册表保存 `virtual_id → (订阅参数 + 当前 real_id)`
//! - 当 worker 上的 real subscription 因 worker 崩溃而消失时，扩展透明：
//!   1. 检测 `IpcError::Worker{kind=SubscriptionNotFound}`（结构化，非字符串匹配）
//!   2. 用注册表里保存的参数调 `ipc::subscribe()` 重订阅
//!   3. 更新 real_id，重试一次原操作
//! - 业务 `$sub` 句柄永远不变；offset 推进语义遵循 Kafka at-least-once：
//!   未提交的消息会在新订阅 join 时从已提交 offset 重新派发
//!
//! 注：commit/unsubscribe 也走自愈，但 commit-after-resubscribe 实际是 no-op
//! （新订阅尚未 poll 出任何位置），符合 Kafka 行为预期。

use crate::ipc::{self, IpcError};
use hi_kafka_proto::{ConsumerMessage, ErrorKind};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

static NEXT_VIRTUAL: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct SubscriptionEntry {
    socket: PathBuf,
    cluster: String,
    group_id: String,
    topics: Vec<String>,
    config: Vec<(String, String)>,
    /// 当前 worker 进程上对应的 real subscription_id。
    /// 重订阅成功后会被更新；正在重订阅时为 None。
    real_id: u64,
    /// 用于序列化对该 virtual subscription 的所有可变操作（poll/commit/resubscribe）。
    /// 避免多协程并发触发多次 resubscribe。
    lock: std::sync::Arc<Mutex<()>>,
}

static REGISTRY: OnceLock<Mutex<HashMap<u64, SubscriptionEntry>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<u64, SubscriptionEntry>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ResubscribeStats {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
}

static RESUB_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static RESUB_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static RESUB_FAILURES: AtomicU64 = AtomicU64::new(0);

pub fn resubscribe_stats() -> ResubscribeStats {
    ResubscribeStats {
        attempts: RESUB_ATTEMPTS.load(Ordering::Relaxed),
        successes: RESUB_SUCCESSES.load(Ordering::Relaxed),
        failures: RESUB_FAILURES.load(Ordering::Relaxed),
    }
}

/// 检查错误是否暗示 "real subscription 在 worker 上不存在了"。
///
/// 结构化判定（替代脆弱的 `"not found"` 字符串匹配）：worker 现在对 subscription
/// 不存在统一回 `Error` 帧 `kind=SubscriptionNotFound`，扩展端经 `recv_resp` 解成
/// `IpcError::Worker`。措辞变化不再影响自愈。
fn is_subscription_gone(e: &IpcError) -> bool {
    matches!(
        e,
        IpcError::Worker {
            kind: ErrorKind::SubscriptionNotFound,
            ..
        }
    )
}

pub fn subscribe(
    socket: &str,
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Vec<(String, String)>,
    timeout: Duration,
) -> Result<u64, IpcError> {
    let real_id = ipc::subscribe(
        socket,
        cluster,
        group_id,
        topics.clone(),
        config.clone(),
        timeout,
    )?;
    let virt = NEXT_VIRTUAL.fetch_add(1, Ordering::Relaxed);
    registry().lock().unwrap().insert(
        virt,
        SubscriptionEntry {
            socket: PathBuf::from(socket),
            cluster: cluster.to_string(),
            group_id: group_id.to_string(),
            topics,
            config,
            real_id,
            lock: std::sync::Arc::new(Mutex::new(())),
        },
    );
    Ok(virt)
}

pub fn poll(
    virtual_id: u64,
    max_messages: u32,
    timeout_ms: u32,
) -> Result<Vec<ConsumerMessage>, IpcError> {
    with_resubscribe(virtual_id, |entry| {
        let socket = entry.socket.to_string_lossy().to_string();
        ipc::poll(&socket, entry.real_id, max_messages, timeout_ms)
    })
}

pub fn commit(virtual_id: u64, timeout: Duration) -> Result<(), IpcError> {
    with_resubscribe(virtual_id, |entry| {
        let socket = entry.socket.to_string_lossy().to_string();
        ipc::commit(&socket, entry.real_id, timeout)
    })
}

pub fn unsubscribe(virtual_id: u64) -> Result<(), IpcError> {
    let entry = {
        let mut reg = registry().lock().unwrap();
        match reg.remove(&virtual_id) {
            Some(e) => e,
            None => return Ok(()), // 已经被移除，幂等
        }
    };
    let socket = entry.socket.to_string_lossy().to_string();
    // unsubscribe 是 fire-and-forget；忽略 worker 是否真的处理了
    let _ = ipc::unsubscribe(&socket, entry.real_id);
    Ok(())
}

/// 通用：执行一次操作；如果失败且属于 "subscription gone" 类，
/// 重订阅一次再重试。
fn with_resubscribe<T, F>(virtual_id: u64, mut op: F) -> Result<T, IpcError>
where
    F: FnMut(&SubscriptionEntry) -> Result<T, IpcError>,
{
    // 取当前 entry 的拷贝（最小持锁时间）
    let mut snap = {
        let reg = registry().lock().unwrap();
        reg.get(&virtual_id).cloned().ok_or_else(|| {
            IpcError::Server(format!("virtual subscription {virtual_id} not found"))
        })?
    };

    match op(&snap) {
        Ok(v) => Ok(v),
        Err(e) if is_subscription_gone(&e) => {
            RESUB_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            // 加 per-entry 互斥锁，防止多线程/多协程并发触发重复重订阅
            let _g = snap.lock.lock().unwrap();

            // 重新读 registry —— 可能别的并发请求已经重订阅完毕
            let current_real_id = {
                let reg = registry().lock().unwrap();
                reg.get(&virtual_id).map(|e| e.real_id)
            };
            if current_real_id != Some(snap.real_id) {
                // 别的请求已经改了 real_id，直接用新的重试
                if let Some(new_id) = current_real_id {
                    snap.real_id = new_id;
                    return match op(&snap) {
                        Ok(v) => {
                            RESUB_SUCCESSES.fetch_add(1, Ordering::Relaxed);
                            Ok(v)
                        }
                        Err(e2) => {
                            RESUB_FAILURES.fetch_add(1, Ordering::Relaxed);
                            Err(e2)
                        }
                    };
                }
            }

            // 我们负责重订阅
            let socket = snap.socket.to_string_lossy().to_string();
            let new_real_id = ipc::subscribe(
                &socket,
                &snap.cluster,
                &snap.group_id,
                snap.topics.clone(),
                snap.config.clone(),
                Duration::from_secs(5),
            )
            .map_err(|e| {
                RESUB_FAILURES.fetch_add(1, Ordering::Relaxed);
                e
            })?;

            // 更新 registry
            {
                let mut reg = registry().lock().unwrap();
                if let Some(entry) = reg.get_mut(&virtual_id) {
                    entry.real_id = new_real_id;
                }
            }
            snap.real_id = new_real_id;

            // 重试一次
            match op(&snap) {
                Ok(v) => {
                    RESUB_SUCCESSES.fetch_add(1, Ordering::Relaxed);
                    Ok(v)
                }
                Err(e2) => {
                    RESUB_FAILURES.fetch_add(1, Ordering::Relaxed);
                    Err(e2)
                }
            }
        }
        Err(e) => Err(e),
    }
}

/// 测试 / 监控辅助：返回当前已注册的 virtual subscription 数。
#[allow(dead_code)]
pub fn registered_count() -> usize {
    registry().lock().unwrap().len()
}

/// 本进程注册表里所有 distinct socket 路径。进程退出（MSHUTDOWN）主动退出协调用。
/// 锁中毒也尽力返回（into_inner）。
pub fn known_sockets() -> Vec<String> {
    let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    let mut set = std::collections::BTreeSet::new();
    for e in reg.values() {
        set.insert(e.socket.to_string_lossy().to_string());
    }
    set.into_iter().collect()
}

/// 取出并从注册表移除某 socket 上本进程的所有订阅，返回它们的 real subscription id。
/// 进程退出（MSHUTDOWN）时调用：这些订阅不再需要，调用方逐个发 Unsubscribe 干净
/// 离组，从而让 worker 的「活跃订阅」立即归零、可立即自退。锁中毒也尽力处理。
pub fn drain_real_ids_for_socket(socket: &str) -> Vec<u64> {
    let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    let matching: Vec<u64> = reg
        .iter()
        .filter(|(_, e)| e.socket.to_string_lossy() == socket)
        .map(|(vid, _)| *vid)
        .collect();
    let mut real_ids = Vec::with_capacity(matching.len());
    for vid in matching {
        if let Some(e) = reg.remove(&vid) {
            real_ids.push(e.real_id);
        }
    }
    real_ids
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试专用：合成一个虚拟订阅项塞进注册表，返回分配的 virtual_id。
    /// 不走 ipc::subscribe，避免依赖真 worker。
    fn insert_test_entry(socket: &str, cluster: &str, real_id: u64) -> u64 {
        let virt = NEXT_VIRTUAL.fetch_add(1, Ordering::Relaxed);
        registry().lock().unwrap().insert(
            virt,
            SubscriptionEntry {
                socket: PathBuf::from(socket),
                cluster: cluster.into(),
                group_id: "g".into(),
                topics: vec!["t".into()],
                config: vec![],
                real_id,
                lock: std::sync::Arc::new(Mutex::new(())),
            },
        );
        virt
    }

    fn remove_test_entry(vid: u64) {
        registry().lock().unwrap().remove(&vid);
    }

    // === is_subscription_gone ===========================================

    #[test]
    fn test_is_subscription_gone_matches_subscription_not_found() {
        let e = IpcError::Worker {
            kind: ErrorKind::SubscriptionNotFound,
            retryable: false,
            native_code: 0,
            message: "gone".into(),
        };
        assert!(is_subscription_gone(&e));
    }

    #[test]
    fn test_is_subscription_gone_rejects_other_worker_kinds() {
        for kind in [
            ErrorKind::InvalidArgument,
            ErrorKind::ClusterNotRegistered,
            ErrorKind::BrokerRetryable,
            ErrorKind::WorkerDraining,
        ] {
            let e = IpcError::Worker {
                kind,
                retryable: false,
                native_code: 0,
                message: "".into(),
            };
            assert!(
                !is_subscription_gone(&e),
                "{kind:?} 不应触发 resubscribe"
            );
        }
    }

    #[test]
    fn test_is_subscription_gone_rejects_non_worker_variants() {
        assert!(!is_subscription_gone(&IpcError::Server("srv".into())));
        assert!(!is_subscription_gone(&IpcError::Encode("enc".into())));
        assert!(!is_subscription_gone(&IpcError::Io(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe
        ))));
    }

    // === virtual_id 单调 =================================================

    #[test]
    fn test_virtual_id_monotonic_via_insert() {
        // 直接调 insert helper，绕开 ipc；观察 virtual_id 严格递增
        let v1 = insert_test_entry("/tmp/subs-mono-1.sock", "c", 10);
        let v2 = insert_test_entry("/tmp/subs-mono-1.sock", "c", 11);
        let v3 = insert_test_entry("/tmp/subs-mono-1.sock", "c", 12);
        assert!(v1 < v2 && v2 < v3);
        remove_test_entry(v1);
        remove_test_entry(v2);
        remove_test_entry(v3);
    }

    // === unsubscribe 幂等 ================================================

    #[test]
    fn test_unsubscribe_unknown_id_is_ok() {
        // 不在 registry 里 → 直接 Ok，不触发 ipc（否则会 spawn 试图连接不存在的 socket）
        let never_used = u64::MAX / 2;
        assert!(registry()
            .lock()
            .unwrap()
            .get(&never_used)
            .is_none());
        assert!(unsubscribe(never_used).is_ok());
    }

    // === with_resubscribe 未知 virtual_id ================================

    #[test]
    fn test_with_resubscribe_unknown_id_returns_server_error() {
        let called = std::cell::Cell::new(false);
        let never_used = u64::MAX / 2 + 7;
        let r: Result<(), IpcError> = with_resubscribe(never_used, |_| {
            called.set(true);
            Ok(())
        });
        assert!(!called.get(), "未注册的 virtual_id 不应调用 op");
        match r {
            Err(IpcError::Server(msg)) => assert!(msg.contains("not found")),
            other => panic!("expected Server err, got {other:?}"),
        }
    }

    // === with_resubscribe 首次成功 =======================================

    #[test]
    fn test_with_resubscribe_first_try_success() {
        let vid = insert_test_entry("/tmp/subs-succ.sock", "c", 42);
        let seen = std::cell::Cell::new(0u64);
        let r = with_resubscribe(vid, |entry| {
            seen.set(entry.real_id);
            Ok::<u64, IpcError>(entry.real_id + 1000)
        });
        assert_eq!(r.unwrap(), 1042);
        assert_eq!(seen.get(), 42, "op 收到当前 real_id");
        remove_test_entry(vid);
    }

    // === with_resubscribe 非 gone 错误直接透传 ============================

    #[test]
    fn test_with_resubscribe_non_gone_error_no_resubscribe() {
        let vid = insert_test_entry("/tmp/subs-passthrough.sock", "c", 42);
        let calls = std::cell::Cell::new(0);
        let r: Result<(), IpcError> = with_resubscribe(vid, |_| {
            calls.set(calls.get() + 1);
            Err(IpcError::Server("business".into()))
        });
        assert_eq!(calls.get(), 1, "非 gone 错误只调 op 一次");
        assert!(matches!(r, Err(IpcError::Server(_))));
        remove_test_entry(vid);
    }

    // === 注册表辅助方法 ===================================================

    #[test]
    fn test_registered_count_reflects_registry() {
        let before = registered_count();
        let v1 = insert_test_entry("/tmp/subs-count-1.sock", "c", 1);
        let v2 = insert_test_entry("/tmp/subs-count-2.sock", "c", 2);
        assert_eq!(registered_count(), before + 2);
        remove_test_entry(v1);
        remove_test_entry(v2);
        assert_eq!(registered_count(), before);
    }

    #[test]
    fn test_known_sockets_returns_distinct() {
        // 同 socket 插两条应只出现一次
        let sock = "/tmp/subs-known-abc.sock";
        let v1 = insert_test_entry(sock, "c", 1);
        let v2 = insert_test_entry(sock, "c", 2);
        let socks = known_sockets();
        let count = socks.iter().filter(|s| s.as_str() == sock).count();
        assert_eq!(count, 1, "同 socket 应去重");
        remove_test_entry(v1);
        remove_test_entry(v2);
    }

    #[test]
    fn test_drain_real_ids_removes_only_matching_socket() {
        let sock_a = "/tmp/subs-drain-a.sock";
        let sock_b = "/tmp/subs-drain-b.sock";
        let a1 = insert_test_entry(sock_a, "c", 100);
        let a2 = insert_test_entry(sock_a, "c", 101);
        let b1 = insert_test_entry(sock_b, "c", 200);

        let mut drained = drain_real_ids_for_socket(sock_a);
        drained.sort();
        assert_eq!(drained, vec![100, 101]);
        // sock_a 上的都被移除，sock_b 还在
        assert!(registry().lock().unwrap().get(&a1).is_none());
        assert!(registry().lock().unwrap().get(&a2).is_none());
        assert!(registry().lock().unwrap().get(&b1).is_some());
        remove_test_entry(b1);
    }

    // === resubscribe_stats 快照 ==========================================

    #[test]
    fn test_resubscribe_stats_snapshot() {
        let direct = (
            RESUB_ATTEMPTS.load(Ordering::Relaxed),
            RESUB_SUCCESSES.load(Ordering::Relaxed),
            RESUB_FAILURES.load(Ordering::Relaxed),
        );
        let s = resubscribe_stats();
        assert_eq!(s.attempts, direct.0);
        assert_eq!(s.successes, direct.1);
        assert_eq!(s.failures, direct.2);
    }
}
