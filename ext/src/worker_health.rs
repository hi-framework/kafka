//! Worker 就绪状态的进程内缓存。
//!
//! `ensure_worker` 原本每次都做一次 `UnixStream::connect` 探测，
//! 在高频 produce 场景下产生大量短连接（实测 300 次 produce → 300 次探测）。
//!
//! 本模块按 socket 路径缓存 "已知 alive" 状态：
//!
//! - 首次 ensure 成功后置位
//! - IPC 错误（write/read 失败、cid 不匹配、连接断开）时由调用方显式失效
//! - 失效后下次 ensure 重新走完整流程（可能拉起新 worker）

use crate::spawn;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

static KNOWN_ALIVE: OnceLock<Mutex<HashMap<PathBuf, ()>>> = OnceLock::new();

fn cache() -> &'static Mutex<HashMap<PathBuf, ()>> {
    KNOWN_ALIVE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 本进程「曾成功确保过」的所有 worker socket。与 `KNOWN_ALIVE` 不同：**永不因
/// invalidate 清空**（worker 中途死亡 / 重启也保留），用于进程退出（MSHUTDOWN）时
/// 对每个用过的 worker 发 Goodbye。覆盖 Swoole/Swow driver 这类**不走 Rust 连接池、
/// 只调 `ensure_worker`** 的路径——否则 [`crate::lifecycle`] 的 `collect_sockets`
/// 看不到它们的 socket，主动退出对协程 driver 失效。
static EVER_ENSURED: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

fn ever_ensured() -> &'static Mutex<BTreeSet<String>> {
    EVER_ENSURED.get_or_init(|| Mutex::new(BTreeSet::new()))
}

/// 本进程曾确保过的所有 worker socket 快照。进程退出主动退出协调用（[`crate::lifecycle`]）。
pub fn ever_ensured_sockets() -> Vec<String> {
    ever_ensured()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .cloned()
        .collect()
}

/// L: 调用结果——区分"缓存命中"和"刚 spawn"，让上层 ipc::ensure 在新 worker 出现时
/// 触发 cluster_replay。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureOutcome {
    /// 缓存已知 alive，直接 fast-path 返回
    AlreadyAlive,
    /// 本次调用拉起或确认了一个 worker（cache miss 路径）。
    /// 调用方应认为它可能是全新 worker，需要重放 cluster 注册。
    JustSpawned,
}

/// 确保 worker 就绪。
/// - cache hit → `AlreadyAlive` 零开销
/// - cache miss → 调 `spawn::ensure_worker`，根据其返回值决定 `AlreadyAlive`
///   还是 `JustSpawned`（区分"worker 还活着只是缓存丢"和"真新 spawn"）
pub fn ensure(socket: &str) -> Result<EnsureOutcome, spawn::SpawnError> {
    let key = PathBuf::from(socket);
    if cache().lock().unwrap().contains_key(&key) {
        return Ok(EnsureOutcome::AlreadyAlive);
    }

    let cfg = spawn::SpawnConfig::from_env(key.clone());
    let outcome = spawn::ensure_worker(&cfg)?;
    cache().lock().unwrap().insert(key, ());
    // 记下「曾确保过」的 socket（仅在成功后）。即便后续 invalidate，这里也保留，
    // 供 MSHUTDOWN 主动退出对该 worker 发 Goodbye——尤其覆盖只调 ensure_worker、
    // 不进 Rust 连接池的 Swoole/Swow driver。
    ever_ensured()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(socket.to_string());
    Ok(match outcome {
        spawn::SpawnOutcome::AlreadyAlive => EnsureOutcome::AlreadyAlive,
        spawn::SpawnOutcome::JustSpawned => EnsureOutcome::JustSpawned,
    })
}

/// 标记某 socket 的 worker 状态为未知（IO 失败时调用）。
pub fn invalidate(socket: &str) {
    let key = PathBuf::from(socket);
    cache().lock().unwrap().remove(&key);
}

/// 调试/测试用：返回当前已缓存的 socket 数。
#[allow(dead_code)]
pub fn cached_count() -> usize {
    cache().lock().unwrap().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidate_unknown_is_noop() {
        invalidate("/does/not/exist.sock");
        // 不 panic 即可
    }

    #[test]
    fn test_cache_state_is_per_socket() {
        // 不真正触发 ensure（需要 worker 二进制），仅检查缓存独立
        let len_before = cached_count();
        invalidate("/tmp/some-fake-socket-for-test.sock");
        let len_after = cached_count();
        assert_eq!(len_before, len_after);
    }
}
