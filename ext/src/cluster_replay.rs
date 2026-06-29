//! 扩展端 cluster 注册重放缓存。
//!
//! L：worker 进程死亡 → spawn 新 worker 后，新 worker 的 ClusterRegistry 是空的。
//! 业务侧只在启动时调一次 `registerCluster` / `setOAuthBearerToken`，后续 produce /
//! subscribe / commit 都靠那次注册。所以扩展端必须把 PHP 这一侧已经成功过的所有
//! cluster 注册（含 OAuth token）记下来，在新 worker spawn 后透明重放，让"worker
//! 死亡 → 所有 RPC 自愈"链路完整闭环。
//!
//! 设计要点：
//! - per-socket 缓存（多 socket = 多 worker 实例并存场景）
//! - cluster config 全量覆盖（最后一次 register 的最新值）
//! - OAuth token 跟 cluster 绑定，cluster 删则 token 删——但目前没有删 cluster
//!   的 API，所以缓存只增不减
//! - 重放本身不带 retry——重放发生在 ensure 拉起 worker 后的同一调用栈里，
//!   失败让业务调用方拿到清晰错误（而不是死循环）

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone)]
pub struct OAuthToken {
    pub token_value: String,
    pub lifetime_ms: i64,
    pub principal_name: String,
    pub extensions: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct ReplayEntry {
    pub config: Vec<(String, String)>,
    pub oauth_token: Option<OAuthToken>,
}

type ReplayMap = HashMap<String, ReplayEntry>; // cluster_name → entry

static REPLAY: OnceLock<Mutex<HashMap<PathBuf, ReplayMap>>> = OnceLock::new();

fn replay_map() -> &'static Mutex<HashMap<PathBuf, ReplayMap>> {
    REPLAY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 记录一次成功的 cluster 注册。覆盖同名 cluster 的旧 config，保留已有 token slot。
pub fn record_cluster(socket: &str, cluster: &str, config: Vec<(String, String)>) {
    let mut map = replay_map().lock().unwrap();
    let entry = map.entry(PathBuf::from(socket)).or_default();
    let slot = entry.entry(cluster.to_string()).or_default();
    slot.config = config;
}

/// 记录一次成功的 OAuth token 推送。要求该 cluster 已经 record_cluster。
/// 没注册过就先建一个空 config 占位，等后续 record_cluster 补上。
pub fn record_oauth_token(socket: &str, cluster: &str, token: OAuthToken) {
    let mut map = replay_map().lock().unwrap();
    let entry = map.entry(PathBuf::from(socket)).or_default();
    let slot = entry.entry(cluster.to_string()).or_default();
    slot.oauth_token = Some(token);
}

/// 快照该 socket 上所有 cluster + token。重放路径用。返回的 Vec 顺序不保证。
pub fn snapshot(socket: &str) -> Vec<(String, ReplayEntry)> {
    let map = replay_map().lock().unwrap();
    map.get(&PathBuf::from(socket))
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

/// 测试 / 监控辅助
#[allow(dead_code)]
pub fn count(socket: &str) -> usize {
    let map = replay_map().lock().unwrap();
    map.get(&PathBuf::from(socket))
        .map(|m| m.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_snapshot() {
        let sock = format!("/tmp/test-replay-{}.sock", std::process::id());
        record_cluster(
            &sock,
            "main",
            vec![("bootstrap.servers".into(), "kafka-1:9092".into())],
        );
        record_cluster(
            &sock,
            "audit",
            vec![("bootstrap.servers".into(), "audit:9092".into())],
        );
        let mut snap = snapshot(&sock);
        snap.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].0, "audit");
        assert_eq!(snap[1].0, "main");
    }

    #[test]
    fn test_cluster_overwrite_keeps_token() {
        let sock = format!("/tmp/test-replay-overwrite-{}.sock", std::process::id());
        record_cluster(
            &sock,
            "msk",
            vec![("bootstrap.servers".into(), "v1".into())],
        );
        record_oauth_token(
            &sock,
            "msk",
            OAuthToken {
                token_value: "jwt-1".into(),
                lifetime_ms: 1_000_000,
                principal_name: "p".into(),
                extensions: vec![],
            },
        );
        // 覆盖 config
        record_cluster(
            &sock,
            "msk",
            vec![("bootstrap.servers".into(), "v2".into())],
        );
        let snap = snapshot(&sock);
        let e = snap.iter().find(|(k, _)| k == "msk").unwrap();
        assert_eq!(e.1.config[0].1, "v2");
        assert!(
            e.1.oauth_token.is_some(),
            "token slot 不该因 config 覆盖丢失"
        );
    }

    #[test]
    fn test_token_before_cluster() {
        let sock = format!("/tmp/test-replay-tk-{}.sock", std::process::id());
        record_oauth_token(
            &sock,
            "early",
            OAuthToken {
                token_value: "t".into(),
                lifetime_ms: 0,
                principal_name: "p".into(),
                extensions: vec![],
            },
        );
        let snap = snapshot(&sock);
        let e = snap.iter().find(|(k, _)| k == "early").unwrap();
        assert!(e.1.oauth_token.is_some());
        assert!(e.1.config.is_empty());
    }
}
