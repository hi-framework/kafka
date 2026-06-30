//! 进程退出（MSHUTDOWN）时的 worker 主动退出协调。
//!
//! PHP 进程退出（CLI 脚本结束 / FPM worker 回收）时，扩展主动通知它用过的每个
//! worker「我走了」（[`hi_kafka_proto::FrameType::Goodbye`]），让 worker 在最后一个
//! 使用者离开时**立即**自退，而不必干等 idle 超时（默认 5min）。worker 是节点级
//! 共享单例，所以这里只发告别、由 worker 自己判定「是否最后一个连接」——单进程退出
//! 不会误杀仍被其它进程使用的 worker。通知失败 / worker 已死 → 自动退化回 idle 自退。
//!
//! ## 健壮性约束（这条路径跑在 PHP 进程拆除期，必须极度保守）
//!
//! - **绝不 panic**：调用方（`lib.rs::module_shutdown`）用 `catch_unwind` 兜底；本模块
//!   内部全程 best-effort，不 `unwrap` 任何可能失败的 IO。
//! - **绝不 spawn**：只 `UnixStream::connect` 现有 socket，连不上即放弃（worker 已不在，
//!   本就无需清理）。**不**走 `ipc::ensure` / `spawn`——进程退出期 fork 既危险又无意义。
//! - **有界**：connect 对 UDS 近乎即时；读写各设 300ms 超时，worker 卡住也不拖慢退出。
//! - **不依赖 PHP**：纯 Rust + UDS，不调任何 PHP API。

use crate::pool;
use crate::protocol;
use crate::subscription;
use hi_kafka_proto::HEADER_LEN;
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// 单次 IO 的超时上限。worker 正常时这些操作是亚毫秒级；设上限只为防 worker 卡死
/// 时拖慢 PHP 进程退出。
const IO_TIMEOUT: Duration = Duration::from_millis(300);

// === 协程 driver 订阅登记 ===================================================
//
// Swoole/Swow driver 的订阅由 PHP 层管理、**不进 Rust `subscription` 注册表**，
// 故进程退出时 [`cleanup_socket`] 默认看不到、无法 unsubscribe，worker 的活跃订阅
// 不归零 → Goodbye 被挡 → 退化到 idle staleness（非亚秒）。driver 在 `subscribe`/
// `unsubscribe` 时显式登记/注销它创建的 worker real_id，MSHUTDOWN 据此主动 unsubscribe，
// 让协程消费者进程退出也能亚秒触发 worker 自退。

/// socket → 该 socket 上本进程经协程 driver 创建、尚未 unsubscribe 的 worker real_id 集合。
static DRIVER_SUBS: OnceLock<Mutex<HashMap<String, BTreeSet<u64>>>> = OnceLock::new();

fn driver_subs() -> &'static Mutex<HashMap<String, BTreeSet<u64>>> {
    DRIVER_SUBS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 登记一个协程 driver 订阅。见 `hi_kafka_track_subscription`。
pub fn track_subscription(socket: &str, real_id: u64) {
    driver_subs()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(socket.to_string())
        .or_default()
        .insert(real_id);
}

/// 注销一个协程 driver 订阅（driver 主动 unsubscribe 后调）。见 `hi_kafka_untrack_subscription`。
pub fn untrack_subscription(socket: &str, real_id: u64) {
    let mut map = driver_subs().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(set) = map.get_mut(socket) {
        set.remove(&real_id);
        if set.is_empty() {
            map.remove(socket);
        }
    }
}

/// 取出并移除某 socket 的全部 driver 订阅 real_id。MSHUTDOWN cleanup 用。
fn drain_driver_subs(socket: &str) -> Vec<u64> {
    driver_subs()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(socket)
        .map(|set| set.into_iter().collect())
        .unwrap_or_default()
}

/// 当前登记了 driver 订阅的所有 socket。collect_sockets 兜底用。
fn driver_subs_sockets() -> Vec<String> {
    driver_subs()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .cloned()
        .collect()
}

/// MSHUTDOWN 钩子主体。遍历本进程用过的每个 socket，逐个做 best-effort 告别清理。
pub fn on_module_shutdown() {
    for socket in collect_sockets() {
        cleanup_socket(&socket);
    }
}

/// 本进程「用过」的所有 socket：连接池路径 ∪ 订阅注册表路径 ∪ 曾 ensure 过的 socket。
/// 阻塞 Client 走前两者；**Swoole/Swow 协程 driver 不进 Rust 连接池，只在第三者里**。
/// 从没碰过 Kafka 的进程三者皆空 → 整个 MSHUTDOWN 无操作（它本就不是任何 worker 的使用者）。
fn collect_sockets() -> Vec<String> {
    let mut set = BTreeSet::new();
    for path in pool::socket_paths() {
        set.insert(path.to_string_lossy().to_string());
    }
    for s in subscription::known_sockets() {
        set.insert(s);
    }
    // 曾 ensure 过 worker 但不走 Rust 连接池的路径（Swoole/Swow driver）：
    // 它们的 socket 只在 worker_health 里有记录。
    for s in crate::worker_health::ever_ensured_sockets() {
        set.insert(s);
    }
    // 兜底：登记了 driver 订阅的 socket（正常已被 ever_ensured 覆盖，防御性纳入）。
    for s in driver_subs_sockets() {
        set.insert(s);
    }
    set.into_iter().collect()
}

/// 对单个 socket 的告别清理：关空闲连接 → 直连握手 → 逐个 unsubscribe → 发 Goodbye。
fn cleanup_socket(socket: &str) {
    // 1. 关本进程在该 socket 的空闲池连接。否则它们在进程真正退出前仍 open，
    //    会让 worker 误以为还有使用者、迟迟判不出「最后一个连接关闭」。
    pool::close_idle(Path::new(socket));

    // 2. 取出本进程在该 socket 的订阅 real_id（并从注册表摘除）。进程要退了，
    //    这些订阅不再需要——逐个 unsubscribe 让 consumer 干净离组，worker 的活跃
    //    订阅随即归零，Goodbye 才能立即触发自退。
    //    两个来源：阻塞 Client 走 `subscription` 注册表（virtual_id 自愈层）；
    //    Swoole/Swow 协程 driver 走 `DRIVER_SUBS`（PHP 层管理、显式登记）。
    let mut real_ids = subscription::drain_real_ids_for_socket(socket);
    real_ids.extend(drain_driver_subs(socket));

    // 3. 直连 + 握手（绝不 spawn）。连不上 / 握手失败 → worker 已不在，无需清理。
    let mut stream = match connect_and_handshake(socket) {
        Some(s) => s,
        None => return,
    };

    // 4. 逐个 Unsubscribe（fire-and-forget）。任一步写失败说明连接已断，提前收手。
    for rid in real_ids {
        match protocol::build_unsubscribe_frame(rid) {
            Ok(frame) => {
                if write_all(&mut stream, &frame).is_err() {
                    return;
                }
            }
            Err(_) => return, // 编码失败属不可能；保守退出
        }
    }

    // 5. Goodbye（fire-and-forget）。worker 收到后，待本连接关闭即判定是否最后一个。
    if let Ok(frame) = protocol::build_goodbye_frame() {
        let _ = write_all(&mut stream, &frame);
    }
    // drop(stream)：连接立即关闭 → worker 端 EOF → 触发「最后一个连接关闭」判定。
}

/// 直连 socket 并完成 HELLO 握手。任一步失败返回 None（视作 worker 不可用）。
/// **不** spawn、**不** 复用池连接（池连接随后会被进程退出关掉，这里要一个独立、
/// 发完即关的连接，使 worker 能干净捕捉到它的关闭）。
fn connect_and_handshake(socket: &str) -> Option<UnixStream> {
    let mut stream = UnixStream::connect(socket).ok()?;
    stream.set_read_timeout(Some(IO_TIMEOUT)).ok()?;
    stream.set_write_timeout(Some(IO_TIMEOUT)).ok()?;

    let hello = protocol::build_hello_frame().ok()?;
    write_all(&mut stream, &hello).ok()?;

    // 读 HELLO RESP：13B header + 小 payload。
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).ok()?;
    let payload_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    // 防御：HELLO RESP payload 极小（1B major）。异常大 → 协议错乱，放弃。
    if payload_len > 64 {
        return None;
    }
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).ok()?;

    let mut full = Vec::with_capacity(HEADER_LEN + payload_len);
    full.extend_from_slice(&header);
    full.extend_from_slice(&payload);
    protocol::parse_hello_resp(&full).ok()?;
    Some(stream)
}

fn write_all(stream: &mut UnixStream, buf: &[u8]) -> std::io::Result<()> {
    stream.write_all(buf)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    // 各用例用独立 socket 名，避免共享全局 DRIVER_SUBS 在并行测试间串扰。

    #[test]
    fn test_track_drain_dedup() {
        let sock = "/tmp/hi-kafka-driversubs-track.sock";
        track_subscription(sock, 10);
        track_subscription(sock, 20);
        track_subscription(sock, 10); // 重复 → 幂等
        let mut ids = drain_driver_subs(sock);
        ids.sort_unstable();
        assert_eq!(ids, vec![10, 20]);
        // drain 后该 socket 应清空
        assert!(drain_driver_subs(sock).is_empty());
    }

    #[test]
    fn test_untrack_removes_one() {
        let sock = "/tmp/hi-kafka-driversubs-untrack.sock";
        track_subscription(sock, 1);
        track_subscription(sock, 2);
        untrack_subscription(sock, 1);
        untrack_subscription(sock, 999); // 不存在 → no-op
        assert_eq!(drain_driver_subs(sock), vec![2]);
    }

    #[test]
    fn test_untrack_all_removes_socket_key() {
        let sock = "/tmp/hi-kafka-driversubs-key.sock";
        track_subscription(sock, 7);
        assert!(driver_subs_sockets().contains(&sock.to_string()));
        untrack_subscription(sock, 7); // 空集后应移除 socket key
        assert!(!driver_subs_sockets().contains(&sock.to_string()));
    }

    #[test]
    fn test_drain_unknown_socket_empty() {
        assert!(drain_driver_subs("/tmp/hi-kafka-driversubs-nope.sock").is_empty());
    }
}
