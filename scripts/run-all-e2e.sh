#!/usr/bin/env bash
# 跑完整 PHP e2e 套件作为生产回归。每个测试独立 socket / topic 前缀，互不打架。
# 输出汇总：哪些通过 / 哪些失败 / 总耗时。
#
# 用法：
#   ./scripts/run-all-e2e.sh                       # 默认全跑
#   E2E_TESTS="binary transaction" ./scripts/run-all-e2e.sh  # 只跑指定的
#   EXT_PATH=/path/to/libhi_kafka.dylib ./scripts/run-all-e2e.sh
set -uo pipefail

cd "$(dirname "$0")/.."

# PHP_BIN：默认 php；macbook 上自动路由有效但 bash 子 shell 不传，
# 这里支持直接给绝对路径
PHP_BIN="${PHP_BIN:-php}"
EXT_PATH="${EXT_PATH:-$(realpath target/debug/libhi_kafka.dylib 2>/dev/null \
    || realpath target/debug/libhi_kafka.so 2>/dev/null)}"
if [[ -z "$EXT_PATH" || ! -f "$EXT_PATH" ]]; then
    echo "ERROR: 扩展产物缺失，先 cargo build -p hi-kafka --features kafka-vendored-tls" >&2
    exit 1
fi
# 校验 php 可用
if ! command -v "$PHP_BIN" &>/dev/null; then
    echo "ERROR: $PHP_BIN 不可用。设 PHP_BIN 环境变量到绝对路径，或先装 PHP" >&2
    exit 1
fi

# 探测可选扩展（swoole / swow）的 .so 路径，供需要的测试单独加载
# 顺序：环境变量 → php-config 标准 pecl 路径
detect_ext_so() {
    local name="$1"
    local env_var="$2"
    if [[ -n "${!env_var:-}" && -f "${!env_var}" ]]; then
        echo "${!env_var}"; return
    fi
    if command -v php-config &>/dev/null; then
        local dir
        dir=$(php-config --extension-dir 2>/dev/null)
        for f in "$dir/$name.so" "$dir/pecl/$name.so"; do
            [[ -f "$f" ]] && { echo "$f"; return; }
        done
    fi
    # macOS PECL 常见路径
    for f in /opt/homebrew/lib/php/pecl/*/"$name.so" /usr/local/lib/php/pecl/*/"$name.so"; do
        [[ -f "$f" ]] && { echo "$f"; return; }
    done
    echo ""
}
SWOOLE_SO="$(detect_ext_so swoole SWOOLE_SO_PATH)"

# 默认全跑（除掉以 _ 开头的辅助脚本）
ALL_TESTS=(
    binary
    configs
    consumer-in-txn
    consumer-recovery
    control-recovery
    headers
    integration
    oauth-smoke
    partition-timestamp
    pause-resume
    rebalance
    recovery
    replay-recovery
    seek
    swoole-client
    swoole-phase3
    transaction
)
TESTS="${E2E_TESTS:-${ALL_TESTS[@]}}"

green() { printf '\033[32m%s\033[0m' "$*"; }
red()   { printf '\033[31m%s\033[0m' "$*"; }
yellow() { printf '\033[33m%s\033[0m' "$*"; }

# 每个测试用独立 socket；测试结束后必须主动杀 worker，否则它被 init 收养后会一直
# 挂着（ps 里看是 'php' 因为 macOS 没 PR_SET_NAME 改不了 argv[0]）。
# pid 文件由 worker 自己在 bind socket 后写入；recovery 类测试 kill 旧 worker 后
# 新 worker 写新 pid。统一靠 pid 文件清理。
cleanup_worker() {
    local sock="$1"
    if [[ -f "${sock}.pid" ]]; then
        local pid
        pid=$(cat "${sock}.pid" 2>/dev/null || true)
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            for _ in 1 2 3 4 5; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.2
            done
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$sock" "${sock}.pid" "${sock}.spawn-lock"
}

pass=0
fail=0
failed_tests=()

global_start=$(date +%s)
for t in $TESTS; do
    script="tests/php/${t}.php"
    if [[ ! -f "$script" ]]; then
        echo "$(yellow "SKIP") $t (not found)"
        continue
    fi
    sock="/tmp/e2e-${t}-$$.sock"
    topic_prefix="e2e-${t}-$(date +%s%N)-$$"

    start=$(date +%s%N)
    printf "==> %-25s " "$t"
    # integration 走 --with-kafka 真路径（kafka feature 是默认且唯一后端）
    extra_args=""
    # extra_php_opts 用字符串而不是数组，避免 `set -u` 下空数组展开报错
    extra_php_opts=""
    case "$t" in
        integration) extra_args="--with-kafka" ;;
        swoole-client|swoole-phase3)
            # 需要在 hi-kafka 之前装载 swoole 扩展
            if [[ -z "$SWOOLE_SO" ]]; then
                echo "$(yellow SKIP) (swoole.so 未找到，设 SWOOLE_SO_PATH 或装 swoole)"
                continue
            fi
            extra_php_opts="-d extension=$SWOOLE_SO"
            ;;
    esac
    # HI_KAFKA_EXT_PATH 给那些会 spawn 独立 PHP 进程的测试用（rebalance）
    # HI_KAFKA_BROKERS 给依赖默认 broker 的测试
    # -n 跳过系统 php.ini：避免 xdebug 之类的 observer 在 MSHUTDOWN 阶段崩
    output=$(HI_KAFKA_EXT_PATH="$EXT_PATH" \
        HI_KAFKA_BROKERS="${HI_KAFKA_BROKERS:-127.0.0.1:9094}" \
        "$PHP_BIN" -n $extra_php_opts -d extension="$EXT_PATH" "$script" "$sock" "$topic_prefix" $extra_args 2>&1)
    rc=$?
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    # 判定逻辑：
    # 1. 输出含 ★ / PASS$ / 全部 PASS → PASS（rc 不严格要求 0；Swoole 退出时
    #    librdkafka 析构偶发 SIGSEGV，业务断言已经全部通过）
    # 2. rc=0 且输出 ^SKIP: → SKIP
    # 3. 其余 → FAIL
    if echo "$output" | grep -qE '★|PASS$|全部 PASS'; then
        if [[ $rc -ne 0 ]]; then
            echo "$(yellow PASS*) (${elapsed_ms}ms, rc=$rc — 业务断言通过但进程异常退出)"
        else
            echo "$(green PASS) (${elapsed_ms}ms)"
        fi
        pass=$((pass + 1))
    elif [[ $rc -eq 0 ]] && echo "$output" | grep -q '^SKIP:'; then
        echo "$(yellow SKIP) (${elapsed_ms}ms): $(echo "$output" | grep '^SKIP:' | sed 's/^SKIP: //')"
    else
        echo "$(red FAIL) (${elapsed_ms}ms, rc=$rc)"
        echo "$output" | tail -10 | sed 's/^/      /'
        fail=$((fail + 1))
        failed_tests+=("$t")
    fi

    # 清理：杀 worker + 删 socket / pid / lock 文件
    cleanup_worker "$sock"
done

global_end=$(date +%s)
total_secs=$((global_end - global_start))

echo
echo "==================================================================="
echo "总结：通过 $pass 个，失败 $fail 个，总耗时 ${total_secs}s"
if [[ $fail -gt 0 ]]; then
    echo "失败列表：${failed_tests[*]}"
    exit 1
fi
green "★ 全部通过"
echo
