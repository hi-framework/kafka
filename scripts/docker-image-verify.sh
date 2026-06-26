#!/usr/bin/env bash
# 验证一个 hi-kafka docker 镜像的稳态性：
# 1. PHP 能 load 扩展
# 2. .so 没有 NEEDED librdkafka / libssl / libcrypto（vendor 全成功）
# 3. 扩展暴露的 PHP 类 / 函数齐全
# 4. 镜像总大小、扩展大小合理
#
# 用法：
#   scripts/docker-image-verify.sh <image>
#   scripts/docker-image-verify.sh hi-kafka:php8.3-alpine
#   scripts/docker-image-verify.sh hi-kafka:php8.3-debian

set -euo pipefail

IMAGE="${1:?usage: $0 <image>}"

red()    { printf '\033[31m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }

fail=0
check() {
    local name="$1"; shift
    if "$@"; then
        green "  ✓ $name"
    else
        red "  ✗ $name"
        fail=1
    fi
}

# ============================================================================
echo "=== 1. 镜像存在 + 大小 ==="
docker image inspect "$IMAGE" >/dev/null || { red "image not found"; exit 1; }
size=$(docker image inspect "$IMAGE" --format '{{.Size}}')
size_mb=$((size / 1024 / 1024))
echo "  total: ${size_mb} MB"
if [[ $size_mb -gt 300 ]]; then
    yellow "  ⚠ 镜像偏大 (>300MB)，可考虑 multi-stage 清理"
fi

# ============================================================================
echo
echo "=== 2. PHP 加载扩展 ==="
loaded=$(docker run --rm "$IMAGE" php -m 2>&1 | grep -i 'hi[_-]kafka' || true)
check "php -m 出现 hi-kafka" test -n "$loaded"
echo "    $loaded"

# ============================================================================
echo
echo "=== 3. 扩展产物链接稳态（核心断言）==="
docker run --rm --entrypoint sh "$IMAGE" -c '
    set -e
    apt-get install -y -q binutils 2>/dev/null \
        || apk add --no-cache -q binutils 2>/dev/null \
        || true
    SO=$(php -r "echo ini_get(\"extension_dir\");")/hi_kafka.so
    [ -f "$SO" ] || SO=/usr/lib/php/extensions/hi_kafka.so
    echo "  扩展路径: $SO"
    ls -lh "$SO"
    echo
    echo "  NEEDED:"
    readelf -d "$SO" | grep NEEDED
    echo
    if readelf -d "$SO" | grep -E "NEEDED.*(librdkafka|libssl|libcrypto)" >/dev/null; then
        echo "  ✗ 出现禁用的 NEEDED 依赖（librdkafka/libssl/libcrypto），vendor 链路有漏"
        exit 1
    fi
    echo "  ✓ 无 librdkafka/libssl/libcrypto 依赖（vendor 全部进 .so）"
' || fail=1

# ============================================================================
echo
echo "=== 4. PHP API 完整性 ==="
api_check=$(docker run --rm "$IMAGE" php -r '
$expected = [
    "hi_kafka_version",
    "hi_kafka_ensure_worker",
    "hi_kafka_register_cluster",
    "hi_kafka_produce_fnf",
    "hi_kafka_produce_sync",
    "hi_kafka_subscribe",
    "hi_kafka_poll",
    "hi_kafka_commit",
    "hi_kafka_unsubscribe",
];
$missing = [];
foreach ($expected as $f) {
    if (! function_exists($f)) $missing[] = $f;
}
if (! class_exists("Hi\\Kafka\\Client")) $missing[] = "Hi\\Kafka\\Client (class)";
if ($missing) {
    fwrite(STDERR, "MISSING: " . implode(", ", $missing) . "\n");
    exit(1);
}
echo "version=" . hi_kafka_version() . PHP_EOL;
echo "OK: " . count($expected) . " functions + 1 class" . PHP_EOL;
' 2>&1)
echo "$api_check" | sed 's/^/  /'
if echo "$api_check" | grep -q MISSING; then
    fail=1
fi

# ============================================================================
echo
echo "=== 5. 集群配置（registerCluster + cluster name）走通 ==="
docker run --rm "$IMAGE" php -r '
$c = new Hi\Kafka\Client("/tmp/verify-" . getmypid() . ".sock");
// 注册集群（不连真实 broker，只走 IPC 路径）
$c->registerCluster("verify", [
    "bootstrap.servers" => "127.0.0.1:65535",
]);
echo "registerCluster ok" . PHP_EOL;
' 2>&1 | sed 's/^/  /' || fail=1

# ============================================================================
echo
if [[ $fail -eq 0 ]]; then
    green "★ $IMAGE 全部验证通过"
else
    red "✗ $IMAGE 验证失败，见上方 ✗ 项"
    exit 1
fi
