#!/usr/bin/env bash
# 一键产 .so 到 dist/。三种 flavor：
#   - mac    本机 cargo build，产 Mach-O .so（Mac 真机 PHP 直接 extension= 加载）
#   - debian Docker 出 Linux glibc .so（部署到 Debian/Ubuntu 服务器）
#   - alpine Docker 出 Linux musl .so（部署到 Alpine 镜像）
#
# 用法：
#   ./scripts/build-so.sh                            # Mac 上默认就给 mac flavor
#   FLAVOR=mac ./scripts/build-so.sh                 # 明确指定
#   FLAVOR=debian ./scripts/build-so.sh              # 走 Docker 出 Linux
#   FLAVOR=alpine ./scripts/build-so.sh
#   FLAVORS="mac debian alpine" ./scripts/build-so.sh # 三个都出
#   PHP_VERSIONS="8.2 8.3 8.4" FLAVORS="debian alpine" ./scripts/build-so.sh # 批量
#   PLATFORM=linux/amd64 FLAVOR=alpine ./scripts/build-so.sh   # Docker 跨架构
#
# PHP_VERSION 行为：
#   - Mac (native)：可选。不给则探本机 php-config / php --version；探不到产
#                   `hi_kafka-mac-{arch}.so`（不带 -phpX.Y 段，因为 macOS 用
#                   dynamic_lookup，.so 跟 PHP 版本无关）。
#   - Docker：**必须**给（默认 8.3）。base image 直接绑定版本，不同版本 ABI 不同。
set -euo pipefail

cd "$(dirname "$0")/.."

# 探测 default flavor：在 Mac 默认就出 mac，其它系统默认走 Docker debian
if [[ -z "${FLAVOR:-${FLAVORS:-}}" ]]; then
    if [[ "$(uname)" == "Darwin" ]]; then
        FLAVORS="mac"
    else
        FLAVORS="debian"
    fi
fi
FLAVORS="${FLAVORS:-${FLAVOR}}"

# 默认 PHP_VERSIONS 的语义对不同 flavor 不一样：
#   - Docker (debian/alpine)：必须指定具体版本，base image 不同 → ABI 不同
#                             显式不指定时回退到 8.3 是合理保守值
#   - mac native：cargo build 出来的 .dylib 用 dynamic_lookup 解析 PHP 符号，
#                 跟本机 PHP 版本无关，**任何**已装 PHP 都能 load。版本号只是
#                 文件名里的 metadata。所以优先探测本机 PHP 版本作为标签，
#                 否则用 'any' 而不是误导性的 '8.3'。
if [[ -z "${PHP_VERSIONS:-${PHP_VERSION:-}}" ]]; then
    case "$FLAVORS" in
        *mac*)
            if command -v php-config &>/dev/null; then
                PHP_VERSIONS="$(php-config --version | cut -d. -f1,2)"
            elif command -v php &>/dev/null; then
                PHP_VERSIONS="$(php -r 'echo PHP_MAJOR_VERSION.".".PHP_MINOR_VERSION;')"
            else
                # 没装 PHP：用 'any'，明确告诉用户这个 .so 不绑定具体 PHP 版本
                PHP_VERSIONS="any"
            fi
            ;;
        *)
            PHP_VERSIONS="8.3"  # Docker 必须有具体版本来选 base image
            ;;
    esac
fi
PHP_VERSIONS="${PHP_VERSIONS:-${PHP_VERSION:-}}"

PLATFORM="${PLATFORM:-$(uname -m | sed 's/x86_64/linux\/amd64/;s/aarch64/linux\/arm64/;s/arm64/linux\/arm64/')}"
DIST_DIR="${DIST_DIR:-dist}"

mkdir -p "$DIST_DIR"

build_mac() {
    local php="$1"
    local arch
    arch=$(uname -m)
    echo "==> Mac 本机 cargo build --release (--features kafka-vendored-tls)"
    if [[ "$php" == "any" ]]; then
        echo "    PHP 版本未指定/未探测到，按「any」处理（macOS dynamic_lookup 跟版本无关）"
    else
        echo "    PHP 版本标签：${php}（macOS 上仅作文件名 metadata，.so 跟版本无关）"
    fi
    cargo build -p hi-kafka --release --features kafka-vendored-tls
    local src
    src="target/release/libhi_kafka.dylib"
    [[ -f "$src" ]] || { echo "ERROR: $src 不存在" >&2; exit 1; }

    # 文件名：探到具体 PHP 版本就 hi_kafka-php8.3-mac-arm64.so，
    # 否则 hi_kafka-mac-arm64.so（不打 phpany 段，避免误导）
    local dst
    if [[ "$php" == "any" ]]; then
        dst="${DIST_DIR}/hi_kafka-mac-${arch}.so"
    else
        dst="${DIST_DIR}/hi_kafka-php${php}-mac-${arch}.so"
    fi
    # PHP 在 macOS 不挑后缀，但常规命名用 .so；这里只是物理拷贝 + 改名
    cp "$src" "$dst"
    # macOS hardened-runtime PHP 拒 Rust linker-signed 的 dylib（codesign flags
    # 0x20002=adhoc,linker-signed），加载时直接 AMFI SIGKILL → exit 137。
    # 用 `codesign -s -` 重签为纯 ad-hoc（flags 0x2）即可被 PHP 接受。
    if command -v codesign &>/dev/null; then
        codesign --force --sign - "$dst" 2>/dev/null \
            && echo "==> 已重签 ad-hoc（消除 linker-signed 标志，让 hardened PHP 接受）"
    fi
    echo "==> Mac .so 产物: $dst ($(ls -lh "$dst" | awk '{print $5}'))"

    if command -v php &>/dev/null; then
        echo "==> 本机 PHP 加载校验（-n 跳过 ini，避免与系统已装版本撞 already-loaded）"
        php -n -d extension="$(realpath "$dst")" -r '
            if (! extension_loaded("hi-kafka") && ! extension_loaded("hi_kafka")) {
                fwrite(STDERR, "extension not loaded\n"); exit(1);
            }
            echo "version=" . hi_kafka_version() . PHP_EOL;
            echo "ok\n";
        '
    else
        echo "==> 跳过本机 PHP 校验（命令不可用），用 -d extension=$dst 自行测"
    fi
}

build_docker() {
    local flavor="$1"
    local php="$2"
    local dockerfile
    case "$flavor" in
        debian) dockerfile="Dockerfile" ;;
        alpine) dockerfile="Dockerfile.alpine" ;;
        *) echo "ERROR: unknown FLAVOR=$flavor (mac | debian | alpine)" >&2; exit 2 ;;
    esac

    if [[ "$php" == "any" ]]; then
        echo "ERROR: Docker flavor 必须指定 PHP_VERSION（base image 跟版本绑定）" >&2
        echo "       例：PHP_VERSION=8.3 FLAVOR=$flavor ./scripts/build-so.sh" >&2
        exit 2
    fi

    local tag arch
    tag="hi-kafka:php${php}-${flavor}-$(uname -m)"
    arch=$(uname -m)
    echo "==> Docker 构建 ${flavor} PHP ${php} (${PLATFORM})"
    docker build \
        -f "$dockerfile" \
        --platform "$PLATFORM" \
        --build-arg "PHP_VERSION=${php}" \
        -t "$tag" \
        .

    echo "==> 抠产物到 ${DIST_DIR}/"
    local cid
    cid=$(docker create --platform "$PLATFORM" "$tag")
    docker cp "$cid:/usr/lib/php/extensions/hi_kafka.so" \
        "${DIST_DIR}/hi_kafka-php${php}-${flavor}-${arch}.so"
    docker rm "$cid" > /dev/null

    echo "==> 容器内 PHP 加载校验"
    docker run --rm --platform "$PLATFORM" "$tag" php -r '
        echo "version=" . hi_kafka_version() . PHP_EOL;
        echo "loaded=" . implode(",", array_filter(get_loaded_extensions(), fn($e) => stripos($e, "kafka") !== false)) . PHP_EOL;
    '
}

for flavor in $FLAVORS; do
    for php in $PHP_VERSIONS; do
        if [[ "$flavor" == "mac" ]]; then
            build_mac "$php"
        else
            build_docker "$flavor" "$php"
        fi
    done
done

echo
echo "==> 产物清单"
ls -lh "${DIST_DIR}/"

if command -v sha256sum &>/dev/null; then
    sum=sha256sum
elif command -v shasum &>/dev/null; then
    sum="shasum -a 256"
else
    sum=""
fi

if [[ -n "$sum" ]]; then
    (cd "$DIST_DIR" && $sum hi_kafka-*.so) > "${DIST_DIR}/checksums.txt"
    echo "==> SHA256：${DIST_DIR}/checksums.txt"
fi
