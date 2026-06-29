#!/usr/bin/env bash
# Phase 1 dev build：编译 worker + 扩展，复制扩展到 PHP extension dir。
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE="${PROFILE:-debug}"
# 用 string + word-splitting 而非 array，避开 macOS bash 3.2 下
# `set -u` + 空 array `[@]` 展开 "unbound variable" 报错。
# 这里 flag 只可能是单 token `--release`，没空格安全。
CARGO_FLAGS=""
if [[ "$PROFILE" == "release" ]]; then
    CARGO_FLAGS="--release"
fi

echo "==> cargo build ($PROFILE)"
# shellcheck disable=SC2086
cargo build -p hi-kafka-proto $CARGO_FLAGS
# shellcheck disable=SC2086
cargo build -p hi-kafka-worker $CARGO_FLAGS
# shellcheck disable=SC2086
cargo build -p hi-kafka $CARGO_FLAGS

TARGET_DIR="target/$PROFILE"

# 探测扩展产物（macOS .dylib / Linux .so）
EXT_SRC=""
for cand in "$TARGET_DIR/libhi_kafka.dylib" "$TARGET_DIR/libhi_kafka.so"; do
    if [[ -f "$cand" ]]; then
        EXT_SRC="$cand"
        break
    fi
done
if [[ -z "$EXT_SRC" ]]; then
    echo "ERROR: 找不到扩展产物 (libhi_kafka.{dylib,so})" >&2
    exit 1
fi

EXT_DIR="$(php-config --extension-dir)"
echo "==> 扩展产物: $EXT_SRC"
echo "==> PHP extension dir: $EXT_DIR"

if [[ "${INSTALL:-0}" == "1" ]]; then
    echo "==> 复制到 $EXT_DIR/hi_kafka.so"
    cp "$EXT_SRC" "$EXT_DIR/hi_kafka.so"
    # macOS hardened PHP 加载 Rust linker-signed dylib 偶发被 AMFI SIGKILL，
    # 重签纯 ad-hoc（flags 0x2）后稳定
    if [[ "$(uname)" == "Darwin" ]] && command -v codesign &>/dev/null; then
        codesign --force --sign - "$EXT_DIR/hi_kafka.so" 2>/dev/null \
            && echo "==> 已重签 ad-hoc"
    fi
fi

echo "==> worker: $TARGET_DIR/hi-kafka-worker"
echo "==> 构建完成"
