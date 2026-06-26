# 多阶段 Linux 构建：基于官方 php-cli 镜像（Debian bookworm），输出 hi_kafka.so。
#
# 用法（默认走 USTC 镜像加速 apt / rustup / crates.io）：
#   docker build --build-arg PHP_VERSION=8.3 -t hi-kafka:php8.3 .
#   docker run --rm hi-kafka:php8.3 php -m | grep hi
#
# 关掉镜像走官方源（境外环境）：
#   docker build \
#       --build-arg APT_MIRROR= \
#       --build-arg RUSTUP_DIST_SERVER=https://static.rust-lang.org \
#       --build-arg RUSTUP_UPDATE_ROOT=https://static.rust-lang.org/rustup \
#       --build-arg CRATES_INDEX= \
#       -t hi-kafka:php8.3 .
#
# 跨架构（在 colima/Docker Desktop 上启 buildx）：
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       --build-arg PHP_VERSION=8.3 -t hi-kafka:php8.3 .
#
# 产物路径：
#   /usr/lib/php/extensions/hi_kafka.so
#   /usr/local/etc/php/conf.d/hi_kafka.ini  (自动激活扩展)
#
# 稳态构建（`kafka-vendored-tls`）：
#   - librdkafka 跟随 rdkafka-sys vendored 编译（CMake 链路），不用 apt 的 librdkafka1
#     （Debian 12 的 1.9.2 比 vendored 的 2.12 老两年，缺 KIP-447 完整支持）
#   - OpenSSL 也 vendor（rdkafka/ssl-vendored），跳过 Debian 12 → 13 OpenSSL 3 ABI 变化
#   - 仅 libsasl2 / libz 用系统包（ABI 稳，几十年不变）
#   - 同一 commit 在 Debian / Alpine / macOS 出的 .so 字节行为一致

ARG PHP_VERSION=8.3
ARG RUST_VERSION=1.85
# Debian 发行版 codename：pin 在 bookworm（Debian 12 / LLVM 14）。
# 不 pin 的话，`php:8.3-cli` 现在已经 follow trixie（Debian 13 / LLVM 19），
# 而 rdkafka-sys 4.10 用的 bindgen 0.68 不识别 LLVM 19+，会撞「Dynamic loading not supported」。
# 升级 bindgen 需要 rdkafka 上游跟进，目前 pin Debian 是稳态。
# 升 bookworm → trixie checklist 同 Dockerfile.alpine 的 ALPINE_VARIANT 升级说明。
ARG DEBIAN_CODENAME=bookworm

# 国内镜像加速（USTC）。设为空字符串则走默认源。
# - apt 源：deb.debian.org / security.debian.org → mirrors.ustc.edu.cn
# - rustup：static.rust-lang.org → mirrors.ustc.edu.cn/rust-static
# - crates.io：sparse+https://mirrors.ustc.edu.cn/crates.io-index/
ARG APT_MIRROR=mirrors.ustc.edu.cn
ARG RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
ARG RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup
ARG CRATES_INDEX=sparse+https://mirrors.ustc.edu.cn/crates.io-index/

# ============================================================================
# Stage 1: builder
# ============================================================================
FROM php:${PHP_VERSION}-cli-${DEBIAN_CODENAME} AS builder

ARG RUST_VERSION
ARG APT_MIRROR
ARG RUSTUP_DIST_SERVER
ARG RUSTUP_UPDATE_ROOT
ARG CRATES_INDEX

# 切 apt 源到 USTC（如指定）。Debian 12 用 deb822 格式 /etc/apt/sources.list.d/debian.sources，
# 老版本用 /etc/apt/sources.list。同时改 main + security 镜像。
RUN if [ -n "${APT_MIRROR}" ]; then \
        find /etc/apt -type f \( -name '*.sources' -o -name 'sources.list' \) \
            -exec sed -i \
                -e "s|deb.debian.org|${APT_MIRROR}|g" \
                -e "s|security.debian.org/debian-security|${APT_MIRROR}/debian-security|g" \
                -e "s|security.debian.org|${APT_MIRROR}/debian-security|g" \
                {} + \
        && echo "==> apt sources:" \
        && (cat /etc/apt/sources.list.d/debian.sources 2>/dev/null || cat /etc/apt/sources.list) | head -10; \
    fi

# 构建依赖：
# - cmake + perl：vendored librdkafka + OpenSSL 用 CMake 链 + perl Configure
# - libsasl2-dev / zlib1g-dev / libcurl4-openssl-dev：rdkafka/sasl + rdkafka/libz + rdkafka/curl
#   （curl 头是 librdkafka 2.12 OAUTHBEARER OIDC 强制依赖；ABI 稳，可以链系统）
# - libclang-dev / clang：bindgen 生成 rdkafka-sys 绑定
# - 不再装 librdkafka-dev：vendored 模式根本不读它（之前是历史遗留）
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        perl \
        libsasl2-dev \
        zlib1g-dev \
        libcurl4-openssl-dev \
        libclang-dev \
        clang \
        pkg-config \
        curl \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Rust 工具链。RUSTUP_DIST_SERVER / RUSTUP_UPDATE_ROOT 让 rustup 从 USTC 拉。
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUSTUP_DIST_SERVER=${RUSTUP_DIST_SERVER} \
    RUSTUP_UPDATE_ROOT=${RUSTUP_UPDATE_ROOT}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal --no-modify-path \
    && rustup --version && cargo --version && rustc --version

# 把 crates.io registry 指向 USTC sparse 索引
RUN if [ -n "${CRATES_INDEX}" ]; then \
        mkdir -p ${CARGO_HOME} \
        && printf '[source.crates-io]\nreplace-with = "ustc"\n\n[source.ustc]\nregistry = "%s"\n' "${CRATES_INDEX}" \
            > ${CARGO_HOME}/config.toml \
        && echo "==> cargo source:" && cat ${CARGO_HOME}/config.toml; \
    fi

WORKDIR /src

# 利用 docker 层缓存：先只复制 manifest，预拉依赖
COPY Cargo.toml Cargo.lock ./
COPY proto/Cargo.toml ./proto/
COPY worker/Cargo.toml ./worker/
COPY ext/Cargo.toml ./ext/
COPY rust-toolchain.toml ./
RUN mkdir -p proto/src worker/src ext/src \
    && echo 'pub fn _stub() {}' > proto/src/lib.rs \
    && echo 'pub fn _stub() {}' > worker/src/lib.rs \
    && echo 'fn main() {}' > worker/src/main.rs \
    && echo 'pub fn _stub() {}' > ext/src/lib.rs \
    && cargo build --workspace --release --features hi-kafka-worker/kafka-vendored-tls 2>/dev/null || true

# 真实源码
COPY proto ./proto
COPY worker ./worker
COPY ext ./ext

RUN touch proto/src/lib.rs worker/src/lib.rs worker/src/main.rs ext/src/lib.rs \
    && cargo build -p hi-kafka --release --features kafka-vendored-tls

# 稳态校验：.so 不应当 NEEDED librdkafka / libssl / libcrypto（全部 vendored 进来）
RUN apt-get update && apt-get install -y --no-install-recommends binutils \
    && rm -rf /var/lib/apt/lists/* \
    && echo "==> readelf NEEDED:" \
    && readelf -d /src/target/release/libhi_kafka.so | grep NEEDED \
    && ! (readelf -d /src/target/release/libhi_kafka.so | grep -E "NEEDED.*(librdkafka|libssl|libcrypto)") \
    && echo "==> ✓ no librdkafka/libssl/libcrypto runtime dependency"

# ============================================================================
# Stage 2: runtime
# ============================================================================
FROM php:${PHP_VERSION}-cli-${DEBIAN_CODENAME} AS runtime

ARG APT_MIRROR

# 切 apt 源到 USTC（与 builder 对称）
RUN if [ -n "${APT_MIRROR}" ]; then \
        find /etc/apt -type f \( -name '*.sources' -o -name 'sources.list' \) \
            -exec sed -i \
                -e "s|deb.debian.org|${APT_MIRROR}|g" \
                -e "s|security.debian.org/debian-security|${APT_MIRROR}/debian-security|g" \
                -e "s|security.debian.org|${APT_MIRROR}/debian-security|g" \
                {} +; \
    fi

# 运行时只需 system libsasl2 + libz + libcurl4（其它全部 vendor 进 .so）
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsasl2-2 \
        zlib1g \
        libcurl4 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG EXT_DIR=/usr/lib/php/extensions
RUN mkdir -p ${EXT_DIR}

# 单 .so —— worker 代码 + librdkafka + OpenSSL 全部链进来
COPY --from=builder /src/target/release/libhi_kafka.so ${EXT_DIR}/hi_kafka.so

# 激活扩展
RUN echo "extension=${EXT_DIR}/hi_kafka.so" > /usr/local/etc/php/conf.d/hi_kafka.ini

# PHP driver 文件（Hi\Kafka\SwooleClient / SwowClient 等）
COPY php-driver/src /opt/hi-kafka/php-driver/src

# 测试脚本（CI 集成测试用）
COPY tests/php /opt/hi-kafka/tests/php

LABEL org.opencontainers.image.title="hi-kafka" \
      org.opencontainers.image.description="PHP Kafka extension with embedded Rust worker (vendored librdkafka + OpenSSL)"

CMD ["php", "-m"]
