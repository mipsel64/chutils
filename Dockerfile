FROM --platform=${BUILDPLATFORM} debian:12-slim AS base

ENV DEBIAN_FRONTEND=noninteractive
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="$CARGO_HOME/bin:$PATH"

WORKDIR /app

RUN apt update && apt install -y --no-install-recommends \
  build-essential \
  curl \
  git \
  ca-certificates \
  libssl-dev \
  pkg-config \
  # Install cross-compilers
  gcc-aarch64-linux-gnu \
  libc6-dev-arm64-cross \
  gcc-x86-64-linux-gnu \
  libc6-dev-amd64-cross \
  && apt clean \
  && rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
  CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc

RUN case "$TARGETPLATFORM" in \
  "linux/arm64") echo "aarch64-unknown-linux-gnu" > rust_target.txt ;; \
  "linux/amd64") echo "x86_64-unknown-linux-gnu" > rust_target.txt ;; \
  *) exit 1 ;; \
  esac

COPY rust-toolchain.toml rust-toolchain.toml

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --target $(cat rust_target.txt) --profile minimal --default-toolchain none
RUN rustup toolchain install
RUN rustup target add $(cat rust_target.txt)

RUN cargo install cargo-chef --locked

FROM base AS planner
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app

COPY --from=planner /app/recipe.json recipe.json

RUN --mount=type=cache,id=cargo-registry,target=$CARGO_HOME/registry,sharing=locked \
  --mount=type=cache,id=cargo-git,target=$CARGO_HOME/git,sharing=locked \
  cargo chef cook --release --target $(cat rust_target.txt) --recipe-path recipe.json

COPY . .

RUN --mount=type=cache,id=cargo-registry,target=$CARGO_HOME/registry,sharing=locked \
  --mount=type=cache,id=cargo-git,target=$CARGO_HOME/git,sharing=locked \
  cargo build --bin chutils --target $(cat rust_target.txt) --release && \
  mkdir -p /tmp/out && \
  cp target/$(cat rust_target.txt)/release/cli /tmp/out/ch-migrator

FROM --platform=${BUILDPLATFORM} alpine:3.22 AS certs
RUN apk --update add ca-certificates

FROM gcr.io/distroless/cc-debian12

WORKDIR /
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /tmp/out/ch-migrator /usr/bin/ch-migrator

LABEL org.opencontainers.image.source="https://github.com/mipsel64/ch-migrator"
LABEL org.opencontainers.image.description="Clickhouse Migration CLI"
LABEL org.opencontainers.image.licenses="MIT"

ENTRYPOINT ["ch-migrator"]
