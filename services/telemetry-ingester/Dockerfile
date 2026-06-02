# Build context is the repository root (COPY paths are repo-relative).
FROM rust:1.79-bookworm AS builder
WORKDIR /app

# rdkafka is built with the `cmake-build` feature (vendored librdkafka), so the
# builder needs cmake + a C/C++ toolchain. librdkafka-dev/pkg-config are kept for
# parity with the other Rust service images.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential librdkafka-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# The whole Cargo workspace must be present for cargo to resolve it — copy every
# member, not just this crate.
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
COPY services/telemetry-ingester services/telemetry-ingester
RUN cargo build --release -p iicpc-telemetry-ingester

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-telemetry-ingester /usr/local/bin/iicpc-telemetry-ingester
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/iicpc-telemetry-ingester"]
