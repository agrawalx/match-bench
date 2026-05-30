FROM rust:bookworm AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
RUN cargo build --release -p iicpc-ebpf-latency

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-ebpf-latency /usr/local/bin/iicpc-ebpf-latency
ENTRYPOINT ["/usr/local/bin/iicpc-ebpf-latency"]
