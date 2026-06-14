# Pure-drain TCP sink used to measure a bot-worker pod's max generation rate:
# accepts every connection, reads+discards, never replies (removes contestant-side
# application backpressure). Built from repo root:
#   docker build -f services/bot-fleet/contestant-drain.Dockerfile -t <ref> .
FROM rust:1.96-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential librdkafka-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
COPY services/telemetry-ingester services/telemetry-ingester
RUN cargo build --release -p iicpc-bot-fleet --example tcp_drain

FROM debian:12-slim
COPY --from=builder /app/target/release/examples/tcp_drain /usr/local/bin/tcp_drain
# Bind the capturable port 9898 (eBPF filters 9898/8080) so the orchestrator's
# TCP readiness probe + the bots can reach it.
ENV BIND=0.0.0.0:9898
EXPOSE 9898
ENTRYPOINT ["/usr/local/bin/tcp_drain"]
