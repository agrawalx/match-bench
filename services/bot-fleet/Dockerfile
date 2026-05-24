FROM rust:1.79-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
RUN cargo build --release -p iicpc-bot-fleet

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-bot-fleet /usr/local/bin/iicpc-bot-fleet
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/iicpc-bot-fleet"]
