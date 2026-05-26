FROM rust:1.79-bookworm AS builder
WORKDIR /app

# librdkafka-dev provides headers + pkg-config for dynamic linking against
# the system librdkafka (rdkafka crate `dynamic-linking` feature). Debian
# Bookworm ships librdkafka 2.3.0, which is why Cargo.toml pins rdkafka =
# 0.36.0 — newer rdkafka requires a newer librdkafka than the apt repos
# offer.
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
RUN cargo build --release -p iicpc-bot-fleet

FROM debian:bookworm-slim
# librdkafka1 is the shared library the binary linked against at build
# time. Without it the binary aborts at startup with "cannot open shared
# object file: librdkafka.so.1".
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-bot-fleet /usr/local/bin/iicpc-bot-fleet
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/iicpc-bot-fleet"]
