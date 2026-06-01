# Build context is the repository root (COPY paths are repo-relative).

# Stage 1 — compile the kernel-side BPF object.
# Needs the nightly toolchain + rust-src (for -Z build-std=core) and bpf-linker
# (which links against LLVM). The object is target/bpfel-unknown-none/release/
# libiicpc_ebpf_latency.so and is baked into the runtime image below.
FROM rust:bookworm AS bpf-builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends llvm clang libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN rustup toolchain install nightly --component rust-src \
    && cargo install bpf-linker --locked
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
RUN cargo +nightly build --release -p iicpc-ebpf-latency --lib \
    --target bpfel-unknown-none --features ebpf -Z build-std=core

# Stage 2 — compile the userspace binary (dynamic-linked librdkafka).
FROM rust:bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
RUN cargo build --release -p iicpc-ebpf-latency

# Stage 3 — runtime. Runs as root (needs CAP_BPF/CAP_NET_ADMIN/CAP_SYS_ADMIN +
# setns into the algo pod's netns); the Job's securityContext grants those.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-ebpf-latency /usr/local/bin/iicpc-ebpf-latency
COPY --from=bpf-builder /app/target/bpfel-unknown-none/release/libiicpc_ebpf_latency.so \
     /opt/iicpc/ebpf/libiicpc_ebpf_latency.so
ENV EBPF_OBJECT_PATH=/opt/iicpc/ebpf/libiicpc_ebpf_latency.so
ENTRYPOINT ["/usr/local/bin/iicpc-ebpf-latency"]
