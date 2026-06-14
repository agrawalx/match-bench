# Quick Overview

> **match-bench** — fair, un-gameable benchmarking for high-frequency-trading algorithms.

match-bench runs a contestant's trading algorithm in a locked-down sandbox, fires the *same* deterministic market workload at everyone, measures latency **in the Linux kernel** (outside the algorithm), checks every fill against a reference order book, and publishes a live leaderboard.

**The one idea:** a contestant should only score better by writing a faster, more correct algorithm — never by gaming the measurement. So the platform trusts nothing the algorithm reports: deterministic load *in*, kernel-stamped timing, reference-model correctness *out*. → [The thesis & system overview](the-thesis-system-overview.md)

## How it works in 30 seconds

Upload → build into a container → deploy into a hardened pod → fire deterministic FIX/REST/WS load → the kernel timestamps each request and response at the pod's network boundary → telemetry builds latency histograms while a reference order book checks correctness → score = peak sustained throughput behind quality gates → live leaderboard.
→ [Full run lifecycle](the-benchmark-run-lifecycle.md)

## Components at a glance

| Component | What it does | Key feature |
|---|---|---|
| [Submission API](submission-api-go.md) · Go | HTTP front door — uploads, run-groups, build/run requests | content-hash dedup; one session per scenario |
| [Build worker](build-pipeline-sandbox-orchestrator-go.md) · Go | source bundle → scanned container image | ephemeral Kaniko / Trivy / Syft Jobs |
| [Sandbox orchestrator](build-pipeline-sandbox-orchestrator-go.md) · Go | creates the hardened algo pod + its capture per run | Guaranteed-QoS, integer-core CPU pinning |
| [Bot-fleet controller](bot-fleet-controller-go.md) · Go | orchestrates a run: shard tasks, sync barrier, status | fresh barrier computed *after* readiness fan-in |
| [Bot-fleet workers](bot-fleet-load-generator-rust.md) · Rust | open-loop FIX/REST/WS load generators | catch-up pacing, zero-GC, coordinated-omission-correct |
| [eBPF capture](ebpf-latency-capture-rust.md) · Rust | kernel-boundary timing **and** ordering truth | `t7 − t3`, `effective_t3`, captures every partial fill |
| [Telemetry ingester](telemetry-ingester-rollup-rust.md) · Rust | joins the sent + acked streams into per-wave HDR histograms | co-partitioned shards + lossless HDR merge |
| [Correctness validator](correctness-validator-go.md) · Go | replays the run through a reference order book | six violation classes; `effective_t3` ordering |
| [Score computer](score-computer-leaderboard-api-go.md) · Go | applies the quality gates and ranks | peak sustained TPS behind correctness/latency gates |
| [Leaderboard API](score-computer-leaderboard-api-go.md) · Go | serves the live leaderboard | per-replica SSE broadcast + Redis cache |
| [Foundations](platform-foundations-auth-shared-libraries-schemas.md) | auth, shared libraries, the Kafka schema | one dual-language (Go/Rust) wire contract |

The data tier is **Kafka** (the asynchronous backbone), **Postgres**, **TimescaleDB**, **Redis**, and **MinIO**.

## Go deeper

- **[Kafka topology, partitioning & scaling](kafka-topology-partitioning-horizontal-scaling.md)** — how every service lines up on Kafka and scales horizontally (the co-partitioning trick).
- **[Benchmarking & performance](benchmarking-bottleneck-hunt-scope-for-improvement.md)** — the measured numbers and how each bottleneck was found and fixed.
- **[Deployment & isolation](deployment-isolation-observability.md)** — the node-pool planes, sandbox hardening, and local vs EKS.
- **[Scope for improvement](scope-for-improvement.md)** — the honest open items.

*Everything is in the left sidebar — this page is just the map.*
