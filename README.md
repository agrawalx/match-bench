# IICPC — HFT-Algorithm Benchmarking Platform

IICPC is a multi-tenant, production-grade platform for benchmarking high-frequency-trading matching engines. A contestant uploads a trading algorithm; the platform builds it into a sandboxed image (Kaniko → registry), runs it in an isolated, CPU-pinned pod (one algo pod per node, gVisor-optional), fires a deterministic, barrier-synchronized order load at it from a Rust bot fleet, measures latency **in the kernel** with eBPF at the algo pod's veth, joins the telemetry into HDR histograms, validates correctness by replaying the order stream through a reference price-time-priority CLOB, scores peak-sustained throughput behind latency and correctness gates, and serves a live Next.js leaderboard with Gil-Tene latency-by-percentile charts. Because the product *is* a latency measurement, the measurement path is designed to be **un-gameable** (timestamps are stamped by the kernel on the wire, outside the contestant's sandbox) and the load is designed to be **deterministic and fair** (every contestant receives the byte-identical order stream for a given seed and mix).

## Crown jewels

- **Un-gameable kernel measurement.** The scored metric `service_time = t7 − t3` is stamped by two BPF hooks at the algo pod's veth — `t3` at XDP ingress (before the kernel network stack), `t7` at tc egress — both via `bpf_ktime_get_ns()`. The contestant's userspace never touches the clock. The metric is skew-invariant (same node's CLOCK_MONOTONIC on both stamps, so the per-node offset cancels) and syscall-model-agnostic (io_uring-proof), because timestamping happens at the packet boundary, not the syscall boundary.
- **Fair, deterministic load generation.** A zero-GC Rust/Tokio bot fleet with pre-rendered wire frames fires open-loop, fixed-interval (coordinated-omission-correct) traffic. The order stream is seeded per `(seed, mix)` with a frozen RNG draw order, so a fast and a slow algo get the identical workload. Workers fire synchronously off a Kafka barrier computed *after* ready fan-in.
- **Hardened sandbox isolation.** Each algo runs in a Guaranteed-QoS pod (integer-core, request==limit) so the static CPU manager pins it to an exclusive cpuset; read-only rootfs with RAM-backed tmpfs; all capabilities dropped, no service-account token; default-deny NetworkPolicy that blocks all private/cluster CIDRs; gVisor as a pure config toggle.
- **Cross-language-verifiable HDR telemetry.** Latency is recorded into HDR histograms (1 ns .. 60 s, 3 sig figs) written by Rust (V2-deflate) and read back by three independent stacks — Go (API pass-through), the Next.js frontend (client-side decode), and a Python cross-check — into TimescaleDB + Redis.
- **Authoritative correctness oracle + gated scoring.** A reference price-time-priority CLOB (Go, btree per side + FIFO per level) replays the order stream in TCP head-of-line order and classifies six violation classes. Peak-sustained-TPS is scored only behind per-wave latency (p99) and error-rate SLA gates, with DQ-aware ranking.

## Services

| Service | Lang | Role |
|---|---|---|
| `services/submission-api` | Go | Upload, build trigger, benchmark trigger; JWT-verified API |
| `services/auth-api` | Go | OAuth/JWT issuance for the frontend |
| `services/build-worker` | Go | Spawns ephemeral Kaniko/Trivy/Syft build Jobs (contestant zip → image) |
| `services/sandbox-orchestrator` | Go | Mints one algo Pod + Service per slot; hardened, CPU-pinned, gVisor-optional |
| `services/ebpf-latency` | Rust (aya) | Capture-only XDP/tc data plane + userspace TCP reassembly, FIX/REST/WS parse, per-ClOrdID match → `orders.acked` |
| `services/bot-fleet` | Rust (Tokio) | Deterministic, barrier-synced load generator; three-loop (writer/reader/watchdog) CO instrumentation for FIX/REST/WS |
| `services/bot-fleet-controller` | Go | Computes workload specs + barrier epoch; fans out over Kafka |
| `services/telemetry-ingester` | Rust | `orders.sent` + `orders.acked` → HDR histograms per (session, wave) → TimescaleDB + Redis |
| `services/correctness-validator` | Go | Replays the order stream through the reference CLOB; publishes `scores.correctness` |
| `services/score-computer` | Go | Reads TimescaleDB metrics + correctness; computes gated peak-sustained-TPS and leaderboard scores |
| `services/leaderboard-api` | Go | Read API + SSE for leaderboard, run detail, and live latency charts |
| `frontend/` | Next.js 14 | Public website: upload, live run view, leaderboard, HDR percentile charts |

## Repository layout

```
go.work / Cargo.toml      Go + Rust workspaces (services live in services/*)
schemas/                  Hand-mirrored Go + Rust wire schemas (Kafka topics, fixed-point)
services/*                The services above (Go + Rust)
frontend/                 Next.js 14 SPA
libs/                     Shared Go + Rust libraries (logger, etc.)
k8s/<namespace>/          Kubernetes manifests: data, build, platform, sandbox, benchmark, observability
bootstrap/                Secret bootstrap (templates + gitignored live values); deploy order
deploy-local/             Local k3s bring-up scripts (up.sh, forward.sh, submit.sh)
ops/                      Operational tooling
```

## Quickstart

### Local (single-node k3s)

Requires a running k3s cluster, `kubectl`, and a local image registry. The scripts trim the
committed (EKS) manifests for a laptop: Kafka 1 broker, all replicas 1, gVisor off, tiny scenarios,
NetworkPolicies skipped — without mutating the committed manifests.

```sh
deploy-local/build-images.sh     # build + load service images into the local registry
deploy-local/up.sh               # namespaces, secrets, data tier, services, sandbox, benchmark
deploy-local/forward.sh          # port-forward the public endpoints
deploy-local/submit.sh           # submit a sample algo and trigger a run
```

### Production (AWS EKS)

EKS is deployed from the self-contained runbook **[`DEPLOYMENT_EKS.md`](DEPLOYMENT_EKS.md)** using
the committed manifests under `k8s/<namespace>/` plus imperative secret bootstrap. The platform
spans six namespaces (`data`, `build`, `platform`, `sandbox`, `benchmark`, `observability`) and
relies on a dedicated tainted `sandbox` node group with `cpuManagerPolicy=static`, a kernel that
supports XDP driver-mode capture, ECR, the AWS Load Balancer Controller, and KEDA.

```sh
# 1. namespaces
kubectl apply -f k8s/<namespace>/namespace.yaml

# 2. real secrets (from bootstrap/secrets-live/, NOT the placeholder templates) — see bootstrap/README.md
kubectl apply -f bootstrap/secrets-live/

# 3. follow DEPLOYMENT_EKS.md for node groups, CNI NetworkPolicy enforcement,
#    storage, registry, RuntimeClass, CPU pinning, and the application manifests
```

> Note: secret manifests deliberately do **not** live under `k8s/` — applying placeholders would
> blank real credentials. See [`bootstrap/README.md`](bootstrap/README.md).

