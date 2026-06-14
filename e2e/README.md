# `e2e/` — end-to-end benchmark suite (clone → deploy → run)

A full end-to-end run of the IICPC benchmarking platform on a 5-node EKS cluster:
a load generator drives FIX/REST/WS orders at a contestant, an **eBPF** sidecar
measures kernel-stamped latency, the **telemetry** plane builds per-(session,wave)
HDR histograms, and the **correctness-validator** diffs the contestant's fills
against a reference order book — **every service running, nothing stubbed**.

Architecture diagram: [`../mermaids/node-architecture-current.mermaid`](../mermaids/node-architecture-current.mermaid)
(render at mermaid.live). Performance/bottleneck writeup:
[`../docs/platform-performance-and-scaling.md`](../docs/platform-performance-and-scaling.md).

---

## Topology (5 nodes, 3 isolated planes — `e2e.tfvars`)

| pool | nodes | taint | runs |
|---|---|---|---|
| **general** | 3× m6i.xlarge | none | measurement plane: **Kafka (2 brokers)**, Postgres, TimescaleDB, Redis, MinIO, telemetry-ingester(+rollup), correctness-validator, controllers, APIs, score-computer, build-spawner, observability, frontend |
| **sandbox** | 1× c6i.xlarge | `sandbox=true:NoSchedule` | the contestant pod (`ALGO_CPU=2`, `ALGO_MEMORY=4Gi`) + its privileged eBPF capture |
| **botworker** | 1× c6i.xlarge (max 2) | `botworker=true:NoSchedule` | the bot-fleet load generator |

Taints keep the three planes physically isolated (only the worker tolerates
`botworker`; only orchestrator-spawned algo/capture pods tolerate `sandbox`;
everything else lands on `general`).

## Contestants (two run flows)

| zip / image | what it is | result |
|---|---|---|
| **`contestant-matching-engine.zip`** | a correct price-time-priority limit order book (port of the validator's reference engine) | **QUALIFIED** (correctness ≥ 0.95) — *the reference contestant* |
| `contestant-echo` (image) / `contestant-fix-acker.zip` | just ACK every order, no order book | disqualified on correctness — **throughput contestants** |

- **Frontend flow (qualified):** `kubectl -n platform port-forward svc/frontend 8087:8080`
  → open http://127.0.0.1:8087 (**no sign-in — auth is removed**) → upload
  `contestant-matching-engine.zip` → it builds (Kaniko→ECR) → run. This is the path
  that produces a green qualified run with real correctness verdicts + HDR latency.
- **Terminal flow (throughput):** `03-submission.sh` registers the pre-built echo
  image, then `run.sh <scenario>`. Fast to drive the throughput/latency ceiling.

## Scenarios (default — `04-scenarios.sh`)

| name | shape | peak rate | duration |
|---|---|---|---|
| constant | flat | **50k/s** | 60s |
| spike | 40s / 20s / 40s | 50k → **100k** → 50k | 100s |
| ramp | linear | **20k → 100k** | 120s |

All use a churning order mix (**market 10 / cancel 30 / replace 10 / limit 50**): the
cancels + crossing prices keep the reference order book *bounded*, so both the
matching-engine contestant (4Gi) and the validator (8Gi) stay within memory. Peaks
≤100k sit **comfortably under the delivered ceiling** (table below), so no acks are
dropped → clean correctness. **To stress throughput**, raise the rates and use the **echo** contestant
with telemetry off — the matching engine + validator can't keep up past the ceiling
(that's expected; correctness needs bounded volume).

## Capture fidelity on EKS (jumbo frames) — REQUIRED for latency measurement

EKS VPC-CNI nodes default to **MTU 9001 (jumbo) with segmentation offloads on**. The
eBPF capture copies at most `CAPTURE_CAP = 1536 B` per frame, so any GSO/TSO/GRO
super-frame is **truncated** → FIX framing is corrupted → the request reassembly is
reset → requests never match responses → **~98% of latency samples are lost** (the
capture logs `truncated oversized captures (check GSO/TSO off)`). This is silent: runs
"succeed" but `service_time` is near-empty and `unmatched_responses` is huge.

Two config changes pin every frame ≤1500 B (no code change to the capture):
1. **Sender:** the bot-fleet worker's `net-tune` initContainer sets its `eth0` MTU 1500
   + GSO/TSO off (in `k8s/benchmark/bot-fleet/deployment.yaml`).
2. **Receiver:** `k8s/sandbox/gro-disable-daemonset.yaml` disables GRO on the **sandbox
   nodes' host interfaces**, so cross-node request segments aren't re-coalesced before
   the (generic-mode) XDP capture sees them.

`02-bootstrap.sh` applies both. Verify after a run: `unmatched_responses` should be a
handful (not millions) and `iicpc_ebpf_ringbuf_dropped` ~0. With the fix the match rate
is ~99.9% (was ~2%).

## Validated ceilings

| layer | ceiling | notes |
|---|---|---|
| generation (telemetry OFF, drain) | ~600–790k/s per botworker node | scales ~linearly with nodes |
| telemetry ON (single worker → 1 broker) | ~445k/s | lossless `record()` backpressure |
| **measurement pipeline (capture→Kafka→ingester)** | **lossless ≥ ~144k samples/s**, no drops, ceiling not yet reached | with the fidelity fix above |
| single contestant pod (echo, cross-node) | ~150k delivered/s | one pod + one TCP conn/task; caps before the pipeline does |
| kernel-stamped service-time p99 | ~98µs healthy | eBPF, independent of load-gen jitter |

To drive the measurement pipeline to its *own* ceiling you must stop bottlenecking on a
single contestant (multiple responder pods / more cores). Everything scales out — see
the **[Horizontal scaling guide](../docs/horizontal-scaling.md)** for what-increases-what,
Kafka/worker/ingester scaling, and how not to get OOMed.

## Files / run order

| file | what it does | who runs it |
|---|---|---|
| `cluster.md` | terraform apply/destroy + kubeconfig + re-scaling | **you** |
| `e2e.tfvars` | 5-node topology (mirrors `terraform.tfvars`) | terraform |
| `lib.sh` | shared helpers (ecr login, waits, kafka produce, psql) | sourced |
| `01-images.sh` | build + push 12 platform images + echo (tag = git SHA) | after cluster up |
| `02-bootstrap.sh` | FULL stack: 2-broker kafka + data + all services + eBPF + build-worker + frontend(auth off); validated sizing | after images |
| `03-submission.sh` | (terminal flow) register the echo contestant | after bootstrap |
| `04-scenarios.sh` | seed constant / spike / ramp | after bootstrap |
| `run.sh <name>` | (terminal flow) trigger a scenario, wait, print latency | per test |
| `assert.sh <session>` | gate: latency rows, zero drops, validator pass, no OOM | per test |

## Quick start

```bash
# 1. cluster (see cluster.md) — ~15 min, you run terraform
cd infra/terraform && AWS_PROFILE=iicpc terraform apply
AWS_PROFILE=iicpc aws eks update-kubeconfig --name iicpc-prod --region us-east-1

# 2. platform (from repo root)
e2e/01-images.sh            # prints TAG
export TAG=<that tag>
e2e/02-bootstrap.sh
e2e/04-scenarios.sh

# 3a. QUALIFIED run — frontend
kubectl -n platform port-forward svc/frontend 8087:8080   # http://127.0.0.1:8087, no sign-in
#     upload e2e/contestant-matching-engine.zip -> build -> run -> qualified

# 3b. THROUGHPUT run — terminal
e2e/03-submission.sh
e2e/run.sh constant && e2e/run.sh spike && e2e/run.sh ramp
e2e/assert.sh <session-id>
```

## Play around
- **Bigger throughput:** raise the rates in `04-scenarios.sh` (e.g. constant 300k),
  use the **echo** contestant, set `BOT_DISABLE_TELEMETRY=1` on the worker for the
  pure send-rate. Validation won't complete past the ceiling — that's the throughput
  mode, not the correctness mode.
- **Horizontal-scaling sweep:** scale botworker 1→2 (see `cluster.md` re-scaling) and
  N×1000 tasks; one node ≈ N million/s of generation.
- **Your own contestant:** zip a `benchmark.yaml` + build file + `src/` (see any
  `contestant-*/`) and upload it in the UI. A correct order book qualifies; an acker
  doesn't.

## Status
- ✅ topology, 2-broker Kafka, eBPF capture, telemetry, validator
- ✅ **capture fidelity fix** (jumbo-frame truncation) — worker net-tune initContainer + gro-disable DaemonSet → ~99.9% match rate
- ✅ **telemetry tail-censoring fix** — `service_time` excludes client-timed-out orders, so `service p99 ≤ response p99` always (ingester drops timed-out orders' acks; `TIMED_OUT_IDLE_NS=15s`)
- ✅ frontend with auth removed (submit a zip, no sign-in); throughput chart sums per-wave rows per timestamp (no boundary whipsaw)
- ✅ reference **matching-engine** contestant (qualifies) + responding-drain echo/fix-acker (throughput)
- ✅ default scenarios sized for a clean qualified run; ceilings + scaling documented
- 📈 measurement pipeline verified lossless to ~144k samples/s; see [horizontal-scaling.md](../docs/horizontal-scaling.md)
