# Load-generator capacity benchmark (EKS)

**Goal: how many orders/s can the bot-fleet generate?** Everything else is removed
as a confound — the contestant is replaced by a **drain sink** (read-and-discard,
never replies, so it never back-pressures), eBPF capture is off, and the only
metric is the bot-side **`iicpc_bot_orders_sent`** rate. We then watch that rate
as the load-gen pool scales **1 → 2 dedicated nodes**.

## Topology (Terraform: `infra/terraform`)

| Plane | Node group | Instance | Nodes | Role |
|---|---|---|---|---|
| Measurement (fixed) | `general` | `m6i.2xlarge` | 2 | data tier + Kafka + telemetry (unused here but deployed) |
| Drain sink (fixed) | `sandbox` | `c6i.2xlarge` | 1 | tcp_drain, isolated; capture OFF |
| **Load gen (sweep var)** | `botworker` | `c6i.xlarge` | **1 → 2** | bot-fleet-worker only (tainted) |

## The model (why the numbers line up)

- Every task is an **HFT bot @ exactly 1000 orders/s** — the per-task tokio-timer
  ceiling. You scale by adding tasks, never by faster tasks; catch-up pacing is
  what lets each task actually hold 1000/s at the wall.
- `seed-hft-load.sh N` makes **N×1000 tasks**. With `MAX_TASKS_PER_WORKER=1000`
  the controller computes `worker_count = N`, and `bench-patches.sh N` pins one
  worker per botworker node. So **N nodes ⇒ N×1000 tasks ⇒ target N million/s**,
  and node count is the only variable.

## One-time setup

```bash
# 1. Provision (saved-plan flow, never auto-approve)
cd infra/terraform && terraform plan -out tf.plan && terraform apply tf.plan && cd ..

# 2. Deploy the platform per DEPLOYMENT_EKS.md (single-broker Kafka is fine — we
#    don't read telemetry here). Then build + register the drain sink:
DRAIN_IMAGE=<ecr>/iicpc/contestant-drain:bench deploy-bench/build-drain.sh
DRAIN_IMAGE=<ecr>/iicpc/contestant-drain:bench deploy-bench/seed-drain-submission.sh
#   -> prints SUBMISSION_ID; also turns capture off + reseed off
```

## Sweep

**Point A — 1 load-gen node, target 1M/s:**
```bash
cd infra/terraform && terraform apply -var botworker_desired_size=1 && cd ..
deploy-bench/seed-hft-load.sh 1            # 1000 HFT tasks @1000/s
deploy-bench/bench-patches.sh 1            # 1 worker pinned to the node, KEDA off
deploy-bench/run-sentps.sh <SUBMISSION_ID> # triggers + live sent/s
```

**Point B — 2 load-gen nodes, target 2M/s:**
```bash
cd infra/terraform && terraform apply -var botworker_desired_size=2 && cd ..
deploy-bench/seed-hft-load.sh 2            # 2000 HFT tasks @1000/s
deploy-bench/bench-patches.sh 2            # 2 workers, one per node
deploy-bench/run-sentps.sh <SUBMISSION_ID>
```

`run-sentps.sh` prints `sent/s`, task count, per-task avg, and the running peak.
(Needs Prometheus reachable — `kubectl -n observability port-forward svc/prometheus 9090:9090`.)

## Reading the result

- **Does point A reach ~1M/s?** If yes and **per-task ≈ 1000**, one `c6i.xlarge`
  drives 1M/s and the pacer holds the wall. If per-task < 1000, the load-gen node
  is CPU-bound (or the pacer is the limit) — that's the per-node ceiling.
- **Does B ≈ 2× A?** Clean linear scaling ⇒ load-gen-bound (expected, since the
  drain + capture-off remove every other bottleneck). If B plateaus near A, the
  wall moved off the load gens — check Kafka (telemetry publish path) or NIC.
- Ignore latency / correctness / acked — there are no replies by design.

## Scripts

| File | Purpose |
|---|---|
| `build-drain.sh` | build + push the tcp_drain contestant image |
| `seed-drain-submission.sh` | register drain as a `ready` submission; capture off; reseed off |
| `seed-hft-load.sh N` | seed N×1000 pure-HFT @1000/s tasks as the only scenario |
| `bench-patches.sh N` | pin N workers (one/node) to the botworker pool, KEDA off |
| `run-sentps.sh <sub>` | trigger a run + live `iicpc_bot_orders_sent` rate |
| `kafka-gp3-throughput.yaml` | 500 MB/s gp3 SC (only if Kafka's telemetry path becomes the wall) |
