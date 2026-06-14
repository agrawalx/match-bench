## Scope for Improvement

This page consolidates every limitation flagged in the service sections, grouped
by theme and ordered roughly by impact. Each item is grounded in a file or
symbol so it can be picked up directly. Nothing here is hypothetical polish — it
is the real gap between what the code does today and what the design aspires to.

### The handful that matter most

1. **Exclusive-core cpuset pinning is OFF** (`enable_sandbox_cpuset=false`,
   every committed tfvars). The contestant gets its cores via Guaranteed-QoS
   *quota*, but not an *exclusively pinned* cpuset, because EKS's auto
   kube/system-reserved CPU conflicts with the `reservedSystemCPUs` NodeConfig
   and the kubelet refuses to start (`infra/terraform/main.tf:174-198`). The code
   path that *would* pin (integer-core validation + Guaranteed QoS in
   `sandbox-orchestrator/internal/k8s/slot.go:117`, `:607`) is correct and ready;
   it's the node config that's blocked. **This is the single biggest measurement-
   fidelity gap** — "fair, exclusive cores" is currently quota-only.
2. **The 2M/s tier is a documented target, not a validated run**. Two pieces are explicitly UNVERIFIED: eBPF capture
   throughput at ~2M acks/s (a single capture pod) and echo-contestant capacity
   at 2M/s. The drain variant (send + telemetry + ingester) is the first thing to
   validate now that Kafka is multi-broker.
3. **The correctness-validator does not shard** and is the least-scalable
   component. It replays *every* order through a reference book, so memory and
   wall-clock scale with **total order volume**; it OOMs/timeouts if a run
   overshoots the delivered ceiling. It's deployed `replicas: 1` with no KEDA;
   the code is replica-safe (consumer group + `ON CONFLICT` claim) but the
   autoscaling story is unrealized.
4. **The per-worker telemetry aggregator is a single Tokio task.** Each bot-fleet
   worker funnels all of its `orders.sent` telemetry through one mpsc channel into
   one `run_aggregator` task (`telemetry.rs:62-72`). It has been hardened
   (`recv_many` bulk drain, async delivery), but sharding it into M tasks (the
   audit's T1 fix) is unimplemented — a candidate cap on a *single* worker's
   telemetry-on rate (~445k/s vs ~600–790k/s raw), alongside the single-broker
   smoke cluster. It does not block the multi-worker 2M/s path (one aggregator
   *per worker*), but it is the named lever to close the telemetry-on gap.
5. **Auth is disabled in the shipped demo.** `AUTH_REQUIRED=false` +
   `OptionalContestant` (submission-api) and a hardcoded `authDisabled=true`
   (`frontend/src/config/platform.ts:44`) mean identity comes from an *unverified*
   token or a static default. The capability exists and is tested; it's toggled
   off — a gap for a real multi-tenant contest, not for a benchmark harness.

### Measurement fidelity

- **eBPF capture is a single-NIC, CPU-bound singleton.** One capture per
  contestant veth; the whole userspace drain/parse/publish is one loop on one
  64 MiB ring. A hot contestant can only be helped with more CPU on its one pod
  (raised 2→4 vCPU already), never more replicas. Validated lossless at
  **≥ ~144k samples/s**; the per-contestant ceiling above that is one CPU.
- **Single-pod contestant ceiling ~150k delivered/s** (one pod, one TCP
  connection per task). This caps *before* the measurement pipeline does — to find
  the pipeline's own ceiling you must add responder pods / cores.
- **`net-tune` / `gro-disable` are best-effort privileged loops.** The
  gro-disable DaemonSet re-applies `ethtool … off` every 2 s forever with no
  convergence signal; a window between a new VPC-CNI ENI attaching and the next
  loop can still let a GRO-coalesced (>1536 B) frame through and truncate a
  capture. Driver-mode XDP would remove the need, but the veth forces generic
  mode (post-GRO).
- **`orders.acked` is intentionally loss-tolerant** (kernel sample-clamps
  oversized frames; userspace drops `QueueFull` batches). Correct for a latency
  *distribution*, but it is not exactly-once — losslessness depends on the MTU/GRO
  fix and a non-saturated broker.
- **gVisor is off by default** (`RUNTIME_CLASS=""`); isolation leans on the
  hardened security context + default-deny NetworkPolicy. It's a pure config
  toggle, so enabling it is `runsc` + an env flip, no code change.
- **Capture attaches after the algo is serving** (orchestrator spawns the capture
  Job only once a `Refresh` sees the slot `Ready`) — a small race window for the
  earliest packets.

### Throughput & scaling

- **Telemetry-on per-worker ceiling (~445k/s) < telemetry-off (~600–790k/s).**
  The single broker in the smoke cluster caps durable telemetry, and the
  serialization audit's **T1 "shard the aggregator across M tasks"** lever is
  identified but **not implemented** — `record` still drains through one Tokio
  task per worker. The 2-broker split + aggregator sharding are the named fixes.
- **1→2-node linear scaling is argued from architecture, not measured.**
  `deploy-bench/scale-sweep-off.tsv` has only zero rows; "2 nodes ≈ 1.9×" rests on
  "independent workers, separate NICs," which is sound but unswept. Run
  `deploy-bench/drain-scale-sweep.sh "1 2"` to populate it.
- **Worker fan-out is hard-capped at 24** (`validateWorkerCapacity`,
  `bot-fleet-controller/internal/controller/producer.go:88`) because
  `workload.assignments` and `orders.*` are 24 partitions. Scaling past 24
  generators per session is a coupled topic-repartition, not a config flag.
- **Hard singletons with in-memory state.** `bot-fleet-controller` holds all
  per-session barrier/fan-in state in memory (a crash mid-run loses in-flight
  fan-in; `RecoverInFlightRuns` only fails them out cleanly). `telemetry-rollup`
  is a single process doing per-tick full-history HDR merges — the serialization
  point for final percentiles. `score-computer` is effectively a singleton whose
  per-run-group cost is a global `ROW_NUMBER` rank (O(rows)); fine at contest
  scale, would need an incremental/ZSET rank if the table grew large.
- **No KEDA on most scale-ready services.** Only bot-fleet autoscales. The
  ingester, validator, score-computer, leaderboard-api, and submission-api are
  fixed-replica Deployments despite being scale-safe — capacity is provisioned,
  not autoscaled.

### Correctness & behavioral gaps

- **Aggressive-fill tolerance is currently inert.** `main.go` wires the
  *streaming* validator, which honors only the strict (tolerance==0) path; the
  tolerant branch lives in the retired *batch* `validate.Run`. So
  `AGGRESSIVE_FILL_TOLERANCE_US` is read but currently has no effect — porting it
  into the streaming path is a future enhancement.
- **`workload.failed` is produced but not consumed** by the controller, so a
  worker that aborts mid-run is invisible; trouble is only detected via missing
  `bot.ready` (`ReadyDeadline`) or a run timeout.
- **Partial fan-in fires anyway** — if some workers miss the `ReadyDeadline`, the
  run proceeds with fewer workers and is still scored (degrade-don't-stall, but a
  degraded run isn't flagged).
- **`rank_delta` is hardcoded to 0** (`score-computer/internal/worker/worker.go:86`)
  — the "moved up/down" signal on the leaderboard is dead.
- **`scores.correctness` partition key is decorative** — the validator sets
  `Key=session_id` but uses the `LeastBytes` balancer, which ignores keys.
  Harmless (aggregation is in SQL) but misleading.
- **Self-trade detection is order_id-format-coupled** — `model.ParticipantOf`
  parses the bot id by string position; a malformed id silently weakens the check.
- **The validator's settle delay is a fixed 10 s heuristic**, not completeness-
  driven; a telemetry write later than 10 s + the 500 ms join window could
  under-count fills (surfaced via the sent/acked/matched coverage counters).

### Reliability & operations

- **Single-replica data tier.** Postgres, TimescaleDB, Redis, MinIO, Loki,
  Prometheus, Grafana are all `replicas: 1` — each a SPOF, each EBS-AZ-pinned.
  Fine for a re-runnable benchmark, not for production durability.
- **Kafka StatefulSet hardcodes `replicas: 3`** (`k8s/data/kafka/statefulset.yaml:15`)
  with a fixed 3-voter KRaft quorum, **independent of `kafka_desired_size`**. The
  tfvars describe 1–2 brokers, but the StatefulSet still tries to schedule 3 (the
  3rd stays `Pending` under anti-affinity); the bench "2-broker" claim likewise
  doesn't match. Running fewer brokers requires editing the StatefulSet.
- **Build pipeline throughput is bounded by 3 partitions.** Serial per replica,
  ≤3 concurrent cluster-wide, `BackoffLimit=0` (a transient kaniko/registry blip
  fails the submission terminally), only cpp/rust/go templates, and ECR
  `image_ref` is `:latest` per submission (a rebuild overwrites rather than
  versioning by digest).
- **submission-api edges:** the build-request publish is best-effort (a Kafka
  failure leaves the submission at `uploaded` with no reconcile loop); a Postgres
  failure after a MinIO upload orphans the artifact (logged, not GC'd);
  `RecomputeRunGroupStatus` is a read-modify-write without a CAS (eventually
  consistent, transient stale parent status possible); scenario seeding runs on
  every replica.
- **Ingester `auto.offset.reset=latest` + auto-commit** means a late-starting or
  restarted replica skips the backlog and *reads as* telemetry loss when it isn't
  — an operational footgun to pin before re-measuring loss.
- **Two diverging retention values** for `orders.sent`/`orders.acked` (6 h in the
  in-cluster Job vs 24 h in the script / bot-fleet self-create). Because topics
  are `--if-not-exists`, the effective retention depends on which creator runs
  first.
- **Loki shipping is in-process and lossy** (drops + counts under backpressure),
  with a single Loki replica and a hard per-service `LOKI_URL` dependency — log
  completeness isn't guaranteed during bursts.
- **leaderboard-api snapshot-on-connect is uncached** — a connection storm of new
  SSE clients each hits Postgres for the top-100, bypassing the 2 s Redis cache
  that fronts the steady-state query.

### Contract & schema

- **No Go `partition_for`.** The FNV-1a co-partition hash lives only in the Rust
  schema crate (both `orders.*` producers are Rust). Fine today, but a future Go
  producer to `orders.sent`/`orders.acked` would have to port it exactly or break
  co-partitioning.
- **Go/Rust schemas can drift.** They mirror each other by convention plus
  decode-the-other's-payload tests, not a generated single-source IDL; a field
  added in one language and forgotten in the other only surfaces when a test or a
  live message fails.

### Security & multi-tenancy

- **VPC-CNI NetworkPolicy is not enforced by default** on EKS — every isolation
  policy is a silent no-op until `enableNetworkPolicy` is on, so a contestant
  could reach Postgres/Kafka/MinIO/other contestants while `kubectl get netpol`
  shows the policies present. `infra/scripts/netpol-deny-test.sh` is the hard gate
  that asserts denial before allowing a deploy.
- **`auth-api` does no ID-token signature verification** (`parseClaims`) — safe
  only because the token comes straight from Google in the code exchange; the
  `platform_token` it returns is just the Google `id_token` passed through (no
  platform-minted, revocable token), so lifetime/revocation are Google's.

### Environment

- **EKS Free-plan 2-vCPU cap.** New AWS Free-plan accounts can only launch
  free-tier instances (largest `m7i-flex.large`, 2 vCPU). The platform can't run
  correctly there (the data tier alone needs ~5 vCPU of requests; exclusive
  cpuset needs ≥4 vCPU). The remedy is a paid plan; otherwise the **local k3s
  harness** runs the full stack with none of these caps for a no-cost demo. (This
  is why the cloud cluster was stood up, validated, and torn down, and the demo
  runs on k3s while the repo ships the paid-account cluster config.)

---
