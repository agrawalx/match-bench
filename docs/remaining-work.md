# Remaining work

Status as of 2026-07-28, branch `feat/bot-tps`. Everything on the adversarial-review
list is closed (32 fixed / 1 mitigated / 1 accepted — see
`docs/branch-review-findings.md`); what remains is verification, a few small features,
the Kafka topology exercise, and a deliberately-parked EKS bucket. Strategy: test
everything possible on a local cluster (k3s/kind); touch EKS only when a task is
physically impossible locally.

---

## A. Code prerequisites (small; unblock the e2e track)

1. ~~**Per-session validator mode selection.**~~ **DONE (72cb924)** — `VALIDATOR_MODE` is a global env var;
   the two-pass design needs the validator to choose full-replay (pass-1
   `correctness` scenario) vs invariants (pass-2 scale scenarios) per session.
   Cleanest source: learn scenario kind from `workload.assignments` the same way the
   validator already learns the session's order band (BandCache pattern), or stamp it
   on the benchmark status event. Prerequisite for B2.
2. **Echo-engine template rendering.** The echo contestant still renders execution
   reports with the pre-P2 `format!` builder and unbatched writes; measured locally
   as roughly half of the pacing knee (300k/s with echo vs 500-700k/s drain). Any
   measurement-capacity claim is echo-capped until this gets the P2 treatment
   (`execution_report_frame`).
3. **Frontend badge for `ResultTainted`** (optional). The taint flag reaches the
   score event; surface it on the leaderboard row so a flagged pass-2 result is
   visibly provisional.

## B. Local-cluster verification track (k3s/kind; eBPF and KEDA both run locally)

1. **Two-concurrent-sessions e2e — highest value item.** Two contestants submitted
   together: parallel controller dispatch, partition + order-band leases, two sandbox
   slots + capture pods, band-scoped validation, two live leaderboard tiles, isolated
   correct scores. Exercises nearly everything built on this branch in one run.
   Includes the live-SSE smoke (Redis → poll loop → `live_metrics` → frontend).
2. **Two-pass flow e2e.** Same submission through the pass-1 `correctness` scenario
   (single-connection max-rate, full book replay) and a pass-2 scale scenario
   (invariants mode); verify the taint path fires on induced t7 lateness/anomalies;
   verify jitter lands on the leaderboard. Depends on A1.
3. **Mixed-protocol run.** `ProtocolAll` submission against the dual-listener
   reference engine: FIX + REST + WS to one contestant simultaneously, eBPF
   capturing 9898 and 8080, per-task targets from Shape A.
4. **Stalled-peer harness.** Sink that stops reading mid-run: proves the
   drain-deadline write exit, the watchdog last-tick pending sweep, and that every
   offered order ends accounted (matched or timed_out, inflight back to zero).
5. **KEDA session-count trigger.** Replace the start-of-run lag pulse with a
   session-count-driven ScaledObject; implement and verify scale-up/down on the
   local cluster.
6. **W calibration.** Reference engine vs a deliberately-naive (thread-per-conn,
   no ingress ordering) engine; compare jitter distributions; set the published
   cross-flow window W where they separate. Fully local.

## C. Kafka topology exercise (wants a running local cluster to measure against)

1. **Single topic ownership.** Topic creation exists in BOTH
   `services/bot-fleet/src/kafka.rs` (retention values disagree with the manifest)
   and `k8s/data/kafka/topic-init-job.yaml`, both `--if-not-exists` — boot order
   decides which config wins. Init Job becomes sole creator; app code only verifies.
2. **Per-topic sizing table** from measured inventory (msg rate, msg size, consumer
   parallelism, replay-window need, per local runs): partitions, replication factor,
   retention, compression per topic — replacing the arbitrary 3/24 two-tier split.
3. **Retention by semantics.** Control/barrier topics are consumed in seconds (hours
   of retention, not 24h); `orders.*` retention = the allowed ingester lag/replay
   window (couple with the `auto.offset.reset=latest` footgun fix); scores belong in
   Postgres, not 30-day Kafka.
4. **`benchmark.requested` → 1 partition** if strict global queue FIFO is wanted
   (tiny volume; 3 partitions only approximate FIFO).
5. **Partition count vs bands revisit.** 24 partitions / width-6 / 4 exclusive bands
   was the interim call; revisit alongside the uniform-lease idea (every session
   gets exactly 6 workload partitions → starvation class disappears, session size
   capped) and whether 32 partitions buys anything.
6. **Disk/compression plan** (from the sizing discussion): gp3 throughput is
   provisioned separately from volume size — default 125 MB/s was the likely
   producer-backpressure culprit; target ~500 MB/s per broker for lz4/RF=2 or
   250 for zstd/RF=1 when back on EKS. zstd-1 on `orders.*` is already shipped.

## D. EKS-only (parked; touch only when unavoidable)

- Karpenter node autoscaling (sessions → pods → nodes).
- Graviton (c7g) botworker pool: 1 vCPU = 1 physical core, bot-fleet is pure Rust,
  no eBPF on those nodes; needs multi-arch images + arm64 node group only.
- gp3 throughput provisioning per C6.
- cpuset NodeConfig conflict (`reservedSystemCPUs` vs EKS-managed kubelet args) —
  the static-CPU-manager behavior itself can be rehearsed on k3s first.
- Frame-pointer image builds for on-cluster profiling.
- **Final benchmark numbers — last, after everything above.** Baseline to beat:
  747,911 orders/s single-node drain (pre-P1/P2); local 4-thread drain reached
  2.2–2.6M/s; EKS expectation ~1.1–1.4M/s per c6i.xlarge node.

## Housekeeping

- ~25 commits on `feat/bot-tps` unpushed: `git push fork feat/bot-tps`.
- Eventually: PR to main; ARCHITECTURE.md "Where this is going" chapter will need a
  refresh once B/C land (it describes some of this as future work that is now done).

## Suggested sequence

A1 → local k3s bring-up → B1+B2 in one cluster session → C on that same cluster →
B3–B6 → D when AWS access returns → final bench.
