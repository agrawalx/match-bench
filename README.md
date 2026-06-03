# IICPC — High-Frequency-Trading Algorithm Benchmarking Platform

Contestants submit a trading-engine (a matching engine reachable over FIX/REST/WebSocket). The
platform builds it into a container, security-scans it, deploys it in an isolated sandbox, fires a
deterministic, market-realistic stream of synthetic orders at it, and scores it on **latency,
throughput, and correctness** — then ranks everyone on a live leaderboard.

The hard problem the whole platform is built around is **fair, un-gameable measurement**. A
contestant's engine is untrusted code that could lie about its own timing, accept orders out of
order under load, or report fills it never made. So the platform never trusts the contestant for
anything that affects the score: it measures latency **in the kernel** at the pod's network
interface before any contestant userspace runs, and it re-derives correctness by **replaying the
order stream through its own reference exchange**. Everything below follows from that stance.

Two diagrams, kept out of this file so they stay readable:

- **`mermaids/end_to_end_pipeline.mermaid`** — one run, click to score, with every Kafka message's
  key/encoding/acks and the measurement spine inline. This is the technical view.
- **`mermaids/k8s_namespaces.mermaid`** — the cluster: namespaces and what lives where.

---

## Services

Every service is its own module (`go.mod` / `Cargo.toml`). They share only Kafka message schemas
and never import each other; the pipeline is Kafka-decoupled, so load generation, sandboxing,
measurement, and scoring are independent stages that fail independently.

| Service | Lang | Role | Status |
|---|---|---|---|
| `submission-api` | Go | Uploads, mints `session_id`, triggers builds and benchmark runs | built |
| `build-worker` | Go | Builds the contestant image (Kaniko), scans it (Trivy/Syft), promotes it | built |
| `sandbox-orchestrator` | Go | Spawns/tears down one isolated algo Pod + Service per run; launches the per-slot eBPF capture Job | built |
| `bot-fleet-controller` | Go | Single-replica run conductor: fans out workloads, fires the start barrier | built |
| `bot-fleet` | Rust + Tokio | KEDA-scaled load workers: deterministic FIX/REST/WS generation, barrier-synced fire | built |
| `ebpf-latency` | Rust + aya | The measurement engine: kernel hooks copy packets, userspace reassembles + matches, emits `orders.acked` | built + verified |
| `telemetry-ingester` | Rust | Joins `orders.sent` + `orders.acked`, HDR percentiles to TimescaleDB + Redis | built + verified |
| `correctness-validator` | Go | Post-run reference-order-book replay, six violation classes, emits `scores.correctness` | built + verified |
| `schemas` / `libs` | Go + Rust | Shared Kafka contracts; shared logging/metrics/health | built |
| scoring-service, leaderboard-api + SSE, frontend | Go / Next.js | Per-wave TPS scoring, rankings, live fan-out, dashboard | designed |

The measurement spine is proven end-to-end on a live cluster at ~500k orders/session. The remaining
layer is scoring to leaderboard to frontend.

---

## Engineering highlights

Every claim below points at the exact code. Grouped by subsystem.

### Measurement you cannot game (`services/ebpf-latency`)

This is the crown jewel. The contestant never touches a number that affects its score.

- **Timestamps stamped in the kernel, at the veth, before contestant userspace runs.** An XDP
  program stamps request ingress (`t3`) and a tc-clsact egress classifier stamps response egress
  (`t7`); the scored metric is `service_time = t7 - t3`. `services/ebpf-latency/src/ebpf.rs:162`
  (`iicpc_xdp_ingress`), `services/ebpf-latency/src/ebpf.rs:170` (`iicpc_tc_egress`).
- **Syscall-model-agnostic.** It hooks packets, not syscalls, so `recv`/`recvmsg`/`io_uring`/kernel-
  bypass all measure identically — there is no blind spot for the fastest contestants.
  `services/ebpf-latency/src/ebpf.rs:285` (`xdp_payload_bounds`), `:337` (`tc_payload_bounds`); there
  is no tracepoint/kprobe program anywhere.
- **Skew-invariant by construction.** Both stamps come from one node's clock
  (`bpf_ktime_get_ns`, monotonic) and userspace adds a single once-sampled monotonic-to-realtime
  offset; because `t3` and `t7` share that one offset, `t7 - t3` is exact regardless of offset
  accuracy and needs no cross-node clock sync or PTP. `services/ebpf-latency/src/pipeline.rs:138`
  (`realtime_minus_monotonic_ns`), applied at `:34`; subtraction at
  `services/ebpf-latency/src/matcher.rs:119`.
- **Parsing kept out of the kernel on purpose.** The kernel only copies TCP payload; reassembly,
  framing, and ClOrdID matching happen in userspace over a plain HashMap, dodging BPF-verifier limits
  and supporting arbitrary contestant message layouts (FIX framed by BodyLength tag 9, chunked HTTP,
  masked WebSocket). `services/ebpf-latency/src/parse.rs:74` (`frame_fix`), `:179`/`:231`.
- **Provably-bounded kernel copy via a verifier trick.** `ptr::read_volatile` on the clamped length
  keeps the `1..=CAPTURE_CAP` bound opaque to the optimizer so the `bpf_xdp_load_bytes` /
  `bpf_skb_load_bytes` copy survives verification. `services/ebpf-latency/src/ebpf.rs:194`, `:221`,
  `clamp_cap` at `:244`.
- **TCP reassembly with first-byte timestamp attribution.** Handles coalescing (many orders per
  segment) and straddling (one order across segments); a straddled message's timestamp is the arrival
  of its **first** byte, not its last. `services/ebpf-latency/src/reassembly.rs:205` (`timestamp_at`),
  marks recorded at `:73`.
- **Out-of-order, retransmit, and an anti-stall overlap fix.** Future segments are held and drained
  when the gap fills, duplicates are counted not re-delivered, and a held segment a later gap-filler
  advances into the *middle* of still has its tail drained instead of stranding the flow forever.
  `services/ebpf-latency/src/reassembly.rs:95` (`drain_hold`), regression test at `:331`.
- **One packet = one order: offloads disabled at attach.** The capture runs
  `ethtool -K <veth> tso/gso/gro/lro off` inside the algo netns before attaching, so per-packet
  timestamps equal per-order timestamps; truncated GSO super-frames re-anchor the flow instead of
  desyncing. `services/ebpf-latency/src/main.rs:333` (`disable_offloads`), truncation reset at
  `pipeline.rs:54` to `reassembly.rs:190`.
- **One event per response, including every partial fill.** Unlike the bot's pending map, the matcher
  retains the entry after the first response, so each ExecutionReport emits its own `orders.acked`
  sharing the request's `t3`. `services/ebpf-latency/src/matcher.rs:103` (`on_response`).
- **Survives load and broker hiccups.** Per-CPU drop/truncation counters make capture loss observable
  rather than silent; publish is size-bounded chunked (<=1000 events/msg, drains only what Kafka
  accepted) and non-fatal (bounded backlog, drop-oldest past 100k) so a transient outage never kills
  the per-slot Job. `services/ebpf-latency/src/main.rs:209` (`flush`), `:265`-`277`, counters at
  `ebpf.rs:273`/`:246`.

### Sandbox fairness and isolation (`services/sandbox-orchestrator`, `k8s/sandbox`)

- **Security lives in one auditable, unit-tested function — no admission engine.** The orchestrator
  is the single writer of contestant pod specs, so hardening is hardcoded in the pod template and
  unit-tested, rather than policed cluster-wide by Kyverno (which would add a misconfigurable layer
  for a threat that does not exist with one writer). `services/sandbox-orchestrator/internal/k8s/slot.go:460`
  (`SecurityContext`), test `slot_bugfix_test.go:29`. There is zero Kyverno/Gatekeeper/webhook in the repo.
- **Hardened container without breaking legit images.** `AllowPrivilegeEscalation:false`,
  `Capabilities.Drop:[ALL]`, `SeccompProfile:RuntimeDefault`, `ReadOnlyRootFilesystem:true`, and no
  service-account token — but `RunAsNonRoot` is deliberately left to gVisor rather than forced.
  `services/sandbox-orchestrator/internal/k8s/slot.go:460-465`, `:408`.
- **gVisor as the real isolation boundary, env-toggled.** Production runs every contestant under the
  gVisor userspace kernel (`runtimeClassName`); dev leaves it unset so the same code path runs on k3s.
  `services/sandbox-orchestrator/internal/k8s/slot.go:471-474`, test `slot_test.go:143`.
- **Jitter-free latency via Guaranteed QoS + integer CPU = exclusive cpuset.** request==limit with an
  *integer* core count is what makes the kubelet hand the pod a dedicated cpuset (millicores would
  fall back to CFS throttling that corrupts latency comparisons); config validation rejects non-integer
  CPU. `services/sandbox-orchestrator/internal/k8s/slot.go:723` (`containerResources`), validation `:157`.
- **Zero disk-I/O contention.** `readOnlyRootFilesystem` plus four RAM-backed tmpfs mounts
  (`/tmp,/var/tmp,/var/log,/var/run`) mean a contestant's heavy logging burns its own `memory.max`
  budget, not a neighbour's disk bandwidth. `services/sandbox-orchestrator/internal/k8s/slot.go:500-526`.
- **Default-deny network: bot ingress only, internet egress, cluster blocked.** Contestants can pull
  public dependencies but provably cannot reach Postgres/Kafka/MinIO or another contestant's pod.
  `k8s/sandbox/network-policy.yaml:11-40`; even the privileged capture Job's one egress hole is pinned
  to Kafka :9092 (`:48-78`).
- **A contestant cannot silently evade measurement.** A slot on any port outside the capturable set
  `{8080,9898}` is rejected with HTTP 400 at `CreateSlot`, rather than coming up Ready and producing
  zero timings. `services/sandbox-orchestrator/internal/k8s/slot.go:53`, enforced `:189`.
- **Privileged measurement can never outlive or perturb what it measures.** The capture Job is
  Burstable (kept off the algo's pinned cores), GC'd three ways (ownerReference to the algo Pod, TTL
  300s, startup orphan sweep), and both pods carry `activeDeadlineSeconds=3600` as a leak backstop.
  `services/sandbox-orchestrator/internal/k8s/slot.go:693` (resources), `:630`/`:598`/`:351` (GC),
  `:423`/`:604` (deadlines).

### Load generation: deterministic and coordinated-omission-correct (`services/bot-fleet`)

- **Byte-identical reproducibility.** Each task seeds its RNG from `global_seed XOR task_id` with a
  documented draw-order contract, so any run replays exactly — essential for disputed scores.
  `services/bot-fleet/src/worker.rs:824`, `services/bot-fleet/src/content.rs:133`/`:147`.
- **Fixed-interval pacing that a slow engine can't game.** `sleep_until` on an absolute schedule with
  `next_send_ns += interval_ns` that never recoils by work duration, and `t0` captured **before** the
  sleep (the schedule's intent, not a clock read) — so under coordinated omission `t1 - t0` grows and
  becomes the back-pressure signal instead of hiding the latency. `services/bot-fleet/src/worker.rs:840`
  (t0), `:845` (sleep), `:931` (advance).
- **Three-timestamp CO instrumentation with first-response-wins + watchdog.** `t0` intended, `t1`
  post-write, `r9` first response; the first ExecutionReport consumes the pending entry (no
  double-count) and a 5s watchdog emits the rest as `timed_out=true`, distinguishing lost from
  instant-zero. `services/bot-fleet/src/worker.rs:840`/`:888`/`:1005`, eviction `:1089`.
  (Full three-loop CO instrumentation is FIX-only; REST/WS are write-only.)
- **Simultaneous fire across the whole fleet without PTP.** A Kafka barrier carries an absolute epoch
  computed **after** fan-in (never stale-in-the-past) plus a 500ms safety gap; each worker converts it
  to a local deadline and `sleep_until`. `services/bot-fleet/src/worker.rs:293` (wait),
  `services/bot-fleet-controller/internal/controller/runner.go:270` (epoch after fan-in).
- **From-scratch FIX engine with an O(1) hot-path timestamp patch.** SendingTime (tag 52) is rewritten
  over a fixed-width placeholder and the checksum is fixed by byte-delta mod 256 — not recomputed over
  the frame. `services/bot-fleet/src/fix.rs:57` (`patch_timestamp`), delta math `:84`. MsgSeqNum is
  decoupled from the per-task seq (logon=1, first order=2) so a strict FIX engine doesn't reject the
  first order: `services/bot-fleet/src/fix.rs:180`.
- **Producer split matched to data criticality.** Control plane (`bot.ready`) uses `acks=all` +
  idempotence so fan-in can't silently stall; telemetry (`orders.sent`) uses `acks=1` + lz4 + linger
  for volume, where a rare drop is just an HDR-histogram gap. `services/bot-fleet/src/kafka.rs:123`/`:150`.
- **MessagePack named encoding + size-bounded flush.** `orders.sent` is encoded with
  `rmp_serde::to_vec_named` (schema-evolution-safe) and flushed in <=1000-event chunks under
  `max.message.bytes`. `services/bot-fleet/src/telemetry.rs:174`, `:167`.
- **Scenario = a schedule of constant-rate tasks.** Spike and ramp shapes emerge purely from when
  constant-rate `TaskSpec`s start and stop, keeping the bot loop trivial; the controller sizes the
  fleet `ceil(tasks/1000)` and shards round-robin by `task_id` so each pod gets a representative mix.
  `services/bot-fleet-controller/internal/controller/runner.go:317`/`:449`. Sessions run serially,
  one per scenario in a run-group: `consumer.go:79`.

### Correctness: a reference exchange as the oracle (`services/correctness-validator`)

- **The platform runs its own canonical exchange and scores the diff.** A price-time-priority CLOB
  (two btrees: asks ascending, bids descending, so `Min()` is the best price; FIFO per level; O(1)
  order index) replays the delivered request stream and produces the fills a correct engine *would*
  have. `services/correctness-validator/internal/book/book.go:65` (`Engine`).
- **The maker/taker attribution problem, sidestepped.** Because the reference engine knows every order
  from `orders.sent`, it attributes each trade to **both** sides itself and never needs the contestant
  to name the counterparty on the wire. `services/correctness-validator/internal/book/book.go:166`.
- **Six violation classes, all decidable from the oracle:** price priority, time priority (FIFO arrival
  rank), overfill, phantom fill, self-trade (participant inferred from `order_id`, not the uniform FIX
  SenderCompID), and cancel-replace priority loss (split from a plain time break). `internal/validate/validate.go`
  — `:240` (`queueJump`), `:219` (`selfTrade`), `:144` (overfill), `:208` (`flagJump`).
- **TCP head-of-line-aware replay.** Orders are replayed in the order TCP would actually have
  delivered them: `effective_t3 = max(t3, prev_in_flow)` per flow in tcp_seq order, then a stable
  global sort by `(effective_t3, flow, tcp_seq)` — so a network-reordered segment is judged the way a
  correct engine would have seen it. `services/correctness-validator/internal/replay/order.go:37`/`:51`.
- **A 100ns cross-flow tie tolerance applied only at judgment.** Two orders on *different* flows whose
  delivery times differ by less than 100ns were genuinely free to resolve either way, so an ordering
  "violation" between them is suppressed — while ordering within a single flow stays strict.
  `services/correctness-validator/internal/replay/order.go:68` (`CrossFlowTie`), used at `validate.go:260`.
- **A contestant cannot steer the reference engine.** Cancel/replace targets resolve from the
  bot-authoritative `orders.sent.orig_order_id`, never the contestant's echoed FIX tag 41.
  `services/correctness-validator/internal/pipeline/pipeline.go:85` (`preferOrig`).
- **Bounded post-run drain via the UUIDv7 timestamp.** Instead of re-scanning a 24h topic (a real OOM
  we hit), it derives session start from the `session_id`'s embedded ms timestamp minus a margin and
  seeks each partition by time, reading only `[start, watermark)`; non-v7 ids fail safe to earliest.
  `services/correctness-validator/internal/source/drain.go:171`/`:154`.
- **Deterministic and idempotent.** The pipeline is a pure function over a total ordering, so a rerun
  is byte-identical; exactly one worker publishes `scores.correctness` via an atomic
  `INSERT ... ON CONFLICT DO NOTHING` claim, with summary + (millions of) violation rows committed in
  one `CopyFrom` transaction. `services/correctness-validator/internal/store/postgres.go:95`, `main.go:235`.
- **Cross-service price-scale reconciliation.** `orders.sent.price` is a raw int and
  `orders.acked.fill_price` is scaled by 1e9; the reference order is lifted into the scaled domain, or
  every fill would read as a price violation. `services/correctness-validator/internal/pipeline/pipeline.go:52`.

### Telemetry: accurate percentiles, decoupled from the control plane (`services/telemetry-ingester`)

- **HDR histograms.** p50/p90/p99/p99.9 accurate to 0.1% in fixed memory regardless of sample count,
  mergeable across workers, raw histogram preserved for offline re-aggregation.
  `services/telemetry-ingester/src/aggregate.rs:89` (`new_with_bounds(1, 60s, 3 sig figs)`).
- **Waves are a pure time bucket — no scenario lookup.** `wave_index = floor((t - session_start)/wave)`
  is computed from timestamps alone, so the telemetry path never touches the scenarios table or
  `TaskSpec`, fully decoupling measurement from the control plane.
  `services/telemetry-ingester/src/aggregate.rs:130`.
- **Late-fill safety.** The dedup idle clock is fed each response's arrival `t7`, not the fixed request
  `t3`, so a resting limit order whose fills stream in over seconds is never re-scored as a fresh
  first response (which would corrupt p99 and tps). `services/telemetry-ingester/src/aggregate.rs:187`,
  bounded 5s-eviction tracker `join.rs:19`.
- **Self-provisioning TimescaleDB + DB-free hot reads.** On startup it creates the `metrics`
  hypertable and a `metrics_10s` continuous aggregate (best-effort, still runs on plain Postgres), and
  writes hot p99 to Redis under `contestant:{id}:{session}:{wave}` so the leaderboard reads without a
  DB round-trip and two waves can't overwrite each other on a boundary second.
  `services/telemetry-ingester/src/store.rs:74`/`:32`, `redis_sink.rs:55`.
- **Honest rows only.** A row is emitted only when a scored sample exists and the contestant is known,
  so an all-timeout wave never writes a 0-latency unattributable row that would drag down the
  aggregate p99. `services/telemetry-ingester/src/aggregate.rs:229`.

### Supply chain and control plane (`build-worker`, `submission-api`, `schemas`, `k8s/data`)

- **Kaniko builds — no Docker socket.** Per-submission K8s Jobs build the image in userspace,
  eliminating the docker-socket-mount escape vector; Trivy (CVE scan) and Syft (SBOM) run as parallel
  Jobs and only both-succeed advances the submission. `services/build-worker/internal/k8s/spawner.go:357`,
  `:187`.
- **PostgreSQL status as the trust anchor (no image signing).** Only `build-worker` holds Harbor push
  creds and sets `status=ready` after the scan/promote gate; `submission-api` refuses to start a run
  unless `status=='ready'`. This replaces Cosign/Kyverno, which add value only with multiple
  independent pushers — not our model. `services/build-worker/internal/k8s/spawner.go:261`,
  `services/submission-api/internal/handler/benchmark.go:112`.
- **Idempotent run-groups.** A partial unique index `run_groups(submission_id) WHERE status NOT IN
  (completed,failed)` means re-clicking "benchmark" returns the existing group (HTTP 200), never a
  duplicate run. `services/submission-api/internal/store/postgres.go:105`, handler `benchmark.go:133`.
- **At-least-once, done right.** Consumers commit the Kafka offset only after durable work, so a crash
  mid-pipeline replays rather than drops; `createJob` adopts an already-existing Job on redelivery
  instead of force-failing a successful build; terminal status writes run on a detached
  `context.Background()` so SIGTERM can't strand a row non-terminal.
  `services/build-worker/internal/consumer/kafka.go:64`, `spawner.go:288`, `:576`.
- **Single-replica serial controller with crash recovery.** Locked to one replica with `Recreate`; on
  restart it re-fails in-flight runs and releases the parent run-group so a crash can't permanently
  hold the unique index. `k8s/benchmark/bot-fleet-controller/deployment.yaml:7`,
  `services/bot-fleet-controller/internal/controller/recovery.go:36`.
- **One contract, two languages, no protobuf.** Message schemas are hand-mirrored Go and Rust structs
  — JSON for the control plane, MessagePack named for the high-volume `orders.*` path; that is the
  entire cross-service contract. `schemas/go/topics/topics.go`, `schemas/rust/src/lib.rs`.
- **Kafka KRaft, no ZooKeeper.** Three combined broker+controller nodes, `podManagementPolicy: Parallel`
  to bootstrap quorum, RF=3, `min.insync.replicas=2`, `max.message.bytes=1MiB`, auto-create off.
  `k8s/data/kafka/statefulset.yaml:14`/`:46`.
- **UUIDv7 ids everywhere.** `session_id` / `run_group_id` / submission ids embed a millisecond
  timestamp, so they are globally unique and k-sortable by creation time without a separate sequence —
  and the validator reuses that embedded timestamp to bound its drain.
  `services/submission-api/internal/handler/benchmark.go:351`.

### Observability

Every service exposes `/metrics` on `:9090` (RED middleware) and ships structured logs to Loki when
`LOKI_URL` is set; the full Prometheus + Grafana + Loki stack is deployed. The platform that
benchmarks others is itself fully observable. `libs/go/metrics/metrics.go:137`,
`libs/go/logger/loki.go:66`, `k8s/observability/`.

---

## Repo layout

```
services/        one folder per service (own go.mod / Cargo.toml)
schemas/         Kafka message contracts — go/ and rust/, hand-mirrored
libs/            shared infra — go/ (logger, metrics, health) and rust/
k8s/             one folder per namespace; `kubectl apply -f k8s/<ns>/` deploys a tier
bootstrap/       secret templates + the bootstrap flow (real secrets never live in k8s/)
mermaids/        architecture diagrams (the two referenced at the top)
testing/         end-to-end and integration test harness
```


---

## Running locally

```sh
# 1. Backing services (Kafka, Postgres, TimescaleDB, Redis, MinIO)
docker compose up -d

# 2. Environment
cp .env.example .env        # edit if needed

# 3. A Go service
cd services/submission-api && go run ./cmd/...

# 4. A Rust service
cd services/bot-fleet && cargo run

# 5. Submit a test bundle: a .zip with submission.yaml or benchmark.yaml at the root
```

