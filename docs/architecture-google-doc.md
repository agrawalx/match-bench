# match-bench Architecture Document

## 1. Executive Summary

match-bench is a production-style benchmarking platform for high-frequency-trading algorithms. It accepts participant submissions, builds them into container images, runs each algorithm inside a restricted Kubernetes sandbox, sends deterministic market traffic, measures latency outside the participant process with Linux eBPF, validates trading correctness against a reference matching engine, computes a gated score, and displays live results on a web leaderboard.

The central architectural goal is fairness. A participant should not be able to improve their score by manipulating timestamps, changing protocol framing, avoiding certain system calls, slowing down the workload, or exploiting shared infrastructure. The platform therefore treats measurement, workload generation, sandboxing, correctness, and scoring as separate control points.

At a high level, the system is built around five decisions:

1. Use Kafka as the durable contract between services.
2. Measure service time at the algorithm pod network boundary, not inside participant code.
3. Generate deterministic open-loop traffic so every participant receives the same workload.
4. Run untrusted algorithms in locked-down Kubernetes pods.
5. Score only after correctness, latency, error-rate, and telemetry-coverage gates pass.

## 2. Problem Being Solved

The project benchmarks trading algorithms that may be written by untrusted participants. A useful benchmark must answer two questions:

- How fast is the algorithm under the same workload as everyone else?
- Did the algorithm behave like a correct exchange-facing matching engine?

Traditional application-level timing is not enough for this problem. If the participant process reports its own timings, it can lie, stamp early, batch responses in misleading ways, or use an I/O path that bypasses the measurement logic. Similarly, if the workload is closed-loop, a slow algorithm can accidentally receive less load because the next request waits for a previous response.

match-bench solves this by moving trust away from the participant:

- The platform observes network traffic from outside the algorithm.
- The workload is generated independently of the algorithm's response speed.
- Correctness is checked by replaying observed behavior through a reference order book.
- The leaderboard accepts a score only when the run has enough telemetry and meets quality gates.

## 3. System Components

### Frontend

The frontend is a Next.js application. It provides submission, leaderboard, live run, and run-detail views. It consumes platform APIs and uses server-sent events for live leaderboard updates.

### Platform APIs

The platform tier contains:

- `auth-api`: handles authentication and Google OAuth-related flows.
- `submission-api`: accepts uploads, stores submission metadata, publishes build and benchmark requests, and tracks run status.
- `leaderboard-api`: serves leaderboard data, run details, chart data, health information, and SSE updates.

### Build System

`build-worker` is responsible for converting submitted source bundles into runnable container images. It uses metadata from the submission API, stores artifacts in MinIO/S3-compatible storage, and updates submission status through Kafka events.

### Sandbox Orchestrator

`sandbox-orchestrator` owns the lifecycle of untrusted algorithm pods. For each benchmark slot it creates:

- An algorithm pod.
- A Kubernetes service pointing to the algorithm.
- An optional eBPF capture job colocated on the same node.

The orchestrator also handles cleanup, slot state recovery, and orphan capture-job reaping.

### Bot Fleet Controller

`bot-fleet-controller` coordinates benchmark runs. It consumes benchmark requests, provisions sandbox slots, shards workload tasks across bot workers, waits for worker readiness, emits the barrier event, and marks benchmark completion or failure.

### Bot Fleet Workers

`bot-fleet` workers are Rust/Tokio load generators. They receive workload assignments, connect to the algorithm, publish readiness, wait for the barrier, then fire FIX, REST, or WebSocket traffic according to deterministic task specs. They publish `orders.sent` telemetry with intended send time, actual send time, response completion time, order attributes, timeout state, and barrier epoch.

### eBPF Latency Capture

`ebpf-latency` captures packets at the algorithm pod's network boundary. It timestamps request ingress and response egress, reconstructs TCP streams in userspace, parses FIX/REST/WS messages, matches responses to requests by `ClOrdID`, and publishes `orders.acked`.

### Telemetry Ingester

`telemetry-ingester` joins `orders.sent` and `orders.acked` into per-session, per-contestant, per-wave metrics. It writes HDR histogram snapshots and time-series metrics to TimescaleDB and writes hot p99 snapshots to Redis.

### Correctness Validator

`correctness-validator` replays the captured order stream through a reference price-time-priority central limit order book. It detects invalid fills, phantom fills, overfills, price violations, time-priority violations, self trades, and cancel/replace priority loss. It publishes a correctness score event.

### Score Computer

`score-computer` consumes correctness results and reads telemetry metrics. It computes peak sustained throughput using p99 latency, error-rate, correctness, and telemetry-coverage thresholds. It writes final score rows and leaderboard update events.

### Data and Observability

The data tier uses:

- Kafka for asynchronous service contracts.
- PostgreSQL for metadata, submissions, run groups, and scores.
- TimescaleDB for metrics and HDR histogram snapshots.
- Redis for hot leaderboard and p99 snapshots.
- MinIO/S3 for uploaded artifacts.

The observability tier uses Prometheus, Grafana, and Loki.

## 4. End-to-End Run Flow

1. The user uploads a submission.
2. `submission-api` stores metadata and publishes a build request.
3. `build-worker` builds the container image and marks the submission ready.
4. The user starts a benchmark.
5. `submission-api` publishes `benchmark.requested`.
6. `bot-fleet-controller` asks `sandbox-orchestrator` for a slot.
7. `sandbox-orchestrator` creates the algorithm pod and service.
8. If capture is enabled, the orchestrator creates the eBPF capture job on the same node.
9. The controller publishes `workload.assignments`.
10. Bot workers connect to the algorithm and publish `bot.ready`.
11. The controller waits for readiness, computes a fresh barrier time, and publishes `barrier`.
12. Workers begin sending deterministic traffic at the barrier.
13. Workers publish `orders.sent`.
14. eBPF capture publishes `orders.acked`.
15. `telemetry-ingester` joins the streams and writes live metrics.
16. The controller tears down the sandbox slot and publishes benchmark completion.
17. `correctness-validator` validates the session and publishes `scores.correctness`.
18. `score-computer` computes final scores.
19. `leaderboard-api` serves updated leaderboard and chart data to the frontend.

## 5. Architectural Decisions

### 5.1 Kafka as the Backbone

Kafka is used as the boundary between major services. This was chosen because benchmark runs have several independent stages: build, sandbox allocation, load dispatch, telemetry, validation, scoring, and UI updates. Kafka gives these stages a durable handoff point.

Important topics include:

- `submission.build.requested`
- `submission.status.updated`
- `benchmark.requested`
- `benchmark.status.updated`
- `workload.assignments`
- `bot.ready`
- `barrier`
- `orders.sent`
- `orders.acked`
- `scores.correctness`
- `leaderboard.updates`

This design has three advantages:

- Services can be restarted without losing the logical run state.
- Consumers can be scaled independently when they are stateless or partitionable.
- The benchmark pipeline is auditable because the main facts of a run are represented as events.

The tradeoff is operational complexity. Kafka requires careful topic configuration, partitioning, consumer groups, max poll intervals, and at-least-once deduplication. The code addresses this with explicit schemas, typed topic constants, bounded batches, idempotent claims in consumers where needed, and validation around long-running workload assignments.

### 5.2 eBPF Boundary Measurement

The platform measures algorithm service time using two timestamps:

- `t3`: request enters the algorithm pod network boundary.
- `t7`: response leaves the algorithm pod network boundary.

The scored metric is:

```text
pod_service_time_ns = t7 - t3
```

This is the most important design decision in the system. The timestamp is taken outside the participant process, so the participant cannot directly manipulate the scored clock. It is also independent of whether the participant uses blocking sockets, nonblocking sockets, `recvmsg`, `recvmmsg`, or io_uring because the measurement point is the packet boundary rather than the syscall boundary.

The capture path disables network offloads and clamps MTU to make packet capture reliable. Userspace then performs TCP reassembly, protocol parsing, and per-order matching.

The tradeoff is that this measurement path is more complex than application-level timing. It requires privileged capture jobs, kernel compatibility, eBPF verifier-safe code, careful packet parsing, and Kubernetes placement logic. The benefit is a much stronger fairness story.

### 5.3 Deterministic Open-Loop Load

The bot fleet uses deterministic task specs and seeded generation so each contestant receives the same logical workload. The generator is open-loop: it sends at the planned schedule instead of waiting for the previous order to finish before sending the next one.

This avoids coordinated omission. If an algorithm becomes slow, the offered load continues according to the schedule, and the latency distribution reflects the slowdown instead of hiding it.

Workers also publish both intended send time and actual send time. This allows the telemetry layer to separate:

- Algorithm service time from eBPF (`t7 - t3`).
- End-to-end response time (`r9 - t0`).
- Schedule slip (`t1 - t0`).

This separation is useful because a leaderboard should score algorithm service time, while operators still need diagnostics for load-generator delay and end-to-end user-visible delay.

### 5.4 Kubernetes Sandbox Isolation

Each algorithm runs as an isolated Kubernetes pod. The sandbox is configured to reduce shared-resource interference and limit the blast radius of untrusted code.

Key controls include:

- Equal CPU and memory requests/limits for Guaranteed QoS.
- Integer CPU validation so static CPU-manager pinning can work.
- Optional node-pool pinning.
- Optional gVisor via `runtimeClassName`.
- Read-only root filesystem.
- Memory-backed writable directories for `/tmp`, `/var/tmp`, `/var/log`, and `/var/run`.
- Dropped Linux capabilities.
- `AllowPrivilegeEscalation=false`.
- RuntimeDefault seccomp.
- No mounted service-account token.
- Default-deny network policy.
- Capturable-port validation so the platform does not run benchmarks that cannot be measured.

The design intentionally separates the untrusted algorithm pod from the privileged capture job. The algorithm pod is hardened; the capture job is privileged only because eBPF attach and namespace inspection require it.

### 5.5 Telemetry Model

Telemetry is split into durable metrics and hot read models.

TimescaleDB stores time-series snapshots and HDR histogram blobs. Redis stores hot p99 and leaderboard-oriented snapshots. This split keeps historical analysis available without forcing every UI refresh to query the heavier store.

The telemetry ingester maintains per-wave windows. Waves are time buckets derived from the barrier/session start, not from controller state. This keeps aggregation stable even if control-plane messages arrive late.

The ingester records multiple histograms:

- Service-time histogram from eBPF measurement.
- Response-time histogram from bot-observed response completion.
- Schedule-slip histogram from intended versus actual send timing.

That separation makes the final score explainable. A participant can see whether failure came from algorithm latency, load generator slip, timeouts, rejects, missing telemetry, or correctness.

### 5.6 Correctness Before Score

The correctness validator prevents a fast but invalid algorithm from ranking highly. It reconstructs the session and replays orders through a reference order book.

The validator checks:

- Fills for orders that were never sent.
- Overfills beyond order quantity.
- Fill prices not produced by the reference engine.
- Time-priority violations.
- Self trades.
- Cancel/replace priority loss.

The score computer then applies correctness thresholds before ranking. This makes the leaderboard a benchmark for correct high-throughput trading behavior, not merely low latency.

### 5.7 Scoring Model

The score is based on peak sustained throughput, not one lucky spike. The score computer finds the highest offered wave that passes the gates:

- Correctness threshold, default 95%.
- Minimum telemetry coverage, default 90%.
- Maximum error rate, default 1%.
- Maximum stable p99, default 1 millisecond.
- Warmup wave skipped.

Tie-breaking favors higher throughput, then lower p99 at peak, then faster spike recovery, then higher correctness.

This makes the result meaningful because it rewards stable performance under load, not just isolated low-latency responses.

## 6. Data Contracts

The main runtime contracts are:

- `BenchmarkRequested`: identifies submission, run group, scenario, contestant, and session.
- `WorkloadSpec`: tells a worker which tasks to run, the target host and port, protocol, seed, and worker shard information.
- `ReadySignal`: tells the controller a worker connected and is waiting for the barrier.
- `BarrierEvent`: gives workers a shared epoch to begin sending.
- `OrderSentBatch`: records intended send time, actual send time, response done time, order attributes, and timeout status.
- `OrderAckedBatch`: records eBPF-derived request/response timestamps, order ID, execution type, fill quantity, fill price, retransmission metadata, and reorder metadata.
- `CorrectnessScoreEvent`: reports validation counts and correctness score.
- `LeaderboardUpdateEvent`: reports computed ranking fields.

The important design choice is that `orders.sent` and `orders.acked` are separate facts. The bot fleet knows what it attempted to send and when it observed completion. The eBPF capture knows what crossed the algorithm boundary and how long the algorithm took. Joining those two streams gives both score-quality and diagnostic-quality data.

## 7. Deployment Architecture

The Kubernetes layout is separated by namespace:

- `platform`: user-facing APIs and frontend.
- `build`: build spawner and build jobs.
- `sandbox`: untrusted algorithm pods and capture jobs.
- `benchmark`: controller, bot fleet, telemetry, validation, scoring.
- `data`: Kafka, PostgreSQL, TimescaleDB, Redis, MinIO.
- `observability`: Prometheus, Grafana, Loki.

This namespace split supports least-privilege RBAC, clearer network policy, and simpler operations. It also makes the architecture easier to reason about: untrusted code is isolated in `sandbox`, durable services live in `data`, and benchmark workers live in `benchmark`.

The project supports both local development and AWS EKS deployment. Local development uses Docker Compose for data and observability dependencies. The AWS path uses Terraform, EKS, ECR, IRSA, KEDA, gp3 storage, and Kubernetes manifests.

## 8. Reliability and Failure Handling

The system includes several reliability mechanisms:

- Slot lifecycle is explicitly modeled as requested, creating, ready, failed, and terminating states.
- The sandbox orchestrator can list existing pods and reconcile slot state.
- Capture jobs are owned by algorithm pods and have TTL cleanup.
- Orphan capture jobs are reaped when no live slot exists.
- Long bot assignments validate max poll interval risk before running.
- Telemetry batches are bounded.
- Capture flushes buffered events on shutdown and retains unsent events on transient publish failure.
- Score computation treats missing metrics and low coverage as score-impacting rather than silently accepting incomplete runs.

The most important reliability principle is that partial or uncertain data should not be silently converted into a good score.

## 9. Security Model

Security is layered rather than dependent on one control:

- Submitted code runs in a restricted pod.
- The pod has no service-account token.
- Capabilities are dropped.
- The root filesystem is read-only.
- Writable paths are memory-backed.
- Default-deny network policy restricts lateral access.
- Optional gVisor can add a stronger runtime boundary.
- Build and sandbox responsibilities are separated from platform APIs.
- Secrets are provided through Kubernetes secrets rather than committed config.

There is one intentional privileged component: the capture job. It needs privileges to attach eBPF programs and inspect the algorithm network namespace. Its scope is narrowed by running it per slot, colocating it with the target pod, and cleaning it up with the slot lifecycle.

## 10. Why the Design Is Not Vague

The system has explicit answers to the main fairness questions:

- **Who controls the timestamp?** The kernel capture path, outside participant code.
- **Can the participant slow down future load?** No. The bot fleet uses open-loop scheduling.
- **Does everyone get the same workload?** Yes. Workload generation is seeded and task-spec driven.
- **Can an invalid engine win by being fast?** No. Correctness gates scoring.
- **Can missing telemetry become a good score?** No. Score computation has coverage gates.
- **Can untrusted code reach internal services?** The sandbox design and network policies are intended to prevent that.
- **Can protocol framing tricks evade measurement?** The parser handles FIX framing, HTTP chunking, WebSocket messages, and per-order matching by `ClOrdID`.

## 11. What I Would Add With More Time

### Production Scheduler

I would build a first-class benchmark scheduler with queueing, priorities, concurrency limits per team, node-affinity awareness, and admission control. The current design has strong per-run orchestration, but a production competition needs a global scheduler to decide which runs should start, pause, retry, or wait.

### Stronger Supply-Chain Guarantees

I would add image signing, SBOM retention, vulnerability policy gates, provenance attestations, and immutable promotion from build to benchmark. The build flow already separates submitted artifacts from runnable images, but production trust would improve with formal attestations.

### Automated Calibration

I would add a calibration suite that runs known reference algorithms before each event. It would measure baseline p99, jitter, Kafka lag, eBPF drop rate, node-level noise, and capture correctness on the exact cluster being used. This would make it easier to prove the environment is healthy before participant runs.

### Better Replay and Forensics

I would add downloadable per-run forensic bundles: workload seed, task specs, sent/acked samples, correctness violations, HDR histograms, capture counters, pod spec, image digest, and relevant logs. This would help participants understand failures and help organizers resolve disputes.

### Schema Evolution Tooling

I would formalize schema compatibility checks between Go and Rust. The current schema mirrors are practical, but a production system should enforce compatibility with generated schemas, versioned events, and migration tests.

### Multi-Region and Disaster Recovery

For a large event, I would add backup/restore procedures for Kafka, PostgreSQL, TimescaleDB, Redis snapshots, and artifact storage. I would also document RPO/RTO targets and test cluster recovery.

### Deeper Security Hardening

I would add policy-as-code checks for Kubernetes manifests, runtime admission policies, seccomp profiles tailored to supported languages, egress DNS restrictions, and stronger separation between capture privileges and application workloads.

### More Workload Types

I would extend the workload model beyond constant, ramp, and spike scenarios. Useful additions include auction opens, news bursts, crossed-market recovery, heavy cancel/replace storms, multi-symbol correlation, and adversarial sequencing.

### Participant-Facing Diagnostics

I would improve the UI with clearer explanations of why a score failed: correctness failure, p99 gate, error-rate gate, telemetry coverage, timeout rate, or load-generator schedule slip. This would turn the leaderboard from a score table into a useful performance lab.

## 12. Conclusion

match-bench is architected around a simple fairness principle: a participant should only improve their score by building a faster and more correct algorithm. The implementation supports that principle through boundary-based eBPF measurement, deterministic open-loop load, isolated Kubernetes sandboxes, event-driven service contracts, reference-model correctness validation, and gated scoring.

The result is not just a leaderboard application. It is a measurement system with explicit trust boundaries and enough telemetry to explain the final score.
