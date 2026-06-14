## The thesis & system overview

### The one idea: you cannot trust the thing you are measuring

A latency benchmark for trading code has a fatal temptation built in: if you ask
the contestant's program *"how fast were you?"*, a contestant who wants to win
simply lies — or, more subtly, measures from a point that flatters them
(after the syscall returns, before the response is serialized, using a clock they
control). Any number the contestant's process produces is, by construction,
**gameable**.

match-bench is built around removing the contestant from the measurement loop
entirely:

1. **Determinism in →** every contestant receives the *same* logical order stream
   for a given seed and workload mix, paced **open-loop** (the generator does not
   wait for the contestant to keep up, so a slow contestant cannot slow the
   clock — it just misses its deadlines, which is the thing we want to measure).
2. **Kernel measurement ⟷** request and response timestamps are taken by an
   **eBPF/XDP program in the kernel**, at the network boundary of the contestant's
   pod, using kernel time. The contestant's code never touches the clock, the
   counter, or the packets being timed.
3. **Reference-model scoring out →** every fill the contestant emits is replayed
   through a correct price-time-priority order book. A score is only accepted if
   the contestant's behavior matches what a correct engine would have done.

Everything else in this document — the split-producer Kafka design, the
co-partitioned telemetry streams, the HDR histograms, the hardened pods, the
distributed ingester — exists to make those three properties hold **at scale and
under load** without ever letting the measurement instrument perturb the thing it
measures.

### The shape of the system

The platform is a **multi-tenant, asynchronous, Kafka-triggered** pipeline of
small services written in **Go** (control plane / APIs) and **Rust** (the
performance-critical data plane: load generation, kernel capture, telemetry
aggregation). Services never call each other synchronously for the hot path —
they communicate through a small set of **Kafka topics with an explicitly shared
schema**, which is what lets each stage scale independently.

Work is physically separated into three **planes** that never share machines:

- **Measurement / control plane** — APIs, controllers, Kafka, databases,
  telemetry, scoring, observability. The brains and the bus.
- **Sandbox plane** — the untrusted contestant pod and its dedicated, privileged
  eBPF capture pod. One contestant per node, hardened and network-isolated.
- **Load-generation plane** — the bot-fleet workers that fire the deterministic
  order stream. Scaled out node-by-node; tainted so nothing else lands there.

Keeping these on separate, tainted Kubernetes node pools is not an operational
nicety — it is part of the fairness guarantee. The measurement instrument must
never steal CPU from the cores it is measuring, and the load generator must never
contend with the contestant for the NIC.

### Languages & responsibilities at a glance

| Layer | Language | Services |
|-------|----------|----------|
| HTTP / control plane | Go | submission-api, build-worker, sandbox-orchestrator, bot-fleet-controller, correctness-validator, score-computer, leaderboard-api, auth-api |
| Data plane (hot path) | Rust | bot-fleet (load gen), ebpf-latency (kernel capture), telemetry-ingester (HDR aggregation) |
| Web | TypeScript / Next.js | frontend |
| Shared contracts | Go + Rust (mirrored) | schemas/ (Kafka topics + event structs), libs/ (auth, logging, metrics) |

### Repository layout (the parts that matter)

```text
services/          the 12 microservices (Go + Rust)
schemas/           the shared Kafka topic + event contract, mirrored in Go and Rust
libs/              shared Go/Rust libs: JWKS auth, Loki logging, Prometheus metrics
ops/kafka/         create-topics.sh — the authoritative topic/partition declaration
k8s/               Kubernetes manifests, grouped by plane (data/platform/sandbox/build/benchmark/observability)
infra/terraform/   the EKS cluster topology (node pools, taints, instance types)
e2e/               full end-to-end suite (every service on) + the reference contestant
bench/             the ~2M orders/s platform self-benchmark tier
deploy-bench/      the load-generator capacity sweep (drain sink, 1→2 nodes)
deploy-local/      local k3s bring-up + HDR plots
```

---
