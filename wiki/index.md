# match-bench — Platform Architecture

> Fair, repeatable, **un-gameable** benchmarking for high-frequency-trading algorithms.
>
> A contestant uploads a trading algorithm; the platform builds it, runs it in a
> locked-down pod, fires the *same* deterministic market workload at every
> contestant, measures latency **in the Linux kernel — outside the contestant's
> process**, replays every fill through a reference order book to score
> correctness, and publishes a live leaderboard.

This document is the **single source of truth** for how the platform is built. It
was written by reading the code directly, and every non-obvious claim is cited to
a `path:line`.

---

## How to read this document

The document is organized as a set of self-contained pages. Each can be read on
its own, but the recommended order for a newcomer is top to bottom.

| # | Page | Read this to understand… |
|---|------|--------------------------|
| 1 | [The thesis & system overview](the-thesis-system-overview.md#the-thesis-system-overview) | *Why* the platform exists and the one idea everything serves |
| 2 | [Map of the system](map-of-the-system.md#map-of-the-system) | The components, planes, and how data moves between them |
| 3 | [Kafka topology, partitioning & horizontal scaling](kafka-topology-partitioning-horizontal-scaling.md#kafka-topology-partitioning-horizontal-scaling) | How services line up on the bus and how the platform scales out |
| 4 | [The benchmark run lifecycle](the-benchmark-run-lifecycle.md#the-benchmark-run-lifecycle) | The end-to-end sequence of one run, click → score |
| 5 | **Service reference** (one section each) | The internals of every microservice |
| 6 | [Benchmarking, bottleneck hunt & scope for improvement](benchmarking-bottleneck-hunt-scope-for-improvement.md#benchmarking-bottleneck-hunt-scope-for-improvement) | The numbers we measured, how we found and killed each bottleneck, and what's left |
| 7 | [Deployment, isolation & observability](deployment-isolation-observability.md#deployment-isolation-observability) | How it's deployed on Kubernetes/EKS and how fairness is enforced |
| 8 | [Scope for improvement (consolidated)](scope-for-improvement.md#scope-for-improvement) | Every open gap across the platform, ordered by impact |
| 9 | [Appendix: glossary](appendix-a-glossary.md#appendix-a-glossary) | Terminology used throughout this document |

**Service reference index**

- [Submission API](submission-api-go.md#submission-api-go) — the HTTP front door
- [Build Pipeline & Sandbox Orchestrator](build-pipeline-sandbox-orchestrator-go.md#build-pipeline-sandbox-orchestrator-go) — turn a zip into a running, isolated pod
- [Bot-Fleet Controller](bot-fleet-controller-go.md#bot-fleet-controller-go) — the orchestration brain of a run
- [Bot-Fleet Load Generator](bot-fleet-load-generator-rust.md#bot-fleet-load-generator-rust) — the open-loop order cannon
- [eBPF Latency Capture](ebpf-latency-capture-rust.md#ebpf-latency-capture-rust) — kernel-stamped, un-gameable timing
- [Telemetry Ingester & Rollup](telemetry-ingester-rollup-rust.md#telemetry-ingester-rollup-rust) — sent/acked → HDR histograms
- [Correctness Validator](correctness-validator-go.md#correctness-validator-go) — the reference order-book oracle
- [Score Computer & Leaderboard API](score-computer-leaderboard-api-go.md#score-computer-leaderboard-api-go) — ranking & live feed
- [Platform Foundations](platform-foundations-auth-shared-libraries-schemas.md#platform-foundations-auth-shared-libraries-schemas) — auth, shared libs, the schema contract

---
