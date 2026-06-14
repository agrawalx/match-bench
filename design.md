# Match-Bench — Design & Engineering

Match-Bench is a high-frequency-trading algorithm benchmarking platform. A contestant submits a matching engine (FIX/REST/WS). The platform builds it (Kaniko → registry), sandboxes it (one algo pod per node, pinned cpuset, gVisor-optional), fires deterministic barrier-synced load from a Rust bot fleet, measures latency **in the kernel** with eBPF at the algo veth, joins telemetry into HDR histograms, validates correctness against a reference price-time-priority CLOB, scores peak-sustained-TPS behind latency and correctness gates, and serves a live Next.js leaderboard with Gil-Tene latency-by-percentile charts.

This document is self-contained. It explains the implemented architecture, the engineering decisions behind it, and the invariants that make the benchmark fair and repeatable.

---

## 0. Architecture summary for reviewers

### What was implemented

Match-Bench is implemented as an event-driven benchmark platform for untrusted high-frequency-trading algorithms. A participant uploads a submission, the platform builds it into a container image, schedules it into a restricted Kubernetes sandbox, drives it with deterministic traffic, measures service latency from outside the participant process using eBPF, validates trading correctness against a reference order book, computes a gated score, and streams results to a live leaderboard.

The platform is split into five architectural planes:

- **Platform plane:** `submission-api`, `auth-api`, `leaderboard-api`, and the Next.js frontend handle user identity, uploads, benchmark triggers, read APIs, SSE updates, and run detail views.
- **Build plane:** `build-worker` turns source bundles into runnable images, stores artifacts in MinIO/S3, and publishes submission status changes.
- **Sandbox plane:** `sandbox-orchestrator` creates one isolated algo pod per run, applies hardening and resource controls, and starts a colocated `ebpf-latency` capture job.
- **Benchmark plane:** `bot-fleet-controller`, `bot-fleet`, `telemetry-ingester`, `correctness-validator`, and `score-computer` coordinate load, collect telemetry, validate behavior, and compute scores.
- **Data and observability plane:** Kafka carries asynchronous contracts, PostgreSQL stores metadata and scores, TimescaleDB stores time-series metrics and HDR blobs, Redis stores hot snapshots, MinIO/S3 stores uploaded artifacts, while Prometheus/Grafana/Loki support operations.

### Main architectural decisions

**Decision 1: Kafka as the service boundary -** Services communicate through typed Kafka topics such as `benchmark.requested`, `workload.assignments`, `bot.ready`, `barrier`, `orders.sent`, `orders.acked`, `scores.correctness`, and `leaderboard.updates`. This decouples build, run, measurement, validation, scoring, and UI updates while keeping each stage replayable and independently testable.

**Decision 2: measure outside the contestant -** The scored service-time metric is not reported by the submitted algorithm. It is computed from eBPF timestamps taken at the algo pod network boundary: request ingress (`t3`) and response egress (`t7`). This keeps the measured party outside the measurement mechanism.

**Decision 3: deterministic open-loop workload generation -** The bot fleet uses seeded task specs, fixed pacing, a readiness fan-in, and a barrier event so every contestant receives the same logical workload. The workload is open-loop so a slow algorithm cannot reduce future offered load by responding slowly.

**Decision 4: Kubernetes is the isolation boundary -** The sandbox creates restricted pods with request/limit equality, integer CPU validation for cpuset pinning, read-only root filesystems, tmpfs writable paths, dropped capabilities, no service-account token, optional gVisor, and default-deny networking.

**Decision 5: correctness gates scoring -** A fast algorithm is not allowed to win by violating exchange behavior. The correctness validator replays the captured order stream through a price-time-priority reference CLOB and score-computer gates throughput by correctness, p99 latency, error rate, and telemetry coverage.

### End-to-end run lifecycle

1. A user uploads a submission and requests a benchmark.
2. `submission-api` stores metadata and publishes `benchmark.requested`.
3. `bot-fleet-controller` allocates a sandbox slot through `sandbox-orchestrator`.
4. `sandbox-orchestrator` starts the algo pod, service, and capture job.
5. Bot workers connect, publish `bot.ready`, wait for a common barrier, then fire deterministic traffic.
6. Bot workers publish `orders.sent`; eBPF capture publishes `orders.acked`.
7. `telemetry-ingester` joins sent and acked streams into per-wave HDR histograms and hot metrics.
8. `correctness-validator` replays the session and publishes `scores.correctness`.
9. `score-computer` computes peak sustained TPS and writes leaderboard rows.
10. `leaderboard-api` serves the latest state through REST and SSE to the frontend.

### Current constraints and future roadmap

The implemented system is strong on measurement integrity, deterministic load, and isolated execution. With more months of development, the highest-value additions would be: a production-grade multi-tenant scheduling policy, stronger supply-chain attestation for submitted images, automated benchmark calibration per node type, richer per-run forensic exports, stricter schema evolution tooling, disaster-recovery playbooks for Kafka/Postgres/Timescale, and a formal load-test certification suite that runs before every competition.

---

## 1. System overview and the core thesis

### The thesis: fair, un-gameable measurement

The entire platform is built around one invariant: **the contestant cannot influence the measurement except by actually being faster.** Two design choices make this true and everything else follows from them.

1. **The latency clock is opened and closed by the kernel, on the wire, outside the sandbox.** The scored metric `service_time = t7 − t3` is computed from two timestamps stamped in-kernel at the algo pod's veth: `t3` at XDP ingress (before the network stack) and `t7` at tc egress. The contestant's userspace code never touches the clock. The only way to lower `t7 − t3` is to respond faster on the wire. Stamps come from `bpf_ktime_get_ns()` at `services/ebpf-latency/src/ebpf.rs:262`, called from both `try_xdp_ingress` (`ebpf.rs:204`) and `try_tc_egress` (`ebpf.rs:237`); the subtraction is `matcher.rs:119` `let pod_service_time_ns = t7_ns.saturating_sub(inflight.t3_ns)`.

2. **Every contestant receives the byte-identical workload, regardless of how they respond.** Order content is seeded and open-loop: cancel/replace targets are drawn from the bot's own self-accounted ledger, never from observed fills, so a fast and a slow algo see the same stream (`services/bot-fleet/src/content.rs:1-17`, draw-order contract `content.rs:144-146`).

Everything else — the sandbox isolation, the load-generation determinism, the HDR pipeline, the correctness oracle — exists to keep these two invariants honest end-to-end.

### Components

- **submission-api** (Go) — auth, JWT verification, accepts contestant submissions, triggers builds.
- **builder** (Kaniko) — builds the contestant image into a registry/ECR.
- **controller / bot-fleet-controller** (Go) — orchestrates a benchmark run: requests a slot, fans in worker readiness, computes the synchronization barrier, drives the wave schedule.
- **sandbox-orchestrator** (Go) — mints one algo Pod + Service per slot, with Guaranteed-QoS cpuset pinning, hardened security context, default-deny networking, and a co-located eBPF capture Job.
- **ebpf-latency** (Rust + aya) — capture-only XDP/tc data plane + userspace TCP reassembly, FIX/REST/WS parse, per-`ClOrdID` match, emits `orders.acked`.
- **bot-fleet** (Rust/Tokio) + **bot-fleet-controller** (Go) — deterministic open-loop load generator with coordinated-omission-correct pacing and three-loop r9 capture across all three protocols.
- **telemetry-ingester** (Rust) — joins `orders.sent` + `orders.acked` into per-(session, wave) HDR histograms → TimescaleDB + Redis.
- **correctness-validator** (Go) — replays the order stream through a reference price-time-priority CLOB and emits violations + a correctness score.
- **score-computer** (Go) — gates peak-sustained-TPS behind latency + correctness + error-rate, writes the scores row.
- **leaderboard-api** (Go) + **frontend** (Next.js 14) — live SSE leaderboard and Gil-Tene latency-by-percentile charts.

### Data plane topology (one run)

```
contestant image ──> algo Pod (pinned cpuset, gVisor-optional, default-deny net)
                          │ veth (offloads off, MTU<=1500)
        ┌─────────────────┴─────────────────┐
   t3 XDP ingress                       t7 tc egress      (in-kernel, capture-only)
        └─────────────> ring buffer ──> userspace reassembly + parse + match
                                              │
bot-fleet ──orders.sent──> Kafka <──orders.acked── ebpf capture
                              │
            ┌─────────────────┼──────────────────┐
   telemetry-ingester   correctness-validator  score-computer
        │                     │                    │
   TimescaleDB+Redis    scores.correctness     scores row ──> leaderboard-api ──> frontend (SSE + HDR chart)
```

### Repo layout

Go workspace (`go.work`) + Rust workspace (`Cargo.toml`) + `frontend/` (Next 14). Services live in `services/*`. Wire schemas are hand-mirrored in Go and Rust under `schemas/`. Kubernetes manifests are in `k8s/<namespace>/`.

---

## 2. Measurement accuracy — deep dive

This is the strongest part of the system, so it leads. The scored latency is measured in the kernel, on the wire, with the contestant's code provably outside the measurement window.

### 2.1 In-kernel timestamps that the contestant cannot touch

**Problem.** Any application-level latency stamp (the algo's own `recv`/`send`, or even a tracepoint on the algo's syscalls) is gameable: a contestant can stamp early, batch, or pick an I/O model that under-reports. Measurement that the measured party can influence is not measurement.

**Decision.** Stamp both ends of the latency window in the kernel at the algo pod's veth: `t3` at **XDP ingress** and `t7` at **tc egress**, both via `bpf_ktime_get_ns()`. The scored metric is the single subtraction `service_time = t7 − t3`.

**Why.** XDP fires at the earliest possible software point on RX, before the kernel network stack, so `t3` is the most faithful "order entered the pod" stamp; pairing it with tc egress for `t7` makes the window a true wire-to-wire pod service time (queue + drain + compute + send), not an application approximation. The window is opened and closed by the kernel outside the sandbox, so it is un-gameable.

**Evidence.**
- Single stamp helper: `ebpf.rs:262` `ptr::addr_of_mut!((*rec).timestamp_ns).write(bpf_ktime_get_ns())`.
- Called from ingress (`ebpf.rs:204`, `DIR_REQUEST`) and egress (`ebpf.rs:237`, `DIR_RESPONSE`).
- Hook definitions: `ebpf.rs:160-165` `#[xdp] pub fn iicpc_xdp_ingress`; `ebpf.rs:167-173` `#[link_section = "classifier"] pub extern "C" fn iicpc_tc_egress`.
- Rationale in source: `ebpf.rs:16-17` "XDP is kept for the ingress timestamp because it fires before the kernel network stack, giving the most faithful t3."
- Subtraction: `matcher.rs:119`.
- **Numbers:** 2 kernel hooks (XDP ingress + tc egress); 1 subtraction.

### 2.2 Skew-invariant by construction

**Problem.** Cross-node clock comparison invites NTP steps and inter-node drift into the primary metric.

**Decision.** Both stamps come from the **same node's CLOCK_MONOTONIC** (helper id 5 = `bpf_ktime_get_ns`), and the subtraction is taken before any monotonic→realtime offset is applied. The monotonic→realtime conversion exists only to align `t3`/`t7` with the bot's `t0`/`t1`/`r9` for diagnostics — and it adds the **same** `clock_offset_ns` to both stamps, so it cancels exactly in the difference.

**Why.** The primary metric is immune to clock skew, NTP steps, and inter-node drift. No PTP/NTP cross-node sync is required for the scored metric.

**Evidence.**
- `ebpf.rs:413-417` comment "Helper id 5 = bpf_ktime_get_ns (CLOCK_MONOTONIC)".
- `pipeline.rs:34-36` `to_realtime` adds the same `clock_offset_ns` to every stamp.
- `pipeline.rs:131-133` "CLOCK_MONOTONIC matches the kernel's bpf_ktime_get_ns domain".
- **Number:** offset added to both `t3` and `t7` → cancels in the difference. (Note: the code uses MONOTONIC + sampled offset, which is stronger than a `bpf_ktime_get_real_ns` approach.)

### 2.3 Syscall-model-agnostic / io_uring-proof

**Problem.** A serious HFT implementation drains the socket with io_uring. A tracepoint-based syscall stamping path emits **no events** for that path and would silently advantage or disadvantage contestants by I/O model.

**Decision.** Stamp at the veth **packet boundary**, not at the syscall boundary.

**Why.** Wire-boundary stamping is identical for `recv`/`recvmsg`/`recvmmsg`/io_uring — zero missed events, zero bias by I/O model.

**Evidence.** `MEASUREMENT_AND_FAIRNESS.md:31` "would produce no events at all for contestants using io_uring … The XDP hooks at the veth boundary are syscall-path-agnostic"; enforced by the capture-only data-plane design (`ebpf.rs:4-17`).

### 2.4 One-packet-per-order: offloads off + MTU clamp

**Problem.** Without intervention the kernel coalesces multiple orders into one GSO super-frame (or a 9001-byte EKS jumbo segment), so a single timestamp would cover many orders, and an oversized capture would truncate and corrupt the stream.

**Decision.** At attach time, inside the algo netns: disable GSO/TSO/GRO/LRO (`ethtool -K`) **and** clamp the veth MTU down to 1500, so a captured segment never exceeds `CAPTURE_CAP = 1536`.

**Why.** Per-packet timestamps then mean per-order timestamps, and the `captured_len == payload_len` invariant holds (expected `TRUNCATED_CAPTURES = 0`).

**Evidence.**
- `disable_offloads` at `main.rs:382` iterates `["tso", "gso", "gro", "lro"]` then `ethtool -K iface feature off`.
- `mtu::clamp_to` at `main.rs:360` clamps to default 1500; both run inside the algo netns before attach (`main.rs:352-363`).
- CAPTURE_CAP sizing rationale `ebpf.rs:48-54`.
- **Numbers:** `CAPTURE_CAP = 1536` bytes; MTU clamped 9001 → 1500 on EKS (`mtu.rs:145`).

### 2.5 The MTU clamp only ever lowers (proven)

**Problem.** Clamping upward could mask a deliberately small MTU and silently change network behavior under measurement.

**Decision.** The clamp computes its target as "downward only" and no-ops when already within range, run unconditionally at every attach.

**Why.** A downward-only invariant is safe to run unconditionally; it never raises a deliberately-small MTU.

**Evidence.** `mtu.rs:74-80` `(current > clamp).then_some(clamp)`; table-driven test `mtu.rs:141-159` `mtu_clamp_target_only_clamps_downward` asserts (9001,1500)→Some(1500), (1400,1500)→None, (9001,0)→None. **Number:** 6 clamp cases asserted.

### 2.6 Userspace TCP reassembly with first-byte attribution

**Problem.** TCP is a byte stream, not a message stream. Naive per-segment stamping misattributes latency when messages coalesce (many per segment) or straddle (one across two segments), and breaks under reordering and retransmission.

**Decision.** Full userspace byte-stream reassembly attributes each message's `t3`/`t7` to the timestamp of the segment carrying its **first byte**, with wrap-safe 32-bit sequence math.

**Why.** First-byte attribution gives each order its true arrival/departure instant even under realistic segmentation.

**Evidence.**
- `reassembly.rs:205-216` `timestamp_at` walks marks, returns the stamp of the segment whose `abs_start ≤ offset`.
- Straddle test `reassembly.rs:300-310` (first byte keeps @100, tail @200); coalesce test `reassembly.rs:286-297`.
- Wrap-safe `seq_le`/`seq_lt`/`seq_ge` at `reassembly.rs:257-269`; overlapping-held-tail drain `reassembly.rs:84-114`.
- **Numbers:** 9 reassembly unit tests; `MAX_BUFFERED` 1 MiB and `MAX_HOLD_SEGMENTS` 64 bound per-flow memory.

### 2.7 Per-response emission + per-ClOrdID matching

**Problem.** HFT responses are multi-message (ack + each partial fill + fill). Dropping after the first response loses fill latencies and under-counts. And pipelined orders complete out of order, so FIFO matching assigns the wrong `t3`.

**Decision.** Emit one event for **every** ExecutionReport, all sharing the request's single `t3` (the matcher keeps the inflight entry via `get_mut`, does not drop on first response). Match per-`ClOrdID`, not FIFO.

**Why.** Sharing one `t3` across responses yields a correct service-time distribution per response with send-time ground truth fixed; `ClOrdID` keying makes per-order latency correct regardless of wire ordering.

**Evidence.**
- `matcher.rs:103-135` `on_response` uses `get_mut` and retains the entry (comment `matcher.rs:6-9`).
- Test `matcher.rs:161-180` `two_responses_per_order_emit_two_events_sharing_t3` (both events `t3_ns == 100`); pipeline test `pipeline.rs:177-197`.
- Per-`ClOrdID` isolation: `matcher.rs:113` `self.inflight.get_mut(clordid)`; test `matcher.rs:182-196` (B `t3=130`, A `t3=100` though B responds first); `pipeline.rs:200-224`.

### 2.8 Verifier-safe, variable-length kernel copy

**Problem.** A BPF program must pass the verifier or it will not load; the verifier needs a provable bound on the copy length. A fixed full-cap copy also wastes ring buffer on small messages, raising drop risk.

**Decision.** Clamp the copy to `CAPTURE_CAP` and make it opaque to the optimizer via a **volatile reload**, so the verifier proves a `1..=CAPTURE_CAP` bound on `bpf_xdp_load_bytes`/`bpf_skb_load_bytes`. Emit only `CAPTURE_HEADER_LEN + captured_len` bytes (variable-length records).

**Why.** A provably-bounded copy is what lets the data plane load at all; variable-length emission keeps ring-buffer pressure low (fewer drops → no measurement loss).

**Evidence.** `ebpf.rs:190-197` volatile reload `let cap = unsafe { ptr::read_volatile(&clamp_cap(bounds.payload_len)) }; if cap == 0 || cap > CAPTURE_CAP { return; }`; `clamp_cap` `ebpf.rs:242-253`; variable-length output `ebpf.rs:259-273` `let total = CAPTURE_HEADER_LEN + cap;`. **Numbers:** `CAPTURE_HEADER_LEN = 28` bytes; ring buffer 64 MiB (`ebpf.rs:140`).

### 2.9 Payload length from the IP header, not the buffer

**Problem.** L2 Ethernet padding on small frames would be mistaken for TCP payload, injecting fake bytes into SYN/ACK/control packets and desyncing reassembly for every subsequent message.

**Decision.** Derive TCP payload length from the IPv4 `total_length` header field.

**Why.** Header-derived length keeps the captured byte stream exactly equal to the real TCP payload.

**Evidence.** Rationale `ebpf.rs:19-22`; `xdp_payload_bounds` `ebpf.rs:299-326` computes `ip_total = u16::from_be(ip.tot_len)` then `payload_len: ip_end - payload_offset`; mirrored in `parse_skb_ip_tcp_at` `ebpf.rs:367-392`.

### 2.10 Truncation re-anchor (the bug that capped delivery)

**Problem.** An oversized GSO super-frame exceeding `CAPTURE_CAP` is captured short. Advancing `next_seq` by the short length opens a permanent forward gap that wedges the whole connection's measurement and silently drops all later orders on that flow. This was the bug that capped `orders.acked` delivery under load.

**Decision.** Detect truncation in userspace and **re-anchor** the flow instead of advancing by the short length.

**Why.** The reset sacrifices the one truncated message but keeps every subsequent order measurable. (The MTU clamp in §2.4 prevents truncation entirely in the first place; this is the belt-and-suspenders.)

**Evidence.** `pipeline.rs:52-62` detects `truncated = cap.payload_len as usize > cap.payload.len()` then `re.reset_for_truncation()`; `reassembly.rs:183-196` `reset_for_truncation`; test `reassembly.rs:363-373` `truncation_reset_resyncs_without_stall`; `TRUNCATED_CAPTURES` kernel counter `ebpf.rs:154-158` surfaced at `main.rs:194`.

### 2.11 Lossless at the boundaries (SIGTERM + retain-on-failure)

**Problem.** The per-slot capture Job runs with `RestartPolicy=Never` / `backoffLimit=0`. Losing the buffered `orders.acked` tail on every k8s teardown would silently drop the last orders of each run and mark Jobs Failed; a transient broker error would kill the capture.

**Decision.** On SIGTERM/SIGINT, flush the buffered tail and exit 0. On Kafka publish failure, retain the unsent tail for retry rather than dying.

**Why.** Graceful flush + retain-on-failure makes the measurement pipeline lossless at slot teardown and transient broker errors.

**Evidence.** `main.rs:184-189` SIGTERM branch calls `flush(...).await?; return Ok(())`; `ShutdownSignal` handles both signals `main.rs:203-227`; retain-on-failure `main.rs:294-318` `break; // stop this flush; retry the tail next tick` with oldest-drop bound `MAX_PENDING_EVENTS`. **Numbers:** flush interval 5 ms (`main.rs:46`); events chunked at `MAX_EVENTS_PER_BATCH` to stay under broker `max.message.bytes`.

### 2.12 Layout-agnostic FIX/HTTP/WS framing

**Problem.** Contestants control their response framing. A fixed-offset kernel parser would mis-extract `ClOrdID` for legitimate-but-unusual layouts (drifting tag 11, extra fields) and mismatch latencies.

**Decision.** Frame FIX by `BodyLength` (tag 9) and locate fields by scanning SOH-delimited `tag=value` pairs with **no fixed offsets**; handle HTTP chunked and WS-masked responses.

**Why.** Fairness: every valid implementation is measured, not just the reference shape.

**Evidence.** `parse.rs:4-8` "uses NO fixed offsets — FIX is framed by BodyLength (tag 9)"; `frame_fix` `parse.rs:74-109`; offset-independent ClOrdID test `parse.rs:510-517`; chunked-HTTP framing `parse.rs:179-215` + test `parse.rs:453-464`; masked WS `parse.rs:275-305`.

### 2.13 Attach robustness + wire contracts

XDP attaches DRV-mode first with an SKB-mode fallback (`main.rs:445` `program.attach(iface, XdpFlags::DRV_MODE)`, fallback `main.rs:459`). Wire contracts:

- Topic `orders.acked`: MessagePack-encoded `OrderAckedBatch{session_id, contestant_id, events[]}` (`schemas/go/topics/topics.go:15`, `topics.go:299-305`).
- `OrderAckedEvent` fields (`topics.go:308-317`, mirrored `main.rs:270-286`): `order_id, src_ip, src_port, tcp_seq, t3_xdp_ingress_ns, t7_xdp_egress_ns, pod_service_time_ns, exec_type, fill_qty, fill_price, orig_order_id, reordering_detected, retransmission_count`.
- Ring-buffer ABI: `CaptureRecord` = fixed 28-byte `repr(C)` header + `captured_len` payload bytes, little-endian (`bpfel`), over `BPF_MAP_TYPE_RINGBUF` named `EVENTS` (`capture.rs:1-12`, `ebpf.rs:69-83,140`).
- BPF object names: XDP `iicpc_xdp_ingress`, tc classifier `iicpc_tc_egress`, maps `EVENTS`/`DROPPED_EVENTS`/`TRUNCATED_CAPTURES` (`ebpf.rs:140-173`).
- Transport ports: FIX TCP 9898, REST/WS TCP 8080 (`ebpf.rs:44-46`, `capture.rs:24-27`) — also the in-kernel request/response direction discriminator.

---

## 3. Fairness & isolation — deep dive

The measured pod must run alone, pinned, with no noisy-neighbour channel and no path to anything internal. The orchestrator builds this contract; the network policies enforce it.

### 3.1 Guaranteed-QoS with integer-core validation (so pinning engages)

**Problem.** Guaranteed QoS unlocks the kubelet static CPU manager's exclusive cpuset pinning. But a millicpu value like `"2000m"` is still Guaranteed yet the CPU manager **silently skips pinning** — contestants then share cores via CFS bandwidth and hit a throttling-cliff in tail latency.

**Decision.** Build resources by `DeepCopy` of one `ResourceList` (request == limit, bytewise equal) **and** validate that CPU is an integer core count at config time, rejecting millicpu.

**Why.** The integer guard guarantees pinning actually engages, removing the throttling-cliff fairness regression at validation time rather than discovering it under load.

**Evidence.** `slot.go:738-750` `return corev1.ResourceRequirements{Requests: list.DeepCopy(), Limits: list.DeepCopy()}`; `slot.go:169` `if _, err := strconv.Atoi(cfg.CPU); err != nil || cpu.MilliValue()%1000 != 0 { return fmt.Errorf("CPU must be an integer core count for cpuset pinning, got %q", cfg.CPU) }`. **Numbers:** integer cores only; prod default `ALGO_CPU=2`, `ALGO_MEMORY=1Gi` (`deployment.yaml:53-56`).

### 3.2 Zero-disk-IO algo pod

**Problem.** Disk I/O is a shared, unmetered noisy-neighbour channel; one chatty contestant could perturb another's tail latency through it.

**Decision.** `readOnlyRootFilesystem=true` plus exactly four RAM-backed (`Medium: Memory`) tmpfs `emptyDir`s at `/tmp`, `/var/tmp`, `/var/log`, `/var/run`.

**Why.** Writes count against the pod's own `memory.max` instead of contending for node disk bandwidth — the cost is self-contained and the measured interval stays clean.

**Evidence.** `slot.go:424` `readOnlyRoot := true`; `slot.go:468-474` sets `ReadOnlyRootFilesystem`; `slot.go:514-529` `EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}`; mounts `slot.go:533-540`. **Number:** 4 tmpfs mounts.

### 3.3 Hardened untrusted-code pod spec

**Problem.** The algo image is untrusted contestant code.

**Decision.** Drop ALL capabilities, `AllowPrivilegeEscalation=false`, RuntimeDefault seccomp, `AutomountServiceAccountToken=false`, `RestartPolicy=Never`. `RunAsNonRoot` is deliberately **not** forced (legitimate images run as root and there is no USER the platform controls; the real isolation boundary is gVisor).

**Why.** Defense-in-depth that works even on root images; no SA token means a compromised algo cannot talk to the k8s API.

**Evidence.** `slot.go:468-473` `Capabilities.Drop: ["ALL"]`, `SeccompProfileTypeRuntimeDefault`, `AllowPrivilegeEscalation=false`; `slot.go:415,442` `AutomountServiceAccountToken=false`; `slot.go:440` `RestartPolicy=Never`; rationale `slot.go:462-467`.

### 3.4 gVisor as a pure config toggle

**Problem.** The orchestrator pod spec must stay byte-identical across dev and prod, but gVisor may be dropped on EKS (a sanctioned option).

**Decision.** Set `runtimeClassName` only when the `RUNTIME_CLASS` env is non-empty.

**Why.** Dropping gVisor becomes a config-only change with no code-path divergence; when gVisor is off, the default-deny network policy becomes the hard isolation gate.

**Evidence.** `slot.go:485-488` `if m.runtimeClass != "" { rc := m.runtimeClass; pod.Spec.RuntimeClassName = &rc }`; `deployment.yaml:44-45` `RUNTIME_CLASS value: ""` with toggle comment `deployment.yaml:38-43`.

### 3.5 Default-deny networking

**Problem.** Contestants must not reach Postgres, Kafka, MinIO, or each other — no lateral exfiltration or cross-contestant interference channel.

**Decision.** `podSelector:{}` selects every sandbox pod; ingress restricted to the benchmark namespace only; egress denies all RFC1918 CIDRs and permits only public internet + DNS.

**Why.** The only traffic the algo sees is the deterministic load from the bot fleet.

**Evidence.** `k8s/sandbox/network-policy.yaml:11` `podSelector: {}`; ingress from `namespaceSelector name:benchmark` (lines 16-20); egress `ipBlock cidr 0.0.0.0/0` with `except 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16` (lines 24-30). **Number:** 3 private CIDRs blocked.

### 3.6 Additive, label-scoped policies for the measurement/control plane

**Problem.** The privileged measurement plane (capture → Kafka) and control plane (orchestrator → k8s API) need egress, but widening the algo's network surface is forbidden.

**Decision.** The capture Job (`app=ebpf-capture`) and orchestrator (`app=sandbox-orchestrator`) each get exactly the one egress they need; algo pods (no such label) stay locked by the base policy.

**Why.** Capture reaches only Kafka:9092 + Loki:3100 + DNS; the algo still reaches nothing internal.

**Evidence.** `k8s/sandbox/network-policy.yaml:54-56` ebpf-capture-egress `app:ebpf-capture` egress to data ns 9092 (71-78); `k8s/sandbox/sandbox-orchestrator/network-policy.yaml:16-18` `app:sandbox-orchestrator` egress to API 443+6443 (62-69).

### 3.7 The measurement instrument never steals the measured cores

**Problem.** If the eBPF capture consumed a slice of the algo's exclusive cpuset, the act of measuring would perturb the thing being measured.

**Decision.** Make the capture Job deliberately **Burstable** (request 200m < limit 2 CPU) so the static CPU manager never hands it an exclusive cpuset; co-locate it on the algo's node via `NodeName`.

**Why.** Burstable keeps the capture on non-isolated shared cores, preserving measurement non-interference.

**Evidence.** `slot.go:712-723` Requests cpu:200m/mem:256Mi vs Limits cpu:2/mem:512Mi; rationale `slot.go:708-711`; co-located `slot.go:677`. **Numbers:** request 200m, limit 2 cores.

### 3.8 Per-pod NIC bandwidth caps

**Problem.** One chatty contestant could saturate the node NIC and starve neighbours.

**Decision.** CNI bandwidth-plugin annotations cap bytes/sec per pod (`kubernetes.io/egress-bandwidth`, `kubernetes.io/ingress-bandwidth`).

**Why.** Protects shared NIC bandwidth fairness. The comment is honest about the limit: it caps bytes/sec, **not** packets/sec, so high-pps/low-bps IRQ pressure is handled at node level (IRQ pinning), documented as out of code scope.

**Evidence.** `slot.go:402-408` sets the annotations from `m.egressBwBps`/`ingressBwBps`; rationale `slot.go:396-401`; prod `ALGO_EGRESS/INGRESS_BANDWIDTH=100M` (`deployment.yaml:65-68`). **Numbers:** 100M egress + 100M ingress.

### 3.9 One-pod-per-node via node-pool pinning

**Problem.** Platform workloads (submission-api, controller, Kafka) on a contestant node would perturb the measurement; effective one-pod-per-node is needed.

**Decision.** Achieve it by node-pool pinning + integer-core sizing near the node's full core count, not by anti-affinity in code: `nodeSelector pool=<value>` with toleration for taint `sandbox=true:NoSchedule`. Empty `SANDBOX_NODE_POOL` disables it for dev k3s.

**Why.** A dedicated sandbox pool keeps platform workloads off contestant nodes; integer-core Guaranteed sizing fills the node.

**Evidence.** `slot.go:496-504` Tolerations key:sandbox value:true effect:NoSchedule + NodeSelector pool; `deployment.yaml:60-61` `SANDBOX_NODE_POOL=""` (dev); capture shares the toleration `slot.go:621-631`; `MEASUREMENT_AND_FAIRNESS.md:84`.

### 3.10 Least-privilege RBAC (immutable live pods)

**Problem.** Mutating a running contestant's pod is a tampering vector against an in-progress measured run.

**Decision.** A single namespaced Role (no ClusterRole) with verbs limited to `create/get/list/watch/delete` on pods, services, and `batch/jobs`. `update` and `patch` are intentionally absent.

**Why.** The orchestrator can spawn and reap slots but can never modify a running contestant's pod — the measured run is immutable.

**Evidence.** `rbac.yaml:23-34` Role `slot-manager`; comment `rbac.yaml:17` "Update/patch are intentionally absent — we never mutate live algo pods"; `rbac.yaml:9-10` "No ClusterRoles in v1".

### 3.11 Self-healing leak backstops

**Problem.** A leaked Guaranteed pinned-CPU algo pod permanently removes a node from the pool; a leaked privileged `hostPID` capture Job (infinite loop) runs forever.

**Decision.** `ActiveDeadlineSeconds=3600` on both the algo pod and the capture Job, ownerReference GC of the Job on teardown, and an orphan reaper on startup.

**Why.** Both privileged/pinned resources are bounded in time; ownerReference GCs the Job on normal teardown; `reapOrphanCaptureJobs` cleans captures whose algo pod is already gone.

**Evidence.** `slot.go:44,47` deadlines; pod deadline `slot.go:430,441`; Job `ActiveDeadlineSeconds` `slot.go:660`; ownerReference `slot.go:644-649,656`; `reapOrphanCaptureJobs` `slot.go:358-375`. **Numbers:** 3600s deadline; `BackoffLimit=0`, `TTLSecondsAfterFinished=300` (`slot.go:611-612`).

### 3.12 Capturable-port fast-fail

**Problem.** The eBPF program is compiled for a fixed TCP port set {8080, 9898}. A slot on any other port would come up Ready, run a full benchmark, and emit an **empty** `orders.acked` stream — a silent measurement failure.

**Decision.** Reject non-capturable ports at `CreateSlot` with HTTP 400.

**Why.** Turns a silent data loss into a loud 400.

**Evidence.** `slot.go:196-200` `if m.captureEnabled { if _, ok := capturablePorts[port]; !ok { return fmt.Errorf("%w: port %d is not in the eBPF-capture set {8080, 9898}", ...) } }`; `capturablePorts` `slot.go:53`. **Number:** 2 capturable ports.

### 3.13 Slot lifecycle contracts

`slot_id == controller's session_id` verbatim — the orchestrator mints no IDs (`slot.go:65-66`). One Pod + one Service per slot, both named `algo-{slot_id}`, created and deleted together (`slot.go:60-66`, `slot.go:379-381`). HTTP API: `POST /slots`, `GET /slots/{id}`, `DELETE /slots/{id}`; states `creating|ready|failed|terminating` (`deriveState slot.go:752-803`). The controller's `WorkloadSpec` target host is `algo-{slot_id}.{ns}.svc.cluster.local` (`slot.go:385-387`). The contestant id is stamped as annotation `iicpc.dev/contestant-id` on the algo pod and read back by the capture Job (`slot.go:37,411-413,591`). The capture Job publishes `orders.acked` to `kafka.data.svc:9092` carrying `SESSION_ID=slot_id` + `CONTESTANT_ID` (`slot.go:633-642`).

---

## 4. Throughput & coordinated-omission correctness — deep dive

A load generator that slows down under back-pressure hides the very latency it caused. The bot fleet is built so it never does — and so every offered order is accounted for.

### 4.1 Zero-GC, pre-rendered wire frames

**Problem.** A GC pause or per-message heap allocation during a scoring wave injects jitter into the send schedule the platform measures, contaminating `service_time`/`response_time` percentiles.

**Decision.** Rust/Tokio with pre-rendered wire frames; `OrderFrame` holds pre-built `fix`/`rest`/`ws_bytes`. FIX `SendingTime` is patched in-place with O(1) delta-checksum arithmetic, not re-rendered.

**Why.** The pacer's only hot-path cost is a clock read, the frame bytes, and a TCP write.

**Evidence.** `services/bot-fleet/src/fix.rs:28-34` "Bots reuse these bytes … to avoid hot-path encoding"; `OrderFrame` `fix.rs:36-52`; in-place SendingTime patch `fix.rs:57-90`.

### 4.2 Open-loop pacing with `t0` captured before the sleep

**Problem.** If the loadgen captures the fire time after waking from sleep, a slow contestant that backs up the sender makes the loadgen silently slow down — hiding the latency it caused (coordinated omission).

**Decision.** Capture the intended fire time `target_send_ts_ns` **before** the sleep; advance by a fixed interval `interval_ns = 1e9 / target_rps` (no jitter).

**Why.** A slow contestant then shows a monotonically growing `send_ts_ns − target_send_ts_ns` gap — the CO signal — instead of the loadgen hiding it.

**Evidence.** `worker.rs:841` `let target_send_ts_ns = next_send_ns;` captured before `sleep_until` (`worker.rs:846`); comment `worker.rs:837-840`; `worker.rs:818` `interval_ns = 1_000_000_000 / target_rps`; `worker.rs:932` `next_send_ns = next_send_ns.saturating_add(interval_ns)`. **Number:** `interval_ns = 1e9 / target_rps`.

### 4.3 Three-loop r9 capture across all three protocols

**Problem.** Without per-protocol response capture, REST/WS contestants could never have `response_time`/coordinated-omission scoring; only FIX would be measured fully.

**Decision.** All three protocols run the same three-loop (writer + reader + watchdog) setup that captures `r9` (response time). A shared pending map plus a watchdog guarantees every offered order is accounted as either matched (`r9`) or `timed_out` — no phantom drops.

**Why.** All transports are measured identically.

**Evidence.** `worker.rs:592-598` "All three protocols (FIX, REST, WS) run the three-loop writer + reader + watchdog"; FIX `fix_write_loop`/`fix_read_loop`/`watchdog_loop` `worker.rs:667-698`; REST/WS share `run_readwrite_task` `worker.rs:1199-1242`; matching: REST on JSON `cl_ord_id` (`worker.rs:1409-1414`), WS on frame `cl_ord_id`, FIX on tag 11 (`worker.rs:990-1004`). **Number:** 3 loops × 3 protocols.

### 4.4 Barrier-after-fan-in (fresh go-time)

**Problem.** Embedding the barrier epoch in the `WorkloadSpec` before fan-in meant the epoch could be ~30s stale by barrier time (fan-in can take up to the 30s `ReadyDeadline`); `instant_from_unix_nanos` then returned `now()` and every worker fired immediately, destroying synchronization.

**Decision.** Compute `barrier_epoch_ns` **after** all ready signals are in, with a 500ms safety gap; connections are pre-warmed before the barrier.

**Why.** The go-time stays fresh; the only residual variance is Kafka delivery (~ms); pre-warm keeps connect latency out of the spike/ramp.

**Evidence.** `runner.go:241-251` "compute the epoch AFTER fan-in (step 5) so it stays fresh"; `runner.go:277` `barrierEpochNs := uint64(time.Now().Add(r.runConfig.BarrierSafetyGap).UnixNano())` after `awaitReady` (`runner.go:265`); worker conversion `instant_from_unix_nanos` (`worker.rs:1632-1639`), `sleep_until` (`worker.rs:816,1274`); pre-warm `worker.rs:439-442`. **Numbers:** BarrierSafetyGap 500ms; ReadyDeadline 30s.

### 4.5 Seeded, open-loop deterministic content

**Problem.** A fast and a slow algo must receive the byte-identical workload; the stream must not diverge based on how the algo responds.

**Decision.** Per-task seed = `global_seed ^ task_id`; `SmallRng::seed_from_u64`; the rng draw order is a fixed, documented contract; cancel/replace targets come from the bot's own self-accounted ledger, never from observed fills.

**Why.** Cross-contestant fairness — identical stream per (seed, mix).

**Evidence.** `content.rs:1-17` documents Deterministic + Open-loop as "load-bearing for cross-contestant fairness"; seed `content.rs:111-112`, `worker.rs:825`; `SmallRng::seed_from_u64` `content.rs:133`; "DRAW ORDER IS PART OF THE DETERMINISM CONTRACT … Do not reorder." `content.rs:144-146`; test `same_seed_produces_identical_stream` over 5000 draws `content.rs:269-276`. **Numbers:** seed = `global_seed XOR task_id`; 5000-draw determinism test.

### 4.6 Pipelined, size-bounded telemetry flush

**Problem.** A one-await-per-chunk publish loop serializes broker RTTs and caps the telemetry sink at ~50-100k ev/s, silently dropping events exactly at the scoring waves.

**Decision.** Fire every per-chunk delivery future before awaiting any (`join_all`), so one flush pays ~one broker RTT. Chunk at 1000 events (~360KB), under the 1MiB `max.message.bytes`. On failure, retain only the rejected chunks.

**Why.** Removes the RTT serialization that throttled the sink.

**Evidence.** `telemetry.rs:159-163` "the chunk publishes are PIPELINED … The old one-await-per-chunk loop … capped the sink at ~50-100k ev/s"; `futures::future::join_all` `telemetry.rs:180-185`; `MAX_EVENTS_PER_BATCH=1000` (`telemetry.rs:248`) equal to `Config::telemetry_batch_size` default 1000 (`config.rs:70`); failure semantics `telemetry.rs:232-239`. **Numbers:** 1000 events/message ≈360KB < 1MiB; old 200-cap ⇒ ~50-100k ev/s ceiling.

### 4.7 1:1 WorkloadSpec → partition → pod (roundrobin assignor)

**Problem.** librdkafka's default `range,roundrobin` (range wins) hands one pod a contiguous block of partitions, so multiple specs run serially on one pod and the extra specs miss the barrier — a collision-degraded multi-worker run.

**Decision.** Pin each spec to partition `worker_index % N` (controller) and consume with `partition.assignment.strategy=roundrobin` (worker).

**Why.** Each spec reaches a distinct pod when replicas ≥ worker_count.

**Evidence.** `kafka.rs:226-241` "1:1 WORKLOADSPEC→POD MAPPING" sets roundrobin and warns the range default "hands one pod a CONTIGUOUS block"; controller `producer.go:91-97` `workerIndexBalancer`; `validateWorkerCapacity` rejects `worker_count > 24` (`producer.go:110-112`); test `consumer_config_spreads_partitions_roundrobin` `kafka.rs:388-401`. **Numbers:** 24 `workload.assignments` partitions; worker_count ≤ 24.

### 4.8 Split producers (durability vs throughput)

**Problem.** A lost `ReadySignal` stalls the controller's fan-in (must be no-loss), while a lost telemetry event is just an HDR-histogram gap (loss-tolerant). A slow control-plane broker should not back-pressure telemetry, and vice versa.

**Decision.** Control plane (`bot.ready`): `acks=all` + idempotence, `linger.ms=0`. Telemetry (`orders.sent`): `acks=1`, `linger.ms=2`, `compression.type=lz4`.

**Why.** Decouples durability from throughput and isolates back-pressure.

**Evidence.** `worker.rs:131-136` rationale; control_producer `kafka.rs:123-134`; telemetry_producer `kafka.rs:150-161`. **Numbers:** control acks=all/linger 0ms; telemetry acks=1/linger 2ms/lz4.

### 4.9 30-minute poll interval keyed to worst-case wall time

**Problem.** The workload-assignment offset commits only **after** the whole run. If barrier-wait + task span + drain exceeds `max.poll.interval.ms`, Kafka rebalances mid-run and re-delivers the spec → duplicate execution. The old 300s default sat just below the ramp's ~305s worst case.

**Decision.** Raise `max.poll.interval.ms` to 30 min and reject up front any spec whose worst-case wall time exceeds the poll ceiling.

**Why.** Long high-RPS runs never rebalance mid-run.

**Evidence.** `config.rs:50` `DEFAULT_MAX_POLL_INTERVAL = 1_800_000`; `config.rs:42-46` rationale; guard `worker.rs:380-390` rejects `worst_case_wall_time_ns >= poll ceiling`; `worst_case = BARRIER_WAIT(120s) + max(offset+duration) + RESPONSE_TIMEOUT(5s)` (`worker.rs:410-420`); test `default_max_poll_interval_covers_ramp_worst_case` `config.rs:207-221`. **Numbers:** 30 min; ramp worst case ≈305s.

### 4.10 KEDA lag-driven autoscaling

**Problem.** The fleet must scale to the largest scenario's worker_count without standing pods idle; with `latest` offset reset, KEDA reads a fresh consumer group's backlog as zero lag and never scales up.

**Decision.** KEDA scales 2→50 workers on `workload.assignments` lag, with `offsetResetPolicy=earliest` matching the consumer.

**Why.** `earliest` is required so the unconsumed backlog is visible as lag.

**Evidence.** `k8s/benchmark/bot-fleet/scaledobject.yaml:20-21` min 2 / max 50; `lagThreshold '1'` (line 30); `offsetResetPolicy earliest` rationale (lines 31-35); pre-scale caveat (lines 6-11). **Numbers:** 2→50 replicas; lagThreshold 1; poll 5s.

### 4.11 Per-pod generation-ceiling ramp

**Problem.** A realistic 60/25/15 mix at 150k RPS would need thousands of sockets; that measures the socket fan, not the pod's raw generation ceiling.

**Decision.** Three serial constant-RPS tests at 20k / 60k / 150k aggregate RPS over a fixed 256-connection fan, pure `NewOrderSingle` limit orders (Market/Cancel/Replace = 0). One trigger = a 3-step ramp.

**Why.** Isolates one pod's raw generation ceiling by holding connection count fixed and scaling only per-connection rate.

**Evidence.** `loadgen-seed/main.go:15-17` defaults `20000,60000,150000`, `LOADGEN_CONNS=256`, `LOADGEN_DURATION_S=120`; `buildLean` spread with +1 rps remainder `main.go:61-84`; pure limit orders `main.go:79-81`. **Numbers:** 20,000 / 60,000 / 150,000 RPS; 256 conns; 120s each.

### 4.12 Watchdog accounts every offered order

**Problem.** An order sent but never answered must surface as `timed_out`, not vanish. The earlier guard dropped final-tick in-flight orders entirely (counted as neither matched nor timed_out).

**Decision.** On the final tick, evict every remaining entry — including one whose write was still in flight (`send_ts_ns == 0`) — recording it as `timed_out` (`recv_done_ts_ns=0`). First-response-wins via `remove()` consuming the entry.

**Why.** Correct offered-order accounting is essential for the coordinated-omission and coverage gates.

**Evidence.** `worker.rs:1085-1090` rationale; evicted orders `worker.rs:1101-1121`; first-response-wins `worker.rs:1000-1004`, `emit_response worker.rs:1377-1383`. **Numbers:** RESPONSE_TIMEOUT 5s; watchdog tick 250ms.

### 4.13 Load-plane contracts

`workload.assignments` (24 partitions): `WorkloadSpec` JSON, partitioned by `worker_index i → i%N`, consumed roundrobin (`producer.go:91-97`). `barrier` (3 partitions): `BarrierEvent` JSON carrying `target_epoch_unix_nanos` (`runner.go:277`, `worker.rs:304-305`). `bot.ready` (3 partitions): `ReadySignal` JSON, acks=all, key `session_id:worker_id` (`worker.rs:294-300`, `kafka.rs:323-325`). `orders.sent` (24 partitions, 1MiB max.message.bytes, 6h retention): `OrderSentBatch` MessagePack-named (`rmp_serde::to_vec_named`), ≤1000 events/message; carries `target_send_ts_ns` (t0), `send_ts_ns` (t1), `recv_done_ts_ns` (r9), `timed_out` (`telemetry.rs:148-153,219-223`). `TaskSpec`: `target_rps, start_offset_ns, duration_ns, profile, market/cancel/replace_pct`; sharded round-robin by `task_id` (`runner.go:460-463`).

---

## 5. Telemetry, HDR & visualization

The ingester turns two raw streams (`orders.sent`, `orders.acked`) into per-(session, wave) HDR histograms with the decomposition needed to see coordinated omission, and serves them to three independent decoders.

### 5.1 HDR configuration (fidelity in fixed memory)

**Problem.** Latency spans six orders of magnitude (sub-µs to multi-second) and zero-ns samples must not be silently dropped.

**Decision.** All histograms are `1 ns .. 60 s` at 3 significant figures; any `0 ns` sample is clamped to the lower bound `1` rather than dropped.

**Why.** 3 sig figs guarantees percentile fidelity to one part in a thousand across the whole range in fixed memory; the `0→1` clamp preserves genuine zero/sub-ns measurements.

**Evidence.** `aggregate.rs:103-111` `Histogram::<u64>::new_with_bounds(1, HDR_MAX_NS, HDR_SIGFIG)` with `HDR_MAX_NS = 60_000_000_000`, `HDR_SIGFIG = 3` (`aggregate.rs:25-26`); `record` does `hist.record(value_ns.max(1))` (`aggregate.rs:110`). **Numbers:** 1 ns .. 60 s, 3 sig figs, 0→1 clamp.

### 5.2 Three serialized HDR blobs per snapshot (CO decomposition)

**Problem.** To analyze coordinated omission offline you need the full distribution of algo time **and** the full round trip **and** the back-pressure, not just one curve.

**Decision.** Serialize three HDR blobs per snapshot with the V2+deflate serializer into separate BYTEA columns: `service_time` (t7−t3), `response_time` (r9−t0), `schedule_slip` (t1−t0). (A fourth, `fill_latency`, is tracked in memory.) HDR blobs are additive/mergeable.

**Why.** Additive blobs let the frontend/Python merge per-wave histograms into a true run-level distribution without re-streaming raw samples, and expose the full CO decomposition.

**Evidence.** `aggregate.rs:263-265` `hdr_encoded: serialize_hist(&w.service_time), rt_hdr_encoded: serialize_hist(&w.response_time), slip_hdr_encoded: serialize_hist(&w.schedule_slip)`; `aggregate.rs:306-310` `V2DeflateSerializer::new().serialize(...)`. **Number:** 3 serialized HDR blobs/snapshot.

### 5.3 One writer, three independent readers (cross-language audit)

**Problem.** A latency distribution is only trustworthy if an independent decoder agrees with the one that wrote it.

**Decision.** Rust writes V2-deflate; Go re-encodes the raw BYTEA to base64 for the API (pass-through, no re-interpretation); the Next.js frontend decodes client-side; a standalone Python script decodes the same blob with the independent `hdrh` library as a cross-check.

**Why.** Cross-language HDR interop makes the measured distribution verifiable by an independent implementation — a measurement-integrity guarantee.

**Evidence.** Rust `aggregate.rs:308`; Go pass-through `leaderboard-api/internal/read/store.go:292` `p.HDREncoded = base64.StdEncoding.EncodeToString(hdr)`; JS `frontend/src/utils/hdr.ts:62` `hdr.decodeFromCompressedBase64(...)`; Python `deploy-local/plot-hdr.py:59` `HdrHistogram.decode(b64.strip())`. **Number:** 1 writer + 3 readers.

### 5.4 Last-blob-per-wave merge (no double-counting)

**Problem.** Per-wave blobs are cumulative within a wave (to avoid re-sending the full sample stream every second). Summing all rows would double-count the cumulative prefix and inflate p99/tps.

**Decision.** Take only the **last** blob per (session, wave), then HDR-add across waves. The same discipline is implemented independently in both the JS frontend and the Python plotter.

**Why.** This is the one correct way to reconstruct the distribution; pinning it in two implementations guards measurement accuracy.

**Evidence.** `hdr.ts:39-71` `mergeLastPerWave` keeps `lastByWave`/`lastTime` then `merged.add(h)` ("Never sum all rows: that double-counts the cumulative prefix"); `plot-hdr.py:50-62` `select distinct on (wave_index) ... order by wave_index, time desc` then `merged.add(h)`.

### 5.5 Dedup keyed on response arrival (late-fill safety)

**Problem.** `orders.acked` emits N events per order (ack + each fill) all sharing one `t3`. A `t3`-based idle clock would evict a resting order mid-stream, and a trailing fill arriving past the idle window would be re-scored as a fresh first response — corrupting both `service_time` and tps.

**Decision.** Dedup the scored sample once per order with a first-response tracker whose idle clock is keyed on each response's **arrival** time (t7, max-of-arrivals), not the fixed `t3`.

**Why.** A resting order stays deduped across a fill stream longer than the 5s idle window; a trailing fill is never re-scored.

**Evidence.** `join.rs:31-42` `observe` returns true only on first sight and does `*last = (*last).max(now_ns)`; `aggregate.rs:201` `if self.first_response.observe(&e.order_id, e.t7_xdp_egress_ns)`; test `aggregate.rs:391-408` `late_streaming_fill_not_rescored` asserts `tps_1s == 0.0` for the trailing fill. **Number:** `FIRST_RESP_IDLE_NS = 5 s`, 1 sample/order.

### 5.6 No fabricated zero-latency rows

**Problem.** A wave whose orders all timed out (sent-only window) would, if emitted, post a misleading 0-latency, unattributable row that drags down the `metrics_10s` p99 average.

**Decision.** Emit a metrics row only when a scored `service_time` sample exists **and** the contestant is known; per-interval counters still reset.

**Why.** A contestant who never responded cannot accidentally post a perfect latency.

**Evidence.** `aggregate.rs:243` `if !w.service_time.is_empty() && !w.contestant_id.is_empty()`; test `aggregate.rs:413-422` `sent_only_window_emits_no_row`. **Number:** 2-condition emit gate.

### 5.7 Stable wave bucketing decoupled from the control plane

**Problem.** Wave assignment must be deterministic even under reordered Kafka delivery and independent of the scenario/ramp schedule.

**Decision.** `wave_index = floor((t − session_start) / wave_ns)`, where `session_start` is the **earliest** timestamp ever seen for the session across both streams (lowered if an earlier event arrives later).

**Why.** Earliest-wins origin makes the same order always land in the same wave regardless of arrival order.

**Evidence.** `aggregate.rs:144-153` `wave_of` with `if t_ns < *start { *start = t_ns; }` then `((t_ns - *start) / self.wave_ns)`; test `aggregate.rs:443-470`. **Number:** `wave_ns` default 20 s.

### 5.8 Cumulative percentiles, per-interval rates

**Problem.** A wave's percentile distribution should reflect its whole life, but throughput/error rate must reflect "right now" — mixing these is a common telemetry bug.

**Decision.** Percentiles read from a never-reset histogram (cumulative p50/p90/p99/p999); `tps_1s` and `error_rate` come from integer counters zeroed every snapshot.

**Why.** Rolling distribution + instantaneous rate from one accumulator, correctly separated.

**Evidence.** `aggregate.rs:254-261` `value_at_quantile(...)` on the never-reset histogram vs counter resets `aggregate.rs:268-273`; test `aggregate.rs:489-508`.

### 5.9 Self-provisioning stores with graceful degradation

**Problem.** No migration tool, and the engine must still run against plain Postgres where TimescaleDB is absent.

**Decision.** On startup, create a TimescaleDB hypertable (1-hour chunks) + a `metrics_10s` continuous aggregate (avg_p99/peak_tps/avg_error_rate, refreshed every 10s, 10s lag, 1h look-back) + a Redis hot hash. The base table is fatal-on-failure; every TimescaleDB extension/aggregate step is best-effort.

**Why.** Deployable against any Postgres while still getting rollups where TimescaleDB exists; the continuous aggregate gives the leaderboard a pre-computed p99/peak-tps without scanning raw rows.

**Evidence.** `store.rs:48-59` `TIMESCALE_SETUP` (`create_hypertable ... INTERVAL '1 hour'`, `metrics_10s ... avg(p99_ns) AS avg_p99, max(tps_1s) AS peak_tps`); `store.rs:90-106` base table fatal vs best-effort loop `warn!(...)`; `redis_sink.rs:43` `conn.hset_multiple`.

### 5.10 Wave-disambiguated Redis key

**Problem.** A single snapshot tick at a wave boundary can emit two waves for the same contestant in unspecified order; a contestant-only key would let concurrent waves overwrite each other nondeterministically, flickering the live SSE value.

**Decision.** Key the Redis hot hash by `contestant:{contestant}:{session}:{wave}`.

**Why.** A deterministic key per window stops the flicker.

**Evidence.** `redis_sink.rs:55-60` `format!("contestant:{}:{}:{}", s.contestant_id, s.session_id, s.wave_index)`; test `redis_sink.rs:89-99`. **Number:** 3-part key.

### 5.11 The Gil-Tene latency-by-percentile chart

**Problem.** Tail latency must be surfaced honestly, and coordinated omission made visible.

**Decision.** Decode `hdr_encoded` client-side and plot `service_time` vs `response_time` on a log "nines" x-axis (`x = 100/(100−p)`), overlaying both curves.

**Why.** Plotting the decoded full distribution (not a histogram of p99 snapshots) on the log-percentile axis is the correct way to show tails; the visible gap between the two curves **is** the coordinated-omission / network / kernel-queueing overhead — the scored algo metric isolated from the bot-side round trip.

**Evidence.** `hdr.ts:83-87` `nines: pct < 100 ? 100 / (100 - pct) : 1e7, value_us: merged.getValueAtPercentile(pct) / 1000`; `HdrPercentileChart.tsx:50-59` `<XAxis dataKey="nines" type="number" scale="log" ticks={TICKS} ...>` with `TICKS [1,10,100,1000,10000]` (line 17). **Number:** 2 overlaid HDR curves.

### 5.12 Telemetry/HDR contracts

`orders.sent` (key=session_id) and `orders.acked` (key=contestant_id) are MessagePack-named, consumed via `rmp_serde::from_slice`; Go↔Rust interop is by matching string field keys. TimescaleDB `metrics` hypertable columns: `time, session_id, contestant_id, wave_index, p50/p90/p99/p999_ns, rt_p50/rt_p90/rt_p99_ns, tps_1s, error_rate, hdr_encoded BYTEA, rt_hdr_encoded BYTEA, slip_hdr_encoded BYTEA` (`store.rs:15-33`) + `metrics_10s` continuous aggregate. Redis hot hash key `contestant:{contestant_id}:{session_id}:{wave_index}` with fields `p50_ns,p99_ns,p999_ns,tps_1s,error_rate,wave_index,session_id,updated_at_ns` (`redis_sink.rs:33-42`; note `p90_ns` is in TimescaleDB but not Redis). HDR wire format: HdrHistogram V2+deflate, BYTEA, surfaced as base64 (StdEncoding) under `hdr_encoded`/`rt_hdr_encoded`/`slip_hdr_encoded`; decoded by hdr-histogram-js and Python `hdrh`. Prometheus `/metrics` on `METRICS_ADDR` (default `0.0.0.0:9090`), all series prefixed `iicpc_telemetry_`. Price fixed-point: `TELEMETRY_PRICE_SCALE = 1_000_000_000`, mirrored in `schemas/rust/src/lib.rs:15` and `schemas/go/topics/topics.go:18`; `fill_price = round(P*1e9)`.

---

## 6. Correctness oracle & scoring

Throughput only counts if the fills are correct. A reference price-time-priority CLOB replays the order stream and diffs every reported fill; scoring then gates throughput behind latency, error-rate, and correctness.

### 6.1 Reference CLOB: true price-time priority

**Problem.** Every reported fill must be diffed against a canonical ground truth that obeys the exact rule a correct exchange obeys.

**Decision.** A btree per side for O(log n) best-price, a FIFO slice per price level for time priority, and an O(1) order-index map for cancel/replace.

**Why.** Price-then-time is the exact CLOB rule, so any deviation can be called a violation with authority.

**Evidence.** `book/book.go:65-82` `Engine{asks/bids *btree.BTreeG[*priceLevel]; index map[string]*restingOrder}`; ascending asks (best=Min) vs descending bids `book.go:86-87`; FIFO `book.go:61` "front = oldest = time priority"; `book.go:164` `maker := level.orders[0]`. **Number:** btree degree 32.

### 6.2 Maker/taker inferred, not trusted

**Problem.** The wire protocol carries no maker `ClOrdID`; the validator could not otherwise attribute fills or detect self-trades — and trusting a contestant-supplied counterparty would be gameable.

**Decision.** The engine knows every order from `orders.sent` and writes a Fill for **both** sides of each trade, inferring the maker from its own FIFO.

**Why.** Attribution without a maker id on the wire, and un-gameable by misreporting counterparties.

**Evidence.** `book/book.go:166-176` appends `Fill{taker}` + `Fill{maker}` + `Trade{MakerOrderID,TakerOrderID,MakerSeq}`; `validate/validate.go:6-10` "maker is INFERRED from the reference book's FIFO". **Number:** 2 fills per trade.

### 6.3 Six violation classes with queue-jump auto-split

**Problem.** "Wrong" is not enough — the platform must explain why, and catch the subtle rule that a price-changing REPLACE forfeits queue position.

**Decision.** Six violation classes (Phantom, Overfill, Price, Time, SelfTrade, CancelReplaceLoss); a queue-jump auto-splits into Time vs CancelReplaceLoss based on whether the jumping order was repriced.

**Why.** Granular taxonomy explains the failure; the cancel-replace distinction catches queue-position forfeiture.

**Evidence.** `validate/validate.go:42-47` the six consts; `validate.go:208-217` `flagJump` checks `e.Repriced(o.OrderID)`; `book.go:260-262` `if o.Price != ro.price { e.repriced[...] = true }`. **Number:** 6 classes.

### 6.4 TCP head-of-line replay (effective_t3)

**Problem.** A packet reordered on the wire would be buffered by TCP until its predecessor arrived; scoring on raw arrival would punish a contestant for kernel/wire reordering they never observed.

**Decision.** Per-flow `tcp_seq` ordering with running-max `effective_t3` promotion, then a global stable sort by `(effective_t3, flow_id, tcp_seq)` — the deterministic order TCP userspace would have seen.

**Why.** `effective_t3` reconstructs the delivery order the contestant was actually accountable for.

**Evidence.** `replay/order.go:35-46` running-max `if !started || o.T3Ns > prev { o.EffectiveT3 = o.T3Ns } else { o.EffectiveT3 = prev }`; `order.go:51-60` global `SliceStable` by EffectiveT3 then Flow then TCPSeq.

### 6.5 100ns cross-flow tie tolerance

**Problem.** Below the eBPF timestamp jitter floor, the platform cannot prove which of two orders on different flows arrived first; penalizing either choice would be unfair.

**Decision.** Suppress ordering-dependent violations when two orders are on different flows and their `effective_t3` differ by < 100ns. Within a single flow the byte stream is unambiguous, so the check stays strict.

**Why.** Tolerance is applied exactly where measurement uncertainty exists, and nowhere else.

**Evidence.** `replay/order.go:21` `const TieToleranceNs = 100`; `order.go:68-73` `CrossFlowTie` returns false for same-flow else `absDiff(EffectiveT3) < tolerance`; `validate.go:260` `if replay.CrossFlowTie(o, other) { continue }`. **Number:** 100 ns.

### 6.5.1 Aggressive-fill tolerance — judging a live engine against an offline replay

**Problem.** The reference replays the order stream in one canonical offline order — `effective_t3`, the order bytes hit the algo's veth (kernel ingress). A **live** in-sandbox engine cannot observe `effective_t3`; it processes orders in the order its sockets become readable. For resting (limit) orders this is benign — a crossing limit is unambiguous and the 100 ns cross-flow tolerance (§6.5) covers the residual. But a **market/IOC** fill is decided entirely by which liquidity is resting at the instant it is processed, so any cross-flow reordering between the veth and the engine's userspace flips the outcome — and market orders never rest, so the §6.5 tolerance (which only suppresses *resting-order* time-priority breaks) does not apply to them at all. A correct market-filling engine is therefore penalized for an ordering it could not observe.

We measured this exactly. Same matching logic, three configurations:

| Engine | Validator | Correctness |
|---|---|---|
| Go `net/http`, goroutine-per-connection | strict | **0.22** (every violation a market-order "price" break; limit fills 0 violations) |
| Go `net/http` | 50 ms tolerance | **0.92** (residual = Go scheduler reordering by ms under GC on a shared host, beyond 50 ms) |
| Tight single-threaded **epoll** (readiness order ≈ veth arrival) | 50 ms tolerance | **1.00 / 0.998** |

The split is the whole finding: it is **not** a broken validator and **not** a hardware wall — it is (1) **engine architecture** (a real HFT engine is a single-threaded arrival-order loop; a goroutine-scrambled one diverges by milliseconds) and (2) a **measurement-fairness gap** the platform must close for aggressive fills.

**Decision.** During the reference replay, record each resting order's **availability window** `[enter_t3, exit_t3]` (enter = its `effective_t3`; exit = the `effective_t3` of whatever consumed/cancelled it, or +∞ if still resting at end). A reported aggressive fill at price P is accepted if **non-self opposite liquidity at P was genuinely resting within ±tolerance of the aggressor's `effective_t3`** — the fairness analog of the cross-flow tie tolerance, extended to liquidity timing. Overfill and self-trade are never tolerated; under-reporting is never penalized.

**Why.** It holds a live engine to the only thing it *can* satisfy: that it matched real liquidity present near its arrival, not an exact offline interleaving it cannot see. Production sets a tight tolerance (µs, matched to veth→userspace reorder) so a well-built engine fits inside it without it forgiving genuinely wrong fills.

**Evidence.** `book.go` `type Availability` + `closeAvail` + `Availability()` (liquidity timeline); `validate.go` `availByLevel` index + `tolerated()` + `windowsOverlap()`; `main.go` `AGGRESSIVE_FILL_TOLERANCE_US`. Reference contestants: `deploy-local/passing-engine-epoll` (tight epoll) and `deploy-local/passing-engine` (Go `net/http`). **Numbers:** 0.22 → 0.92 → 1.00/0.998; tolerance configured in µs (0 = strict).

### 6.6 Price-scale reconciliation

**Problem.** `orders.sent.price` is a raw FIX tag-44 integer while `orders.acked.fill_price` is fixed-point ×1e9 from the kernel parser; without rescaling, every fill is a phantom price violation and CorrectnessScore collapses to ~0.

**Decision.** Lift reference order prices into the eBPF fixed-point domain (×1e9) before comparing.

**Why.** Avoids a silent total miscalibration.

**Evidence.** `pipeline/pipeline.go:52` `Price: int64(s.Price) * int64(topics.TelemetryPriceScale)`; rationale `pipeline.go:46-51`; `topics.go:18` `TelemetryPriceScale = uint64(1_000_000_000)`. **Number:** 1e9.

### 6.7 UUIDv7-bounded Kafka drain

**Problem.** These topics accumulate every session's data; draining one session from offset 0 scanned the entire topic and OOMKilled the pod even at 3Gi.

**Decision.** Extract session start from the 48-bit UUIDv7 timestamp and use a time-based offset lookup to bound the scan to seconds of data; non-v7 ids fall back to earliest so no event is ever lost.

**Why.** Avoids a DB round-trip on the hot completion path and bounds memory.

**Evidence.** `source/drain.go:264-285` `sessionStartFromID` parses `raw[6]>>4 == 7` and the first 48 bits big-endian; `drain.go:42` `startMargin = 60 * time.Second`; `drain.go:247-253` `startOffsetForSession`; doc `drain.go:14-20`. **Numbers:** 60s safety margin; 3Gi heap that OOM'd before the bound.

### 6.8 The −1 drain bug (resolveStart)

**Problem.** Kafka `ListOffsets` returns −1 when no message has a timestamp at-or-after the requested time. Falling through to `SetOffset(-1) = LastOffset` made the reader block on the high watermark until the deadline and drain zero events — a spurious timeout / zero-correctness verdict for the whole session on any partition holding no data for that session.

**Decision.** Map −1 to an **empty** range (`start = last`), not LastOffset.

**Why.** Empty-range read returns immediately with zero events instead of blocking to deadline.

**Evidence.** `source/drain.go:233-238` `func resolveStart(seek, last int64) int64 { if seek < 0 { return last } return seek }`; comment `drain.go:225-232`. **Number:** −1 sentinel.

### 6.9 At-least-once acked dedup

**Problem.** Producers are at-least-once; a redelivered acked event would inflate cumulative quantity into a false overfill/phantom violation.

**Decision.** Dedup by the 3-tuple `(order_id, exec_type, t7_xdp_egress_ns)` before assembly.

**Why.** The per-response XDP egress timestamp distinguishes genuine repeat responses (two partial fills, same exec_type) from true duplicates.

**Evidence.** `source/drain.go:78-82` `type ackKey struct{ orderID; execType; t7NS uint64 }`; seen-map dedup `drain.go:110-118`; rationale `drain.go:73-77`. **Number:** 3-tuple dedup key.

### 6.10 Single-publisher guarantee (one-way upsert claim)

**Problem.** With `VALIDATOR_CONCURRENCY>1` and at-least-once redelivery, exactly one worker must publish the primary score; a check-then-act TOCTOU between the status precheck and the write could let a fabricated timeout beat a concurrent real validation to the permanent verdict.

**Decision.** A WHERE-guarded conditional upsert: a real scored row is immutable, and a `timed_out` placeholder can only ever be overwritten **by** a scored row, never the reverse. `RowsAffected()==0` means "not ours to publish."

**Why.** Closes the TOCTOU; the real verdict always wins.

**Evidence.** `store/postgres.go:170` `WHERE correctness_summary.status = 'timed_out' AND EXCLUDED.status = 'scored'`; `postgres.go:181-185` `RowsAffected()==0`; `main.go:263-269` "lost the claim race"; `main.go:215-221` timed_out placeholder re-runs validation. **Number:** `VALIDATOR_CONCURRENCY` default 4.

### 6.11 Recoverable timed_out placeholder (no fabricated counts)

**Problem.** A timeout must not become a permanent zero-correctness score or a phantom-fill fabrication.

**Decision.** The placeholder fabricates **no** counts (zero fills/violations, status-column signal), so it is neutral in `aggregateCorrectness` and skipped by the per-session gate (`TotalFills==0`); a redelivery re-runs the full validation to recover.

**Why.** Transient slowness never corrupts a contestant's verdict.

**Evidence.** `main.go:310-318` `timeoutRecord` with `validate.Report{}` zero value; rationale `main.go:306-309`; score-computer `score/score.go:181` `if s.Correct.TotalFills > 0` skips the gate; `score.go:255-258` aggregate returns 0 only when total==0. **Number:** 0/0 placeholder.

### 6.12 Peak-sustained-TPS gating (stable-window p99, warmup skip)

**Problem.** Raw burst TPS is meaningless if it was only achieved while blowing the p99 budget or erroring out. Two subtleties make the *naïve* gate wrong: (a) gating on the **worst single second** fails an otherwise-healthy wave on one cold-start or GC-transient second; (b) **wave 0** is dominated by connection/cache/JIT warmup (tens of ms even on a healthy node), so including it zeroes every climb.

**Decision.** Walk the wave schedule in offered-RPS order, **skip wave 0 (warmup)**, and gate each wave on the **median of its per-second p99** (the sustained "stable-window" value), not the max. Stop at the first wave failing the error-rate or median-p99 gate; peak is the last passing wave's offered RPS. Break-on-first-failure keeps peak monotonic.

**Why.** Throughput is "sustainable-under-SLA"; the median is the wave's representative latency (a single transient second is not), and the warmup wave is not representative of the engine at all. Error rate stays a *max* — a sustained error second is a real fault.

**Evidence.** `score/score.go` wave loop: `if wave.WaveIndex == 0 { continue }` (warmup skip); gate on `m.StableP99NS` (median via `medianU64` over `summarizeMetrics`'s per-second `p99Samples`), `MaxP99NS` retained for the dashboard; `else Passed + PeakSustainedTPS=wave.OfferedRPS`, then `if !wr.Passed { break }`. Defaults: `DefaultMaxErrorRate=0.01`, `DefaultMaxP99NS=1_000_000` (1 ms), `DefaultWaveDurationNS=20s`. **Numbers:** p99 gate 1 ms (production; relax only for jittery local hardware); error-rate 0.01; wave 20 s.

> **Correctness DQ gate.** A run-group is disqualified when its aggregate correctness `Σvalid/Σtotal`, or any session's correctness ratio, falls **below 0.95** (`scoring_config.correctness_dq_threshold`, judge-tunable, was 0.99). The earlier binary "any ramp violation → DQ" rule was removed: a handful of order-dependent violations out of tens of thousands of fills (see §6.5.1) must not disqualify a correct engine — only a sub-threshold *ratio* does, which the per-session gate (the ramp is a session) already enforces. The rule is stated verbatim on the leaderboard (`ScoringRules`).

### 6.13 DQ-aware ranking

Disqualified-ascending is the leading sort key, so disqualified contestants sort below all qualified ones regardless of raw TPS, with the remaining keys breaking ties among the qualified.

### 6.14 Oracle/scoring contracts

The validator consumes `benchmark.status.updated` (JSON `BenchmarkStatusUpdated`), triggers on `Status==completed`, drains `orders.sent` + `orders.acked` (msgpack, per-partition to a snapshotted high-watermark), and publishes `scores.correctness` (`CorrectnessScoreEvent: ValidFills/TotalFills/CorrectnessScore/ViolationCount/SentCount/AckedCount/MatchedCount/ComputedAtNS`). It owns `correctness_summary`, `correctness_violations`; score-computer owns `score_progress`, `scoring_config(v1)`, `scores`, reads metrics from TimescaleDB (`p99_ns/tps_1s/error_rate` per `wave_index`), and writes the `scores` row consumed by leaderboard-api. Order-id wire contract: `{session_id}_{bot_id}_{seq}_{suffix}`, produced by bot-fleet `src/fix.rs`, parsed by `model.ParticipantOf`.

---

## 7. Build pipeline, control plane, security & auth

### 7.1 Build pipeline (Kaniko → registry/ECR)

**Problem.** Contestant source must be built into a runnable image without a privileged Docker daemon on the cluster, and the image must land in a registry the sandbox node can pull.

**Decision.** Build the contestant image with Kaniko into the registry/ECR; the ECR repo is pre-created in the spawner.

**Why.** Kaniko builds in userspace (no daemon, no privileged socket), keeping the build path off the contestant-isolation threat surface; pre-creating the ECR repo avoids a first-pull race where the push target does not yet exist.

**Evidence.** Build flow described in the recent-work record (Kaniko → registry; ECR repo pre-create in spawner). The sandbox pulls the resulting `algo` image into the Guaranteed-QoS pod (`slot.go` algo container spec, §3.1–3.3).

### 7.2 Control plane (controller / bot-fleet-controller)

The controller drives a run as a sequence: request a slot (`POST /slots`), poll to `ready` (`GET /slots/{id}`), publish `WorkloadSpec`s to `workload.assignments` partitioned per worker (`producer.go:91-97`), fan in `bot.ready` signals, compute the fresh barrier epoch **after** fan-in (`runner.go:241-277`, §4.4), publish the `BarrierEvent`, let the waves run, then on completion publish `benchmark.status.updated` which triggers the validator (§6.14). The slot is torn down with `DELETE /slots/{id}`, and the capture Job is GC'd by ownerReference (§3.11).

### 7.3 Security & auth

- **JWT verification in submission-api.** Submissions are authenticated; the API verifies the JWT before accepting a build/run, so only authorized contestants can spend cluster resources. (Recent work: "JWT verification in submission-api.")
- **Fail-closed redirect allowlist + graceful shutdown** in auth-api; hardened deployment (recent commit `bbeb14f`).
- **No SA token in the algo pod** (`AutomountServiceAccountToken=false`, §3.3) — a compromised algo cannot reach the k8s API.
- **Least-privilege namespaced RBAC** with no ClusterRole and no `update`/`patch` (§3.10).
- **Default-deny networking** so the algo reaches nothing internal (§3.5).
- **Hardened untrusted-code pod spec** — drop ALL caps, no privilege escalation, RuntimeDefault seccomp, gVisor as the isolation boundary when enabled (§3.3, §3.4).
- **CSP `unsafe-inline` interim** and dropping the token from the SSE URL on the frontend (recent commit `c04d640`): the SSE token was moved out of the URL (where it would leak via logs/referrer) and consumed as named SSE events with real payload shapes.

---

## 8. Observability

- **Prometheus metrics.** The telemetry-ingester exposes `/metrics` on `METRICS_ADDR` (default `0.0.0.0:9090`), all series prefixed `iicpc_telemetry_` (§5.12).
- **Kernel drop/truncation counters.** The eBPF data plane maintains `DROPPED_EVENTS` and `TRUNCATED_CAPTURES` maps (`ebpf.rs:140-173`); `TRUNCATED_CAPTURES` is surfaced at `main.rs:194` and is expected to be 0 given the MTU clamp (§2.4, §2.10). These make ring-buffer pressure and truncation directly visible.
- **Logs to Loki.** The capture Job's egress policy explicitly permits Loki:3100 (§3.6), so the privileged measurement plane ships logs without widening the algo's network surface.
- **Live SSE leaderboard.** leaderboard-api serves the live feed; the frontend consumes named SSE events with real payload shapes (recent commit `c04d640`), backed by the wave-disambiguated Redis hot hash (§5.10) so live values do not flicker at wave boundaries.
- **Gil-Tene latency-by-percentile chart.** The decoded full HDR distribution surfaces tails and makes coordinated omission visually explicit (§5.11).
- **Python cross-check decoder.** `deploy-local/plot-hdr.py` decodes the same HDR blobs with the independent `hdrh` library (§5.3) — an offline audit that the Rust serializer and JS decoder agree.
- **Self-provisioned rollups.** The `metrics_10s` continuous aggregate gives a pre-computed `avg_p99`/`peak_tps`/`avg_error_rate` without scanning raw rows (§5.9).

---

## 9. Deployment model

### 9.1 Config-only environment parity

**Problem.** The pod spec and code paths must not diverge across dev (k3s) and prod (EKS); environment differences must be config only.

**Decision.** Every environment difference is an env var, not a code branch: `RUNTIME_CLASS` (gVisor toggle, §3.4), `SANDBOX_NODE_POOL` (node-pool pinning, empty disables for dev k3s, §3.9), `ALGO_CPU`/`ALGO_MEMORY` (§3.1), `ALGO_EGRESS/INGRESS_BANDWIDTH` (§3.8). The orchestrator pod spec is byte-identical; only env values differ.

**Why.** No code-path divergence means dev behavior predicts prod behavior; dropping gVisor or node pinning on a constrained cluster is a config change with zero code risk.

**Evidence.** `deployment.yaml:38-68` (RUNTIME_CLASS empty default, SANDBOX_NODE_POOL empty default, ALGO_* defaults); §3.4/§3.9 code toggles.

### 9.2 Local k3s

For dev, `RUNTIME_CLASS=""` (gVisor off, netpol becomes the hard gate), `SANDBOX_NODE_POOL=""` (no node pinning), with the same default-deny network policy and the same Guaranteed-QoS integer-core sizing so cpuset pinning behaves the same as prod. The capturable-port guard, leak backstops, and RBAC are identical.

### 9.3 EKS (gVisor-optional)

On EKS, gVisor may be dropped (a sanctioned option per the platform rule) by leaving `RUNTIME_CLASS=""`; the default-deny netpol then becomes the hard isolation gate (§3.5). Two EKS-specific hardenings exist:

- **MTU clamp 9001 → 1500.** EKS nodes default to a 9001 jumbo MTU; the attach-time clamp (§2.4, §2.5) brings the algo veth to 1500 so one packet equals one order and captures never truncate (`mtu.rs:145`).
- **Production deployment hardening** (recent branch `fix/eks-production`): graceful shutdown + hardened auth-api deployment, submission-api `/ready` probe + `GOOGLE_CLIENT_ID`, KEDA→Kafka allowed, 6h retention on `orders.*`, and a fixed frontend nginx packaging (mime.types, snippets include, pid, server_tokens, temp paths) with env/probes/mounts (commits `bbeb14f`, `c04d640`, `c3410a4`).

### 9.4 Minimal-cost smoke profile

**Problem.** The first EKS bring-up should be cheap but must still respect the hard floor for exclusive cpuset pinning.

**Decision.** A minimal smoke deploy: 2× `m6i.xlarge` general + 1× `c6i.2xlarge` sandbox (the hard floor for cpuset pinning), ~$22/day, with scenarios reseeded small. Benchmark botworker Spot nodes and guardrails come later.

**Why.** The sandbox node must be a dedicated, integer-core machine for Guaranteed-QoS cpuset pinning to engage (§3.1, §3.9); a single `c6i.2xlarge` satisfies that floor at minimal cost while the general pool runs the platform services.

### 9.5 Capacity & autoscaling at prod scale

The bot fleet scales 2→50 workers via KEDA on `workload.assignments` lag with `offsetResetPolicy=earliest` (§4.10); deterministic large runs should pre-scale (`scaledobject.yaml:6-11`). Worker count is bounded by the 24-partition layout (`worker_count ≤ 24`, §4.7). `orders.*` topics retain 6h of data (commit `bbeb14f`), which bounds the validator's UUIDv7 drain window (§6.7) and the telemetry replay surface.

---

## Appendix — invariants at a glance

| Invariant | Mechanism | Evidence |
|---|---|---|
| Latency un-gameable | t3 XDP ingress, t7 tc egress, both `bpf_ktime_get_ns` | `ebpf.rs:262`, `matcher.rs:119` |
| Skew-invariant metric | same-node MONOTONIC, offset cancels in difference | `pipeline.rs:34-36`, `ebpf.rs:413-417` |
| One packet = one order | offloads off + MTU clamp ≤1500 | `main.rs:382`, `mtu.rs:74-80,145` |
| Per-order latency under TCP segmentation | first-byte attribution | `reassembly.rs:205-216` |
| Identical workload per contestant | seeded open-loop, fixed draw order | `content.rs:144-146` |
| Loadgen never hides latency | t0 captured before sleep (open loop) | `worker.rs:837-846` |
| Pinned cores actually engage | Guaranteed QoS + integer-core guard | `slot.go:169,738-750` |
| Algo reaches nothing internal | default-deny netpol, RFC1918 blocked | `network-policy.yaml:11,24-30` |
| Instrument never steals measured cores | capture Burstable | `slot.go:708-723` |
| No double-counted percentiles | last-blob-per-wave merge | `hdr.ts:39-71`, `plot-hdr.py:50-62` |
| Throughput is sustainable-under-SLA | gated, break-on-first-failure | `score.go:227-241` |
| Correct fills, un-gameable counterparty | reference CLOB, maker inferred from FIFO | `book.go:166-176` |
| Config-only env parity | env-var toggles, byte-identical pod spec | `deployment.yaml:38-68`, `slot.go:485-488` |
