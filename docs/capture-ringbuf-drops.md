# Ring-buffer drops in the eBPF capture: what is measured, what is not

Status: **investigation in progress.** The cause is NOT yet identified. This records what has
been measured, which hypotheses the measurements killed, what instrumentation was added to
make the remaining question answerable, and the approach proposed once it is.

Written 2026-07-31.

---

## 1. The problem

The capture drops records from its kernel→userspace ring buffer under load. Observed on one
B3 phase-1 run (session `019fb90e`, REST, 2,058,368 orders in 45s):

| counter | value |
|---|---|
| `ringbuf_dropped` | **23,436** |
| `stream_gap_bytes` | 32,421,526 (32 MB) |
| `hold_overflow` | 254 |
| `matcher_evicted_unanswered` | 36,264 |
| `truncated_captures` | 0 |
| `acked_dropped`, `undelivered_at_exit` | 0 |

Every dropped record is a hole in a TCP stream the reassembler must then write off, which is
why 23,436 dropped records cost 32 MB of stream and produced 65,968 `capture_gaps` (3.2% of
orders) and a **tainted** session. The engine answered every order; the capture failed to
record 3.2% of the answers.

This is the failure the platform can least afford: it is indistinguishable, at grading time,
from a contestant that did not answer.

## 2. What has been measured

Four runs, all 45s, all on the same cluster, engine and commit. `events_decoded` is capture
records that reached userspace; `ringbuf_dropped` is records the kernel discarded because
userspace was not draining fast enough.

| run | shape | orders | events_decoded | ringbuf_dropped |
|---|---|---|---|---|
| `019fb90d` FIX | max-rate, 1 flow | 1,816,128 | 3,788,008 | **0** |
| `019fb90e` REST | max-rate, 1 flow | 2,058,368 | 4,112,553 | **23,436** |
| `019fb910` WS | max-rate, 1 flow | 2,006,656 | 4,186,482 | **0** |
| `019fb921` mixed3 | paced 3x15k/s, 3 flows | 2,025,024 | 4,231,524 | **0** |

## 3. Hypotheses the measurements killed

**Kafka backpressure — ruled out.** `producer_inflight` peaked at **24** during the dropping
run, against 20 on a clean run. If the drain loop were blocking on produce, this would be in
the thousands. `acked_dropped` and `undelivered_at_exit` were both 0.

**Average order rate — ruled out.** WS pushed 2,006,656 orders (2.5% below REST) through the
same capture with zero drops.

**Burstiness — ruled out.** The paced run was the obvious suspect for "smooth traffic does not
drop", but FIX and WS were equally bursty (max-rate, `avg_batch: 64`) and also dropped
nothing. Pacing is not the discriminator.

**Records per second — ruled out.** WS decoded 4,186,482 events and mixed3 decoded 4,231,524,
both MORE than REST's 4,112,553, and neither dropped anything.

So the one run that dropped is not distinguished from the three that did not by any of order
rate, record rate, burstiness, flow count, or Kafka pressure.

## 4. Two problems with the measurements themselves

These matter more than any conclusion above, because they undermine the data the conclusions
rest on.

### 4a. Counter totals are truncated by scrape timing

The capture runs as a **per-run Job** whose pod is reaped on completion. A ~50s lifetime
scraped every ~15s means whatever accumulates after the final scrape is simply lost.

This is not hypothetical. The REST run's own funnel is internally impossible:

```
xdp_packets 1,489,492 + tc_packets 2,506,275 - short_payload_skipped 990,264 = 3,005,503 records emitted
events_decoded                                                              = 4,112,553 records decoded
```

You cannot decode more records than were emitted. The gap is missing final scrapes, and it
means the cross-run totals in §2 are indicative, not authoritative.

**Fixed:** the capture now logs a `"eBPF capture FINAL counters"` line on shutdown carrying
every loss counter plus a freshly re-read `ringbuf_dropped` from the kernel map. Job logs
survive pod reaping; the time series is now only for shape, and the log is the authority.

### 4b. The host was not quiet — and the capture is Burstable

The capture container is deliberately **Burstable**: `requests: 200m CPU`,
`limits: 4 CPU` (`sandbox-orchestrator/internal/k8s/slot.go`, `captureResources`). Under node
pressure the scheduler can squeeze it toward its 200m request, and CFS will throttle it.

**This exact failure is already on record in that file's own comment:**

```go
// 4 (was 2): the userspace drain+parse+publish wants ~3 cores at >150k
// delivered; a 2-core cap CFS-throttled it -> ringbuf drops. Burstable
// (request stays 200m), so this is safe on a 4-vCPU node and lets the
// capture use its needed cores on the c6i.2xlarge (8 vCPU) sandbox node.
```

Two things follow. First, the userspace path is known to want **~3 cores at >150k records/s**
— so it is not lightly loaded, and a squeeze does not have to be severe to hurt. Second, a
CPU cap producing precisely these ring-buffer drops has been diagnosed here once already.

These runs executed on a local k3s single node — the same laptop that was concurrently running
multi-core Rust/Docker builds during parts of this session. That is exactly the node pressure
this container's QoS class exposes it to, on whichever run happened to overlap.

**This is now the leading hypothesis**, and it explains the otherwise inexplicable result in
§3: the run that dropped is not distinguished by anything about its traffic, because the
cause was not in its traffic. It is a genuine confound and it is not yet excluded.
**Any re-measurement must run on a quiet host**, or the result means nothing.

These runs executed on a local k3s single node — the same laptop that was concurrently
running multi-core Rust/Docker builds during parts of this session. A build pegging every
core would starve the capture's userspace loop and produce exactly this signature, on
whichever run happened to overlap.

This is a genuine confound and it is not currently excluded. **Any re-measurement must run on
a quiet host**, or the result means nothing.

## 5. What is still unknown, and the instrumentation added for it

The open question is where userspace time actually goes. It could not be answered at all:
the capture exported **no CPU metric**, and cAdvisor is not scraped on this cluster. The only
datapoint is crude — `/proc/1/stat` read by hand mid-run showed **73 CPU-seconds over a ~50s
process lifetime**, i.e. over a full core — with no attribution.

Per-process totals cannot answer it either, because everything after the ring buffer —
decode, reassembly, framing, matching — runs in **a single tokio task**. "One thread pinned at
100% while the process shows 1.4 cores" and "work spread evenly" are different problems with
different fixes, and a process total cannot tell them apart.

**Added** (`services/ebpf-latency/src/cpu.rs`):

- `iicpc_ebpf_thread_cpu_percent{thread="..."}` — per-thread CPU as a percentage of one core,
  sampled from `/proc/self/task/*/stat` on the stats cadence
- `iicpc_ebpf_process_cpu_percent` — total across threads
- both also written to the Job log, so a reaped pod does not take the answer with it
- `_SC_CLK_TCK` read via `sysconf` rather than assumed to be 100
- comm parsed after the last `)`, since a thread name may contain spaces and parentheses
- `iicpc_ebpf_cpu_throttled_periods` / `_usec` from the cgroup's `cpu.stat` (v2, falling back
  to v1), also logged — this is what separates "needs more CPU" from "was stopped by CFS
  while CPU was available", per §4b

The reading is now a two-way decision rather than a one-way guess:

| observation | meaning | fix |
|---|---|---|
| `throttled_periods` climbing during drops | held below its limit by CFS — §4b | raise the request/limit, or isolate the node; **not** a code change |
| one thread pinned ~100%, no throttling | the single-task pipeline is saturated | shard the pipeline |
| no thread saturated, no throttling | neither — look at the drain loop's wakeup pattern | investigate further before changing anything |

## 5b. RESULT: the drops were host CPU contention

Re-ran the same shape (max-rate REST, single flow) on a quiet host with the new telemetry.
Session `019fb93a`:

| | busy host (`019fb90e`) | quiet host (`019fb93a`) |
|---|---|---|
| orders | 2,058,368 | 1,838,528 |
| events decoded | 4,112,553 | **4,185,669** |
| `ringbuf_dropped` | **23,436** | **0** |
| `cpu_throttled_periods` | not measurable then | **0** |
| `capture_gaps` | 65,968 (3.2%), tainted | **0** |
| score | 0.98716 | **0.99650** |

The quiet run decoded MORE records than the dropping run and dropped none. Combined with
`producer_inflight` having ruled out Kafka, and with zero CFS throttling, the conclusion is:

> **The ring-buffer drops were caused by CPU starvation from other work on the node — not by
> a capture throughput limit, not by Kafka, and not by anything about the traffic.**

That also retires the mystery in §3: no traffic variable distinguished the dropping run
because the cause was never in its traffic.

### 5c. Three of the CPU measurements were wrong — all now fixed

The 970% reading was chased down and it was an artefact. So were two others. Recording them
because each is the same class of error this document exists to prevent.

**1. `process_cpu_percent` of 970% was a sampling artefact, not a measurement.** Sampling ran
every 10 ticks of a `tokio::time::interval`, whose default `MissedTickBehavior` is `Burst`:
under exactly the load worth measuring, the loop falls behind and then fires ticks
back-to-back, so the gap between samples collapses to ~1ms. Against `/proc` accounting
quantised to 10ms (`USER_HZ` 100) that yields `delta = 0` -> **0%**, or `delta = 1 tick` over
~1ms -> **~1000%**. Both were observed from the same healthy process minutes apart. **The
capture does NOT need ~10 cores; that number was noise** and it came within one step of being
used to argue for a bigger EKS node.
*Fixed:* samples spanning less than 250ms are discarded WITHOUT consuming the baseline, so
the next sample measures the full gap. Regression test:
`sub_interval_samples_are_suppressed_and_keep_their_baseline`.

**2. The throttling metric could only ever report zero.** It read
`/sys/fs/cgroup/cpu.stat`, but the capture Job runs `HostPID: true` with no private cgroup
mount, so that path is the host ROOT cgroup — observed reporting `usage_usec` of 119,500
seconds (~33 hours of CPU on a node up 34 hours) for a Job alive for seconds, with
`nr_throttled` permanently 0 because the root cgroup is never throttled.
*Fixed:* the path is resolved from `/proc/self/cgroup`
(`/kubepods.slice/.../cri-containerd-<id>.scope`).

**3. The "73 CPU-seconds over ~50s" figure quoted earlier was not the capture.** It came from
reading `/proc/1/stat` inside the container — and with `HostPID: true`, PID 1 is the host's
systemd. That number should be struck from any reasoning; it measured the wrong process
entirely.

None of this changes the §5b conclusion — that rests on `ringbuf_dropped`, `capture_gaps` and
`producer_inflight`, which were never in doubt. It does mean **no trustworthy CPU figure for
the capture exists yet**; the next loaded run with the fixed sampler produces the first one.

## 6. Proposed approach

**Step 1 — finish the measurement, on a quiet host.** Re-run the dropping scenario with no
builds or other load on the node, and read `cpu_throttled_periods` and the per-thread numbers
together. Everything below is contingent on what they say; committing to a fix now would
repeat the mistake this session already made twice (a confidently wrong root cause for the
WebSocket failure, then another for the connect timeouts).

Note the cheapest possible outcome is also the most likely one right now: if throttling
explains it, the fix is a CPU **request** change on a Burstable container, not a line of
capture code. Do not start sharding a pipeline to solve a scheduling problem.

**Step 2 — fix what the measurement names.** In the order I would expect to matter:

- **Raise the capture's CPU request** (currently 200m against a known ~3-core appetite) so it
  is not squeezable under node pressure, or give it Guaranteed QoS on the sandbox node. This
  is first because it is the leading hypothesis and by far the cheapest fix.

- **Shard the pipeline per flow.** Decode, reassembly, framing and matching are one tokio
  task today. Reassembly is already keyed by `(FlowKey, Direction)`, so per-flow sharding is
  natural: hash the flow to one of N workers, each owning its reassemblers. The matcher is
  keyed by ClOrdID and would need either a shared concurrent map or a merge stage. This is
  the fix if one thread is pinned.
- **Grow the ring buffer.** 64MB is ~43k records, roughly 30ms of headroom at 500k records/s.
  Cheap, independent of the above, and buys margin for bursts regardless of cause. It treats
  the symptom, so it should follow rather than replace the diagnosis.
- **Cut per-record cost in the matcher.** `HashMap<String, Inflight>` keyed on ClOrdID means
  millions of live entries with per-entry String allocation at target rates. An interned or
  fixed-width key removes allocation from the hot path.
- **Parse in the kernel.** Emit fixed-size records (ClOrdID + timestamps, ~64 bytes) instead
  of copying up to 1536 bytes per packet for userspace to re-parse. This removes both the
  copy and the userspace framing cost, and would dissolve the `CAPTURE_CAP` question that
  currently forces MTU 1500 (see the capture-cap plan). Largest payoff, largest effort.

**Step 3 — re-run the gate.** `capture_gaps` on a max-rate single-protocol run is the
acceptance criterion; it must be 0, not merely under the 1% taint threshold.

## 7. Open questions

- ~~Does the sandbox slot impose a cgroup CPU limit on the capture container?~~ **Answered:**
  requests 200m / limits 4, i.e. Burstable and squeezable — see §4b. Whether it was actually
  throttled during the dropping run is now measurable but not yet measured.
- Is the 5s matcher idle-eviction window contributing? `matcher_evicted_unanswered` was 36,264
  on the dropping run, but that is expected downstream of dropped responses rather than an
  independent cause.
- What actually differs about the REST run? No measured variable explains it, and §4b means
  the honest answer may be "nothing — it was the run that happened to share the host with a
  build".
