## Appendix A · Glossary

- **Run-group** — one "Run" click on a submission; expands into N child *sessions*
  (one per scenario), sharing a `run_group_id`.
- **Session** — one scenario's run against the contestant; the unit of correctness
  scoring and the `slot_id` for the sandbox. Identified by a UUIDv7 `session_id`
  (whose embedded millisecond timestamp bounds the validator's Kafka drain).
- **Scenario** — a load shape: `constant` (flat 50k/s), `spike` (50k→100k→50k),
  `ramp` (20k→100k). Each carries a full `TaskSpec` list.
- **TaskSpec / task** — one constant-rate sender = one tokio task = one TCP
  connection. The controller shards a scenario's task list across worker pods.
- **Slot** — one allocated sandbox: a Guaranteed-QoS algo pod + its Service +
  the co-located privileged eBPF capture Job. Slot id = session id.
- **Barrier** — the synchronized "go" epoch, computed by the controller *after*
  all workers report ready, so every worker fires at the same wall-clock instant.
- **Wave** — a fixed 20 s slice of a session's life; the bucketing unit for HDR
  histograms and the peak-sustained-TPS gate. Stabilized across ingester shards by
  the barrier epoch stamped on every order.
- **Co-partitioning** — `orders.sent` and `orders.acked` are both produced to the
  partition `FNV1a(order_id) % 24`, so an order's send and ack land on the same
  partition number → any consumer that owns partition *k* of both sees both legs
  locally, with no cross-replica shuffle. The linchpin of data-plane scale.
- **t3 / t7** — kernel timestamps stamped by the eBPF program: `t3` at XDP ingress
  (request enters the algo veth), `t7` at tc egress (response leaves). The scored
  **service time** is `t7 − t3`, in the monotonic clock domain (skew-invariant).
- **t0 / t1 / r9** — load-gen timestamps: `t0` = scheduled fire time (captured
  *before* the sleep, for coordinated-omission correctness), `t1` = actual send,
  `r9` = client-observed full response. `response_time = r9 − t0`,
  `schedule_slip = t1 − t0`.
- **Coordinated omission (CO)** — the measurement error where a slow system hides
  latency by letting the load generator wait. Avoided by open-loop pacing with `t0`
  fixed on the ideal schedule, so lateness shows up as growing `schedule_slip`.
- **HDR histogram** — High Dynamic Range histogram (1 ns…60 s, 3 sig figs); its
  buckets are fixed, which is what makes per-shard blobs **mergeable** by addition
  (the basis of the two-stage rollup).
- **Drain / echo / matching-engine** — the three benchmark contestants: *drain*
  (discards, never replies → pure generation capacity), *echo* (µs acker →
  measurement capacity), *matching-engine* (correct order book → qualifies for the
  real leaderboard).
