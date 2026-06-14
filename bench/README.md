# `bench/` — platform self-benchmark (~2M orders/s, every service on)

Drives the platform to its generation ceiling and asserts **every** service survives
losslessly — the honest test behind "we generate 2M/s." Distinct from `e2e/` (which
proves correctness + latency at ≤500k/s); `bench/` proves the *scaling tiers* hold.

## What's different from e2e
| tier | e2e | bench |
|---|---|---|
| botworker | 2 nodes | **3 nodes** (~2.4M/s generation) |
| Kafka | 1 broker, gp3-250 | **2-broker KRaft**, dedicated pool, gp3-500 PVCs |
| topics | 24 part | **96 part, RF=1** (transient bench data) |
| ingester | 2 | **8** |
| nodes / vCPU | 5 / 32 | **8 / 44** (needs quota bump) |

Why 2 brokers + RF=1 (not 3 + RF=3): at 2M/s telemetry is ~700 MB/s at RF=1; gp3 PVCs
(500 MB/s each) cover the disk, and 2 brokers split the ~11 Gbps in+out network. RF=1
is right for transient benchmark data (no replication amplification). `kafka-bench.sh`
takes `KBROKERS=3`/`DATA_RF=2` if you want headroom/durability.

## Files
| file | what |
|---|---|
| `bench.tfvars` | 9-node topology (adds the 3-node `kafka` pool + 3rd botworker) |
| `cluster.md` | terraform + **vCPU quota bump** + run order (YOU run terraform) |
| `kafka-bench.sh` | swaps in the 3-broker Kafka tier + 96-part RF=3 topics + scales bot/ingester |

The platform base, images, submission, scenarios, run + assert are **shared with `e2e/`**
(`e2e/01-images.sh`, `02-bootstrap.sh`, `03-submission.sh`, `run.sh`, `assert.sh`).

## Run order
See `bench/cluster.md`. Summary: quota bump → `terraform apply -var-file=bench/bench.tfvars`
→ `e2e/01-images` → `e2e/02-bootstrap` → `bench/kafka-bench.sh` → `e2e/03-submission`
→ drive load → assert lossless across every service.

## What "lossless" must hold at 2M/s (the bench assertions)
- producer: `telemetry_events_dropped_total == 0`, no worker OOM/restart.
- Kafka: balanced `BytesInPerSec` across the 3 brokers, no under-replicated partitions.
- ingester (x8): keeps up, consumer lag bounded, no drops.
- validator: bounded memory (streaming), completes; no OOM.
- storage / score-computer / control-plane: no restarts, bounded lag.

## Contestant choice
- **drain** (no responses): isolates pure platform send+telemetry capacity at 2M/s —
  the cleanest "can the pipeline absorb what we generate" test. (No acks → eBPF/validator
  idle; use this to validate Kafka + ingester scaling first.)
- **echo** (responds): adds acks → exercises eBPF + validator at rate, but the echo's own
  ACK capacity becomes a ceiling; verify it sustains the target before trusting the number.

## Open items before a clean 2M/s run
- **eBPF capture throughput at 2M/s** is unverified (one capture producing ~2M acks/s).
- **Contestant (echo) capacity** at 2M/s is unverified.
- These don't block the **drain** variant (send + telemetry + ingester capacity), which is
  the first thing to validate now that Kafka is multi-broker.
