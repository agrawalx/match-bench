# EKS bring-up handoff — state, order of operations, goals

Written 2026-08-02, mid-bring-up. Self-contained: a fresh session executes from
here. Design authority: `docs/eks-contest-deployment.md`. Runbook history:
`docs/remaining-work.md`.

## 0. Where things stand RIGHT NOW

- **Cluster LIVE**: `iicpc-contest`, us-east-1, 7 nodes Ready (2 general
  m6i.2xlarge, 2 kafka m6i.xlarge, 1 sandbox c6i.2xlarge, 2 botworker
  c7g.xlarge arm64). ~$2/h is burning. kubeconfig context:
  `arn:aws:eks:us-east-1:885232248981:cluster/iicpc-contest` (local k3s is a
  separate context — `kubectl config use-context` to switch; EVERY harness run
  must be against the intended context).
- **AWS**: profile `iicpc` ONLY (`export AWS_PROFILE=iicpc` — the default
  profile is a different user in ap-south-1). Standard vCPU quota 64 (baseline
  needs 40). GPU-quota case 178569075700732 is a mistake to close in console.
- **State**: S3 backend `iicpc-tf-state-885232248981` + `iicpc-tf-lock`
  (DynamoDB). 14 ECR repos exist (12 services + drain-sink/stall-sink
  fixtures).
- **In flight when this was written**: `push-images.sh` running (background) —
  11 amd64 pushes + bot-fleet dual-arch (its QEMU cache is fully warmed;
  push = cache-hit + upload). On completion it sed-stamps the git-sha tag into
  `overlays/eks-contest/kustomization.yaml`.
- **Everything is committed** on `feat/bot-tps` except the tag stamp the push
  script writes (commit it when the push finishes).

## 1. Decisions that shape everything below (do not relitigate)

- Contestant images exist ONLY as build-pipeline products (zip → build-worker →
  Kaniko → ECR). The b-harness on EKS submits real zips via `eks_submission`
  (lib-local.sh): book = `deploy-local/reference-clob-all.zip`, acker =
  `deploy-local/smoke-rest-echo.zip`. drain-sink/stall-sink are platform
  FIXTURE images, pre-pushed like services.
- Benchmark triggering in the b-scripts stays harness-driven (direct
  runs/run_groups + kafka publish, one scenario at a time). The real
  `StartBenchmark` 4-scenario sequential group is exercised deliberately in M5.
- AUTH_REQUIRED=false everywhere on this account; auth is the contest-day
  account's concern.
- All node counts are provisional until measured. M1's per-pod TPS number sizes
  the botworker pool to "serve 4 concurrent runs max".
- Engines: correct book (responder), acker (disqualify side), drain-sink
  (loadgen ceiling), stall-sink (b4). The echo-engine perf fix is REJECTED.

## 2. Order of operations from here

### Phase A — deploy the platform (~30 min)

1. Wait for `push-images.sh` to finish; `git add overlays/ && git commit`
   (the stamped immutable tag).
2. `kubectl apply -k overlays/eks-contest` — includes topic-init (status topic
   born with 4 partitions), gro-disable DaemonSet, KEDA ScaledObjects
   (bot-fleet-worker, correctness-validator), the suspended results-export Job.
3. Wait for pods: `kubectl get pods -A | grep -vE "Running|Completed"` empties.
   Kafka topic-init Job must show Completed.
4. **Port-forwards up (standing)**:
   `kubectl -n observability port-forward svc/grafana 3000:3000 &`
   (+ prometheus 9090 if wanted). Watch dashboards from the first run onward.

### Phase B — trust gates (order matters; each is cheap, stop on failure)

5. gro-disable coverage: pods == sandbox nodes (b2 preflight also asserts it).
6. NetworkPolicy smoke: a cross-namespace probe that must be DENIED (no gVisor
   — netpols are THE isolation boundary).
7. First capture Job load = the BPF verifier proof on the EKS kernel
   (AL2023/6.1). Happens implicitly with the first session; check capture pod
   logs for clean program load, no verifier rejection.
8. **b2-EKS** — the main gate:
   `HARNESS_ENV=eks deploy-local/b2-two-pass.sh`
   - Stage 1 is the BUILD PIPELINE's first end-to-end run ever (upload zips,
     Kaniko builds via spawner IRSA, ECR repos auto-created, status → ready;
     15-min timeout per submission in `eks_submission`).
   - Stage 2: the 11 correctness assertions (book qualifies ~0.99, acker
     disqualified) + capture FINAL counters at MTU 9001:
     **capture_gaps=0, ringbuf_dropped=0, cpu_throttled_periods=0** — the
     jumbo-frame regime's first live proof.

### Phase C — the rest of the suite (each ~10–20 min)

9.  `b2-pass2.sh` (invariants mode on constant).
10. `b4-stalled-peer.sh` (stall-sink fixture image; wedge invariants over real
    network buffers).
11. `b3-mixed3.sh` + `b3-mixed-protocol.sh` (three protocols over the VPC CNI).
12. `b5-autoscale-shards.sh` (KEDA workers + cluster-autoscaler adding real
    Graviton nodes — first live test of the 1-pod-per-node model).
13. b1 LAST and currently BLOCKED on a known issue: with auth off, both
    submissions share DEFAULT_CONTESTANT_ID, so b1's cross-contestant
    isolation assertions need distinct identities. Decide the mechanism
    (header the API accepts, or two default ids) BEFORE wiring — do not
    improvise it mid-run.

### Phase D — measurements (the goals; Grafana open throughout)

14. **M1 — per-pod worker TPS on Graviton** (THE sizing number): drain-sink on
    the sandbox node, ONE worker, max-rate (`run-sentps` pattern,
    `seed-drain-submission.sh` adapted refs). Output:
    `botworker_max = ceil(4-concurrent-run worst case ÷ M1) + 1` → update
    tfvars + docs; the arbitrary "8" dies here.
15. **M2 — capture ceiling live**: book engine, offered rate stepped up until
    FINAL counters degrade or the engine saturates; per-thread CPU split
    (`iicpc-ebpf-late` vs `rdk:*`) says which half matters. Target: confirm
    ≥1M records/s (criterion predicts 2.7M/core FIX).
16. **M3 — telemetry at peak** (same runs): flushed/s vs consumed/s, ingester
    lag → ingester replica count.
17. **M4 — validator throughput**: wall-time of one full-replay (1.8M orders)
    at 2 CPU + one invariants ramp session → sessions/hour vs group arrival.
18. **M5 — full rehearsal**: 4 concurrent run groups via the REAL
    StartBenchmark path (sequential-within-group dispatch, KEDA validator
    scale-out, leaderboard under load) + results-export Job dry run. This is
    the contest go/no-go.

### Phase E — wrap

19. W calibration (B6) if time permits: reference vs naive engine jitter
    distributions in the 9001 regime → `CROSS_FLOW_WINDOW_US`.
20. Record every measured number in `docs/eks-contest-deployment.md` (replace
    "UNMEASURED" markers) + remaining-work.
21. **Teardown**: unsuspend results-export Job → verify
    `s3://iicpc-contest-results-885232248981/<stamp>/EXPORT-COMPLETE` →
    `terraform state rm aws_s3_bucket.results` → `terraform destroy
    -var-file=tfvars/contest.tfvars`. Verify $0: no nodes, no NAT, no ELB left.

## 3. Known traps (each has bitten once already)

- Wrong kubectl context (k3s vs EKS) — check before every harness run.
- Wrong AWS profile (default = crawler-dev/ap-south-1) — every shell.
- `psql_val` dies silently under pipefail on empty results — a scenario
  missing from the DB looks like a script bug (b2 preflight prints the reseed
  hint only because of a guard added after this bit us).
- Scenario seed on EKS is `correctness,ramp,spike,constant` (overlay); local
  is `correctness,constant`. `SeedScenarios` upserts only — stale rows need
  manual SQL deletes.
- ECR tags are IMMUTABLE: new code = new tag (git sha); re-pushing a tag fails
  by design.
- kustomize base regenerating: `k8s/kustomization.yaml` lists files explicitly
  — a NEW manifest file must be added there or it silently doesn't deploy.
- The capture's FINAL counters line lives in the capture Job's logs and the
  pod is reaped — read it promptly or lose it (the doc's §4a lesson).

## 4. Success criteria for this cluster's first lifetime

1. Build pipeline proven (zip → ready, twice).
2. b2–b5 green on EKS; b1 after the identity decision.
3. Jumbo regime proven: FINAL counters clean at MTU 9001 under load.
4. M1 number in hand → botworker sizing derived, not guessed.
5. M2/M3 confirm capture + telemetry hold at ramp-scale rates.
6. M5 rehearsal: 4 groups, real dispatch, export job works.
7. Clean destroy with results in S3.

That is the complete path from "7 idle nodes" to "contest-ready platform with
measured capacity claims".
