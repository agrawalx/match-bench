# EKS bring-up — complete guide

Rewritten 2026-08-03 after the first bring-up (cluster built, validated, torn
down). Self-contained: follow top to bottom. Every step here is either proven or
explicitly marked as unresolved.

- Design authority: `docs/eks-contest-deployment.md`
- What happened last time, and why: `docs/eks-bringup-findings.md`
- Backlog: `docs/remaining-work.md`

**Everything discovered in the first bring-up is committed** (13 fixes, tree
clean as of `d6bce55`). This guide folds them in as ordinary steps, so a fresh
run should not rediscover them.

---

## 0. Rules that prevent the mistakes that cost hours last time

1. **`export AWS_PROFILE=iicpc` in every shell.** The default profile is a
   different user in ap-south-1.
2. **Check your kubectl context before every harness run.** Local k3s and EKS
   are both in kubeconfig; `kubectl config current-context`.
3. **Never `grep -c` the output of `kubectl apply`.** It hides rejected
   resources. Read the errors, or `grep -iE "error|invalid"`.
4. **Never machine-edit YAML with line-oriented scripts.** Two attempts
   corrupted NetworkPolicies. Hand-edit, then apply and read the result.
5. **Verify against live objects.** A dial test against a deleted pod produces
   a confident, meaningless answer.

## 1. One-time per AWS account

```bash
export AWS_PROFILE=iicpc

# State backend (already exists on account 885232248981 — skip if present)
aws s3 mb s3://iicpc-tf-state-885232248981 --region us-east-1
aws dynamodb create-table --table-name iicpc-tf-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-1

cd infra/terraform-v2
cp backend.hcl.example backend.hcl     # fill in bucket name; gitignored

# Quota: ONE Standard quota covers x86 AND Graviton (c7g is C-family).
# L-DB2E81BA "G and VT" is GPU instances — do NOT request it.
./precheck.sh                          # baseline needs 48; contest-day STD_NEED=128

# Multi-arch build support (bot-fleet is amd64+arm64)
docker run --privileged --rm tonistiigi/binfmt --install arm64
docker buildx create --name multiarch --driver docker-container
```

## 2. Bring up the cluster (~20 min, ~$2/h starts here)

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2
terraform init -backend-config=backend.hcl
terraform apply -var-file=tfvars/contest.tfvars
aws eks update-kubeconfig --name iicpc-contest --region us-east-1
kubectl get nodes -L pool          # expect 2 general, 2 kafka, 1 sandbox, 2 botworker
```

**b2 runs two contestants concurrently and needs TWO sandbox nodes** (one slot
per node by design). Terraform does NOT reconcile node-group `desired_size`
(the module defers to autoscalers), so scale via the EKS API:

```bash
NG=$(aws eks list-nodegroups --cluster-name iicpc-contest --region us-east-1 \
     --query 'nodegroups' --output text | tr '\t' '\n' | grep sandbox)
aws eks update-nodegroup-config --cluster-name iicpc-contest --nodegroup-name "$NG" \
  --scaling-config minSize=1,maxSize=4,desiredSize=2 --region us-east-1
```

## 3. Images

```bash
./infra/terraform-v2/push-images.sh    # 13 amd64 + bot-fleet multi-arch; stamps the overlay tag
git add overlays/ && git commit -m "chore(eks): stamp image tag <sha>"
```

Note: `overlays/eks-contest/kustomization.yaml` may still pin
`bot-fleet-controller` to a one-off tag (`45364d4-t180`) from the first
bring-up. A fresh push supersedes it — make sure the stamp applied to every
image, including that one.

## 4. Secrets — MUST run before the overlay

Nothing in the manifest tree creates application Secrets (correct — secrets
don't belong in git). Skipping this is what left 20 pods in
`CreateContainerConfigError` last time.

```bash
export AWS_PROFILE=iicpc
./deploy-local/create-secrets.sh       # idempotent; works against any context
```

## 5. Deploy the platform

```bash
kubectl apply -k overlays/eks-contest 2>&1 | grep -iE "error|invalid"   # MUST print nothing
kubectl get pods -A | grep -vE "Running|Completed"                      # MUST be empty
kubectl -n data wait --for=condition=complete job/kafka-topic-init --timeout=300s
```

If Kafka PVCs are ever wiped (voter-set changes require it), **re-run the
topic-init Job** — auto-create is off, and every publish silently goes nowhere
without topics:

```bash
kubectl -n data delete job kafka-topic-init && kubectl apply -k overlays/eks-contest
```

Port-forwards (detached so they survive):

```bash
setsid nohup kubectl -n observability port-forward svc/grafana 3000:3000 >/dev/null 2>&1 &
setsid nohup kubectl -n platform port-forward svc/frontend 3001:8080  >/dev/null 2>&1 &
```

## 6. Gates, in order — stop on the first failure

1. **gro-disable coverage**: pods == sandbox nodes (b2's preflight asserts it).
2. **Verifier**: the first capture Job must load its BPF programs on the EKS
   kernel (AL2023/6.1). Check capture pod logs for a clean attach; XDP falling
   back to skb mode is expected and fine.
3. **b2** — the platform gate. Contestants go through the REAL submission path
   (zip → Kaniko → ECR → `ready`), which is itself the build-pipeline test:

```bash
export AWS_PROFILE=iicpc
HARNESS_ENV=eks deploy-local/b2-two-pass.sh
```

Expect: book qualifies (~0.994), echo disqualified (~0.045), `capture_gaps=0`,
11/11. Reference numbers from local k3s with the same images.

4. Then `b2-pass2.sh`, `b4-stalled-peer.sh`, `b3-mixed3.sh`,
   `b3-mixed-protocol.sh`, `b5-autoscale-shards.sh` — all with
   `HARNESS_ENV=eks`. b1 is blocked on a decision: with auth off both
   submissions share `DEFAULT_CONTESTANT_ID`, so its cross-contestant isolation
   assertions cannot pass as written.

## 7. Open problems — expect these, and diagnose them THIS way

Both were seen on EKS and **do not reproduce on local k3s even with all 12
NetworkPolicies enforced** (verified 2026-08-03), so they are environment- or
VPC-CNI-specific, not platform logic.

**a) `orders.acked` stays 0 / no graphs.** While a session is RUNNING (the
capture Job is reaped at session end — you get one window):

```bash
P=$(kubectl -n sandbox get pods --no-headers | grep capture | grep Running | awk '{print $1}' | head -1)
kubectl -n sandbox port-forward pod/$P 29090:9090 >/dev/null 2>&1 &
curl -s localhost:29090/metrics | grep -E "events_decoded|ringbuf_dropped|acked_dropped|tc_packets|throttled"
```

- `events_decoded` climbing, `acked_dropped` > 0 → capture sees traffic but the
  Kafka publish is dropping batches (producer QueueFull). Prime suspect.
- `events_decoded` ~0 → the capture is not seeing packets: wrong netns
  (`EBPF_ALGO_POD_UID` / container-id resolution) or packets dropped before the
  hook.
- `ringbuf_dropped` > 0 or `throttled` > 0 → CPU starvation (all clean locally).

**b) ProtocolAll slots never become ready.** `slot.go` TCP-dials the extra port
before declaring ready and *before* creating the capture, so a failed dial
looks like "no capture, stuck in deploying". With a LIVE algo pod:

```bash
IP=$(kubectl -n sandbox get pod <algo-pod> -o jsonpath='{.status.podIP}')
O=$(kubectl -n sandbox get pod -l app=sandbox-orchestrator -o jsonpath='{.items[0].metadata.name}')
kubectl -n sandbox debug $O --image=busybox:1.36 --target=sandbox-orchestrator -q --attach=false \
  -- sh -c "nc -w 5 -zv $IP 8080; echo RC=\$?"
# then read the ephemeral container's logs
```

**Fallback if either blocks progress**: the previous working EKS runs
(`e2e/02-bootstrap.sh`) applied only the build namespace's policy. Deleting the
others reproduces that known-good config and unblocks measurement work — but
policies are the only isolation boundary now that gVisor is off, so this is
acceptable only while contestants are our own engines.

**c) Capture MTU**: the capture still logs `clamped 9001 -> 1500`, so the jumbo
regime is not actually in effect despite `CAPTURE_CAP=9029`. Something passes
`CAPTURE_CLAMP_MTU=1500` (job template or `captureJobSpec`).

## 8. Measurements (only after gates are green)

M1 per-pod Graviton loadgen TPS (fixes `botworker_max`), M2 capture ceiling,
M3 telemetry at ramp peak, M4 validator sessions/hour, M5 four-group rehearsal.
Details in `docs/eks-contest-deployment.md` §6b.

## 9. Teardown — and the two traps that leave money running

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2

# 1. Helm releases in state HANG destroy (KEDA's uninstall times out against a
#    dying API server) and terraform still exits 0 with nodes running.
terraform state list | grep helm_release | xargs -r -n1 terraform state rm

# 2. The results bucket has prevent_destroy; release it (export first if the
#    run produced anything worth keeping).
terraform state rm aws_s3_bucket.results aws_s3_bucket_versioning.results \
  aws_s3_bucket_public_access_block.results aws_s3_bucket_lifecycle_configuration.results

terraform destroy -var-file=tfvars/contest.tfvars
```

Then clean what terraform does not own — **PVC-backed EBS volumes survive the
cluster** (~$11/month if forgotten), and build-worker creates ECR repos per
submission at runtime:

```bash
aws ec2 describe-volumes --region us-east-1 --query 'Volumes[?State==`available`].VolumeId' --output text \
  | tr '\t' '\n' | xargs -r -n1 -I{} aws ec2 delete-volume --volume-id {} --region us-east-1
aws ecr describe-repositories --region us-east-1 --query 'repositories[].repositoryName' --output text \
  | tr '\t' '\n' | grep -E "iicpc/[0-9a-f-]{20,}" \
  | xargs -r -n1 -I{} aws ecr delete-repository --repository-name {} --force --region us-east-1
```

Final sweep — every count must be 0:

```bash
for q in "eks list-clusters --query length(clusters)" ; do :; done
aws eks list-clusters --region us-east-1 --query 'length(clusters)' --output text
aws ec2 describe-instances --region us-east-1 --filters Name=instance-state-name,Values=running,pending --query 'length(Reservations[].Instances[])' --output text
aws ec2 describe-volumes --region us-east-1 --query 'length(Volumes)' --output text
aws ec2 describe-nat-gateways --region us-east-1 --filter Name=state,Values=available --query 'length(NatGateways)' --output text
aws ecr describe-repositories --region us-east-1 --query 'length(repositories)' --output text
aws elbv2 describe-load-balancers --region us-east-1 --query 'length(LoadBalancers)' --output text
```

## 10. Known environment differences to keep in mind

Local k3s is NOT a faithful preview, and each gap has bitten:

| | local k3s | EKS |
|---|---|---|
| Secrets | `up-dev.sh` creates them | `create-secrets.sh` (step 4) |
| NetworkPolicies | `applynp()` skips them; enforcement real if applied | applied by the overlay; VPC CNI semantics differ |
| Images | pre-imported into containerd; no cold pulls | first pull per submission — why timeouts are 180s/300s |
| Nodes | one untainted node | tainted pools; `SANDBOX_NODE_POOL` matters |
| Validator resources | 2 CPU/2Gi starves a laptop (use ~500m/768Mi req) | 2 CPU/2Gi correct |

The highest-value follow-up remains making local run the same tree through the
same overlay mechanism, so these stop being discovered on a paid cluster.
