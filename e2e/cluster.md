# e2e cluster — bring-up / tear-down (YOU run these)

These are the cluster-lifecycle steps you run by hand (terraform apply/destroy is
not automated). Everything else (`01-images.sh` … `run.sh`) runs after the cluster is up.

All commands assume repo root and `AWS_PROFILE=iicpc`, `AWS_REGION=us-east-1`.
Requires a PAID AWS account (Free plan caps instances at 2 vCPU) with vCPU quota ≥ 20.

## 0. (If an old cluster is up) tear it down first

```bash
cd infra/terraform
AWS_PROFILE=iicpc terraform destroy
```

After destroy, clean up anything terraform doesn't own (only if present):
```bash
# orphaned EBS volumes from deleted PVCs (check before deleting):
AWS_PROFILE=iicpc aws ec2 describe-volumes --filters Name=status,Values=available \
  --query 'Volumes[].{id:VolumeId,size:Size}' --output table
```

## 1. Bring up the cluster (5 nodes)

The validated topology lives in `infra/terraform/terraform.tfvars` (auto-loaded), and
is mirrored in `e2e/e2e.tfvars`. Plain `terraform apply` uses terraform.tfvars:

```bash
cd infra/terraform
AWS_PROFILE=iicpc terraform init -upgrade        # first time / after module changes
AWS_PROFILE=iicpc terraform apply
```

Expect ~15 min. Node groups come up as: **general x3, sandbox x1, botworker x1** (5
nodes). The sandbox node group can sit in "Creating" for a few minutes — normal.

## 2. Point kubectl at it

```bash
AWS_PROFILE=iicpc aws eks update-kubeconfig --name iicpc-prod --region us-east-1
kubectl get nodes -L pool,role
# expect 5 Ready: 3 general (role=general), 1 sandbox (pool=sandbox), 1 botworker (pool=botworker)
kubectl get nodes -o custom-columns=NAME:.metadata.name,POOL:.metadata.labels.pool,TAINTS:.spec.taints
# sandbox + botworker must show their NoSchedule taints (isolation); general untainted
```

## 3. Hand back to the suite

Once `kubectl get nodes` shows all 5 Ready, run (from repo root):
```bash
e2e/01-images.sh       # build + push the 12 platform images + contestant-echo (tag = git SHA)
export TAG=<the tag 01-images printed>
e2e/02-bootstrap.sh    # FULL stack: 2-broker kafka + data + all services + eBPF + build-worker + frontend(auth off)
e2e/04-scenarios.sh    # seed constant / spike / ramp (the default modest scenarios)
# then EITHER the frontend flow (qualified) OR the terminal flow (throughput) — see README
e2e/03-submission.sh   # terminal flow: register the echo contestant
e2e/run.sh constant    # then: spike, ramp
e2e/assert.sh <session-id>
```

## Notes
- **Re-scaling a node group (e.g. botworker 1 → 2 for a scaling sweep):** terraform
  has `ignore_changes` on `desired_size` (so it won't fight autoscalers), so editing
  the tfvars does NOT change a running node group. Use the AWS CLI:
  ```bash
  NG=$(aws eks list-nodegroups --cluster-name iicpc-prod --region us-east-1 \
        --query "nodegroups[?starts_with(@,'botworker')]|[0]" --output text)
  aws eks update-nodegroup-config --cluster-name iicpc-prod --nodegroup-name "$NG" \
    --scaling-config minSize=1,maxSize=2,desiredSize=2 --region us-east-1
  ```
  (`max_size` in the tfvars already allows it.) The first/create apply DOES honor
  `desired_size`, so a fresh cluster comes up at the tfvars sizes.
- The Kafka gp3 **throughput** bump (125 → 500 MB/s, `deploy-bench/kafka-gp3-throughput.yaml`)
  is only needed for >150k/s stress runs; the default scenarios don't require it.
