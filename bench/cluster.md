# bench cluster — bring-up / tear-down (YOU run these)

The 2M/s platform self-benchmark cluster: 8 nodes, 44 vCPU (2 general + **2 kafka** +
1 sandbox + 3 botworker).

## 0. vCPU quota — DO THIS FIRST
44 vCPU exceeds the default 32 "Running On-Demand Standard instances" quota. Request
an increase to **≥44 (ask for 48)** before applying, or the node groups stall with
`VcpuLimitExceeded`:
```
AWS console → Service Quotas → Amazon EC2 → "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances" → request 48
```

## 1. Apply (from a clean state)
If the e2e cluster is up, destroy it first (different topology):
```bash
cd infra/terraform
AWS_PROFILE=iicpc terraform destroy -var-file="$(git rev-parse --show-toplevel)/e2e/e2e.tfvars"
```
Then bring up the bench cluster:
```bash
cd infra/terraform
AWS_PROFILE=iicpc terraform init -upgrade
AWS_PROFILE=iicpc terraform apply -var-file="$(git rev-parse --show-toplevel)/bench/bench.tfvars"
AWS_PROFILE=iicpc aws eks update-kubeconfig --name iicpc-prod --region us-east-1
kubectl get nodes -L pool   # expect 8: 2 general, 2 kafka, 1 sandbox, 3 botworker
```

## 2. Deploy platform + 2-broker Kafka
```bash
e2e/01-images.sh                       # build/push images (shared with e2e)
e2e/02-bootstrap.sh                    # platform base (deploys a single-broker Kafka)
bench/kafka-bench.sh                   # SWAP IN the 2-broker tier + 96-part RF=1 topics + scale
e2e/03-submission.sh                   # contestant (echo, or a drain for pure platform capacity)
```
`kafka-bench.sh` tears down the single-broker Kafka from `02-bootstrap` and replaces it
with the 2-broker KRaft cluster on the dedicated `kafka` pool, so run it AFTER
`02-bootstrap`. (Set `KBROKERS=3 DATA_RF=2` for headroom/durability.)

## 3. Tear down
```bash
cd infra/terraform
AWS_PROFILE=iicpc terraform destroy -var-file="$(git rev-parse --show-toplevel)/bench/bench.tfvars"
```
Then clean orphaned EBS volumes (the kafka PVCs are `Delete` reclaim, but check):
```bash
AWS_PROFILE=iicpc aws ec2 describe-volumes --filters Name=status,Values=available --output table
```

## Cost
~8 nodes ≈ **$2.4–2.7/hr** + EKS control plane + EBS. Tear down promptly after a run.
