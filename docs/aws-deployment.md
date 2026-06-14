# AWS Deployment Guide

This guide deploys match-bench to AWS EKS using Terraform, ECR, IRSA, KEDA, AWS Load Balancer Controller, gp3 storage, and the manifests under `k8s/`.

For lower-level infrastructure details, read `infra/README.md`.

## Prerequisites

- AWS CLI v2 with credentials configured
- Terraform >= 1.6
- kubectl
- Helm v3
- Docker
- Permission to create EKS, EC2 node groups, IAM roles, ECR repositories, and load balancers

## Configure Terraform

```bash
cd infra
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars` for region, cluster name, AWS account ID, and node sizes.

## Create Cluster And Addons

```bash
make init
make plan
cd terraform && terraform apply -target=module.eks && cd ..
make apply
make kubeconfig
```

The targeted first apply creates the EKS API before Terraform evaluates Kubernetes and Helm resources.

## Create Secrets

Do not apply placeholder templates directly. Use generated values or AWS Secrets Manager / SSM values. See `bootstrap/README.md`.

Minimum data-tier example:

```bash
kubectl create secret generic postgres-secret -n data --from-literal=password='<postgres-password>'
kubectl create secret generic timescaledb-secret -n data --from-literal=password='<timescaledb-password>'
kubectl create secret generic minio-secret -n data \
  --from-literal=access-key='<minio-access-key>' \
  --from-literal=secret-key='<minio-secret-key>'
kubectl create secret generic kafka-secret -n data --from-literal=cluster-id='<kafka-cluster-id>'
```

Create the remaining platform, build, benchmark, and observability secrets before deploying workloads.

## Build And Push Images

```bash
make images
```

Images are pushed to ECR with the current git SHA tag.

## Deploy Workloads

```bash
make deploy
```

The deploy target applies namespaces, network policies, data services, observability, platform APIs, build services, sandbox services, and benchmark services in dependency order. It also substitutes Kubernetes image references with ECR image URLs.

## Validate Deployment

```bash
kubectl get pods -A
kubectl -n data wait --for=condition=Ready pod -l app=kafka --timeout=420s
kubectl -n platform rollout status deployment/submission-api --timeout=300s
kubectl -n platform rollout status deployment/leaderboard-api --timeout=300s
kubectl -n benchmark rollout status deployment/score-computer --timeout=300s
make netpol-gate
```

`make netpol-gate` is required. It proves the sandbox namespace cannot reach protected in-cluster services while preserving allowed egress.

## Frontend Public Access

Create the frontend ALB Ingress with your ACM certificate ARN, following the details in `infra/README.md`.

```bash
kubectl get ingress frontend -n platform
```

## Optional gVisor

Terraform registers the `gvisor` RuntimeClass object. The sandbox nodes still need `runsc` installed before pods can use it.

After `runsc` is available on sandbox nodes:

```bash
kubectl -n sandbox set env deployment/sandbox-orchestrator RUNTIME_CLASS=gvisor
```

If gVisor is disabled, leave `RUNTIME_CLASS` empty and treat NetworkPolicy validation as a hard deployment gate.

## Teardown

```bash
cd infra
make destroy
```

Check for retained EBS volumes after teardown if PVC-backed storage was created.
