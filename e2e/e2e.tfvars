# e2e cluster topology — full end-to-end suite (contestant + eBPF + telemetry +
# validator), three isolated planes. Matches the validated config in
# infra/terraform/terraform.tfvars (this file just makes the e2e self-contained;
# `terraform apply` alone also works since terraform.tfvars auto-loads the same).
#
# 5 nodes total:
#   - general  x3 (m6i.xlarge, 4 vCPU / 16 GiB = 12 vCPU / 48 GiB): MEASUREMENT plane —
#       Kafka (2 brokers, anti-affinity one-per-node), Postgres, TimescaleDB, Redis,
#       MinIO, telemetry-ingester(+rollup), correctness-validator, controllers, APIs,
#       score-computer, build-spawner, observability, frontend. 3 nodes so the
#       measurement plane has CPU headroom (at 2 it pinned ~95% and starved telemetry).
#   - sandbox  x1 (c6i.xlarge, 4 vCPU): SYSTEM-UNDER-TEST — the contestant pod
#       (ALGO_CPU=2, ALGO_MEMORY=4Gi) + its privileged eBPF capture (filters :9898).
#   - botworker x1 (c6i.xlarge, 4 vCPU, max 2): LOAD GENERATOR. One node sustains
#       ~600k+/s (telemetry off). Bump botworker_desired_size to 2 for a horizontal-
#       scaling sweep (max_size already allows it).
#
# Apply: terraform apply   (or: terraform apply -var-file=<repo>/e2e/e2e.tfvars)
# NOTE: terraform IGNORES desired_size on UPDATES (ignore_changes, so it doesn't fight
# autoscalers). It IS honored on the first/create apply. To re-scale an existing
# nodegroup, use the AWS CLI (see cluster.md), not terraform.

region       = "us-east-1"
cluster_name = "iicpc-prod"

# Measurement plane — 3 nodes for CPU headroom.
general_instance_type = "m6i.xlarge"
general_min_size      = 2
general_max_size      = 3
general_desired_size  = 3

# Contestant / SUT — one node; contestant gets 2 vCPU, capture + system get the rest.
sandbox_instance_type        = "c6i.xlarge"
sandbox_min_size             = 1
sandbox_max_size             = 1
sandbox_desired_size         = 1
sandbox_reserved_system_cpus = "0,1"

# Load generator — one node by default (max 2 for a scaling sweep).
botworker_instance_type = "c6i.xlarge"
botworker_min_size      = 1
botworker_max_size      = 2
botworker_desired_size  = 1

# Kafka runs as 2 brokers ON the general pool (anti-affinity), not a dedicated pool.
kafka_desired_size = 0

# Gates: gVisor + sandbox cpuset + spawner IRSA off (cpuset conflicts with EKS
# kube-reserved; the e2e needs none of them).
enable_gvisor         = false
enable_sandbox_cpuset = false
enable_spawner_irsa   = false

# account_id resolves automatically from the caller identity.
