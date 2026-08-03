# Contest tier, BASELINE shape (docs/eks-contest-deployment.md):
# 2 general + 1 kafka + 1 sandbox + 2 botworker ≈ $2.0/h.
# Scale to contest-day with tfvars/contest-day.tfvars (sandbox 1 -> 4); b2 needs
# 2 sandbox nodes (one slot per node) — terraform cannot change desired_size,
# use `aws eks update-nodegroup-config` (see the bring-up guide).
#
# Quota (precheck.sh validates): ONE Standard on-demand vCPU quota
# (L-1216C47A) covers x86 AND Graviton — baseline needs 48.

region       = "us-east-1"
cluster_name = "iicpc-contest"

general_instance_type = "m6i.2xlarge"
general_desired_size  = 2

# 1 node = 1 broker: the eks-contest overlay pins the statefulset to a single
# broker, so a 2nd node was pure cost. Raise BOTH this and the overlay patch
# (replicas + quorum voters + internal-topic RFs) together if M3 shows one
# broker cannot hold the ramp target.
kafka_instance_type = "m6i.xlarge"
kafka_desired_size  = 1

sandbox_instance_type = "c6i.2xlarge"
sandbox_desired_size  = 1

# botworker_max_size is a placeholder until measurement M1 fixes it:
# ceil(worst-case aggregate / per-pod TPS) + 1.
botworker_instance_type = "c7g.xlarge"
botworker_desired_size  = 2
botworker_max_size      = 8
