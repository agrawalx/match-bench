# Contest tier, BASELINE shape (docs/eks-contest-deployment.md):
# 2 general + 2 kafka + 1 sandbox + 2 botworker ≈ $2.2/h.
# Scale to contest-day with tfvars/contest-day.tfvars (sandbox 1 -> 4).
#
# Quotas (precheck.sh validates): x86 on-demand >= 64 vCPU, Graviton (G) >= 96.

region       = "us-east-1"
cluster_name = "iicpc-contest"

general_instance_type = "m6i.2xlarge"
general_desired_size  = 2

kafka_instance_type = "m6i.xlarge"
kafka_desired_size  = 2

sandbox_instance_type = "c6i.2xlarge"
sandbox_desired_size  = 1

# botworker_max_size is a placeholder until measurement M1 fixes it:
# ceil(worst-case aggregate / per-pod TPS) + 1.
botworker_instance_type = "c7g.xlarge"
botworker_desired_size  = 2
botworker_max_size      = 8
