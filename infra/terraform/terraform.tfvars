# Throughput-benchmark topology. Isolates the three planes onto dedicated node
# groups so the bot-worker scaling sweep (1 -> 2 load-gen nodes) measures
# load-generator capacity, NOT data-tier or contestant contention.
# Requires a PAID AWS account (Free plan caps instances at 2 vCPU). See
# DEPLOYMENT_EKS.md "Free-tier note".
region       = "us-east-1"
cluster_name = "iicpc-prod"

# everything-else / MEASUREMENT plane (fixed): data tier, Kafka, APIs,
# controllers, build, telemetry-ingester x2 + rollup, validator, score-computer,
# observability, KEDA. TWO m6i.2xlarge (8 vCPU / 32 GB each = 16 vCPU total) so
# the measurement plane has real headroom and is never the bottleneck during the
# sweep — and so the data tier + telemetry pipeline + observability spread across
# nodes instead of contending. Pinned at 2 (min=max=desired) so the ONLY variable
# in the sweep is botworker node count.
general_instance_type = "m6i.2xlarge"
general_min_size      = 2
general_max_size      = 2
general_desired_size  = 2

# contestant / SYSTEM-UNDER-TEST (fixed, isolated): one algo pod + its eBPF
# capture Job. c6i.2xlarge = 8 vCPU: 2 reserved for system, the algo gets an
# exclusive 2-core cpuset (ALGO_CPU=2, Guaranteed QoS), the rest run the Burstable
# capture Job + OS. 8 vCPU is the clean floor for that split.
sandbox_instance_type = "c6i.2xlarge"
sandbox_min_size      = 1
sandbox_max_size      = 1
sandbox_desired_size  = 1

# bot-fleet LOAD GENERATORS — the scaling variable. c6i.xlarge = 4 vCPU,
# compute-optimized. Start the sweep at 1 node; for the second data point bump
# botworker_desired_size to 2 (botworker_max_size already allows it):
#   terraform apply -var botworker_desired_size=2
botworker_instance_type = "c6i.xlarge"
botworker_min_size      = 1
botworker_max_size      = 2
botworker_desired_size  = 1

# account_id resolves automatically from the caller identity.
