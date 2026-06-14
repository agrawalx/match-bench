# bench cluster topology — platform self-benchmark at ~2M orders/s, every service on.
#
# 8 nodes, 44 vCPU, four isolated planes:
#   - general   x2 (m6i.2xlarge, 8 vCPU/32 GB): measurement plane — Postgres, Timescale,
#       Redis, telemetry-ingester (scaled) + rollup, correctness-validator (streaming,
#       bounded), controllers, APIs, score-computer, observability.
#   - kafka     x2 (m6i.xlarge, 4 vCPU/16 GB): DEDICATED 2-broker KRaft, one broker per
#       node (podAntiAffinity), I/O-isolated. Telemetry at 2M/s ≈ 700 MB/s at RF=1;
#       split over 2 brokers = ~350 MB/s + ~5.6 Gbps net each (gp3-500 PVCs) → ample.
#       (RF=1 = no replication: correct for transient benchmark data. 2 brokers, not 3,
#       because the bandwidth is solved by gp3 + RF=1; the 2nd broker just spreads the
#       network. Quorum fault-tolerance is irrelevant for a re-runnable bench.)
#   - sandbox   x1 (c6i.2xlarge, 8 vCPU): contestant + eBPF capture.
#   - botworker x3 (c6i.xlarge, 4 vCPU): load generators — ~800k/s each → ~2.4M/s.
#
# Apply: terraform apply -var-file=<repo>/bench/bench.tfvars   (see bench/cluster.md)
#
# vCPU QUOTA: 44 vCPU total exceeds the default 32 "Running On-Demand Standard
# instances" quota — request an increase to >=44 (ask 48) BEFORE applying, or the
# node groups will stall with VcpuLimitExceeded. (Service Quotas → EC2.)

region       = "us-east-1"
cluster_name = "iicpc-prod"

general_instance_type = "m6i.2xlarge"
general_min_size      = 2
general_max_size      = 2
general_desired_size  = 2

# Dedicated 2-broker Kafka pool (the multi-broker tier for 2M/s telemetry writes).
kafka_instance_type = "m6i.xlarge"
kafka_min_size      = 2
kafka_max_size      = 2
kafka_desired_size  = 2

sandbox_instance_type = "c6i.2xlarge"
sandbox_min_size      = 1
sandbox_max_size      = 1
sandbox_desired_size  = 1

# 3 load-gen nodes for ~2.4M/s of generation headroom over the 2M/s target.
botworker_instance_type = "c6i.xlarge"
botworker_min_size      = 3
botworker_max_size      = 3
botworker_desired_size  = 3

enable_gvisor         = false
enable_sandbox_cpuset = false
enable_spawner_irsa   = false
