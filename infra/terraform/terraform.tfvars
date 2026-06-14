# Optimal e2e topology (right-sized after profiling). Three planes on dedicated
# pools; load-gen workers and the contestant are isolated from the measurement
# plane so each is measured without cross-contention. Requires a PAID AWS account
# (Free plan caps instances at 2 vCPU).
#
# Measured needs that drove this sizing:
#   general  : ~24 GiB peak / ~8 vCPU once workers move off (kafka page-cache +
#              validator + data tier + platform + observability). 2x m6i.xlarge.
#   sandbox  : contestant 2 vCPU + eBPF capture + system. c6i.xlarge (4 vCPU).
#   botworker: bounded-in-flight worker (BOT_MAX_INFLIGHT_PER_TASK); one node
#              drives ~600k+/s (drain-verified). c6i.xlarge.
#   kafka    : single broker ON general (kafka_desired_size=0), not its own pool.
region       = "us-east-1"
cluster_name = "iicpc-prod"

# MEASUREMENT plane: data tier (kafka single broker, postgres, timescale, minio,
# redis), platform APIs, controllers, build-spawner, telemetry-ingester(+rollup),
# validator, score-computer, observability, KEDA. 3x m6i.xlarge (4 vCPU / 16 GiB
# each = 12 vCPU / 48 GiB). Bumped from 2 -> 3: at 2 nodes pod CPU REQUESTS hit
# ~95% (3.74/3.94 vCPU each), leaving no room for telemetry-ingester (req cpu 1) ->
# it went Pending. The 3rd node gives the telemetry plane a guaranteed core so it is
# not the bench's bottleneck. Scaling a managed node group is non-disruptive (no
# teardown). NOTE: kafka CPU request is trimmed 2->1 in its manifest.
general_instance_type = "m6i.xlarge"
general_min_size      = 2
general_max_size      = 3
general_desired_size  = 3

# CONTESTANT / system-under-test (isolated): one algo pod + its co-located eBPF
# capture Job. c6i.2xlarge = 8 vCPU. On the old c6i.xlarge (4 vCPU) the contestant
# (ALGO_CPU=2) + the capture (~3 cores) saturated the node, so a responding sink
# CFS-throttled at ~150-168k and spike/ramp collapsed past the knee. 8 vCPU gives
# contestant ALGO_CPU=4 + capture ~3 + kubelet/OS ~1 -> >180k delivered WITH the
# eBPF latency capture keeping up. (Changing instance_type RECREATES the managed
# node group: the c6i.xlarge sandbox node is drained and replaced by a c6i.2xlarge;
# the sandbox taint/label + gro-disable DaemonSet re-apply automatically.)
sandbox_instance_type        = "c6i.2xlarge"
sandbox_min_size             = 1
sandbox_max_size             = 1
sandbox_desired_size         = 1
sandbox_reserved_system_cpus = "0,1" # only used WHEN cpuset is enabled (currently off)
# Exclusive integer cpusets for the contestant (true core isolation). Kept OFF
# until the known NodeConfig conflict (reservedSystemCPUs vs EKS kube-reserved ->
# kubelet won't start) is fixed and validated. With it off, the contestant still
# gets 2 vCPU via Guaranteed QoS (quota), just not exclusively-pinned cores.
enable_sandbox_cpuset = false

# bot-fleet LOAD GENERATORS (dedicated, tainted botworker=true:NoSchedule).
# c6i.xlarge = 4 vCPU. The worker is now memory-bounded (BOT_MAX_INFLIGHT_PER_TASK)
# so it fits this 8 GiB node and never OOMs on a slow contestant. One node drives
# ~600k+/s. Bump desired to add a 2nd worker node for a worker_count>1 sweep.
botworker_instance_type = "c6i.xlarge"
botworker_min_size      = 1
botworker_max_size      = 2
botworker_desired_size  = 1

# Kafka runs as a single broker on the general pool (no dedicated kafka pool).
# kafka_desired_size defaults to 0; set to 3 only for the 2M/s bench suite.
kafka_desired_size = 0

# account_id resolves automatically from the caller identity.
