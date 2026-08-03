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
#
# M1 partial result (2026-08-03, drain-sink control, c7g.xlarge): one worker
# sustained 200,001/s exactly (paced, flat, 60s); at a 1M/s target it peaked
# 901k and decayed to ~460k; at 2M/s across two workers BOTH were OOMKilled.
# The OOM is specific to the no-ack control — with nothing responding, every
# order stays pending for RESPONSE_TIMEOUT, so the worker tracks rate x 5s live
# entries (5M at 1M/s) against a 6.6Gi node. A responding engine holds
# rate x actual latency instead (~100 entries), so this UNDERSTATES the real
# ceiling and botworker_max_size still cannot be fixed from it.
botworker_instance_type = "c7g.xlarge"
botworker_desired_size  = 2
botworker_max_size      = 8

# Annotate the build-spawner ServiceAccount with its IRSA role. MUST be true, but
# only on a SECOND apply: the SA is created by the platform manifests, which are
# applied after terraform, so the first apply has nothing to annotate (that is
# why the variable defaults to false). Left false on 2026-08-03 it cost a failed
# submission — the spawner's ECR CreateRepository fell through the whole AWS
# credential chain to EC2 IMDS, found no role, and the build never started.
#
# Bring-up order that works: terraform apply -> push images -> namespaces ->
# secrets -> kubectl apply -k -> terraform apply AGAIN (this flag takes effect)
# -> kubectl -n build rollout restart deploy/spawner (the web-identity env is
# injected at pod creation, so a running pod does not pick it up).
enable_spawner_irsa = true
