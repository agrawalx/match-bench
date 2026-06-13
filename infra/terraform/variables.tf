# infra/terraform/variables.tf
#
# This Terraform file declares operator-tunable inputs for the AWS deployment.
# It belongs to the IICPC AWS infrastructure layer and should remain
# aligned with infra/README.md and the Kubernetes manifests under k8s/.
# Keep explanatory comments at this file header so resource blocks stay declarative.

variable "region" {
  description = "AWS region for the cluster and ECR repos."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "EKS cluster name. Mirrors $CLUSTER in DEPLOYMENT_EKS.md."
  type        = string
  default     = "iicpc-prod"
}

variable "kubernetes_version" {
  description = "EKS control-plane minor version."
  type        = string
  default     = "1.32"
}

variable "account_id" {
  description = <<-EOT
    AWS account id. Used to construct ECR repository URLs in outputs and to scope
    IAM policy ARNs. Leave empty to resolve it from the caller identity at apply
    time (aws_caller_identity data source).
  EOT
  type        = string
  default     = ""
}


variable "vpc_cidr" {
  description = "CIDR for the VPC created for the cluster."
  type        = string
  default     = "10.42.0.0/16"
}

variable "az_count" {
  description = <<-EOT
    Number of Availability Zones to spread the cluster across. 3 is the default
    so Kafka's 3 KRaft brokers can land in distinct AZs (DEPLOYMENT_EKS.md §3.2).
    EBS is AZ-bound, so each stateful pod is pinned to its volume's AZ.
  EOT
  type        = number
  default     = 3
}


variable "general_instance_type" {
  description = "Instance type for the general node group."
  type        = string
  default     = "m6i.xlarge"
}

variable "general_min_size" {
  type    = number
  default = 2
}

variable "general_max_size" {
  type    = number
  default = 4
}

variable "general_desired_size" {
  type    = number
  default = 2
}

variable "general_disk_size" {
  description = "gp3 root volume (GiB) for general nodes."
  type        = number
  default     = 100
}


variable "sandbox_instance_type" {
  description = <<-EOT
    Instance type for the tainted sandbox node group. MUST be 8 vCPU: cores 0,1
    are reserved for the system (reservedSystemCPUs below), the algo pod is pinned
    to an integer cpuset (ALGO_CPU=2, Guaranteed QoS), and the capture Job runs on
    the remaining Burstable cores. Changing this requires re-deriving ALGO_CPU.
  EOT
  type        = string
  default     = "c6i.2xlarge"
}

variable "sandbox_min_size" {
  description = "Scale-to-zero floor. 0 in prod (no idle sandbox cost); set 1 for the smoke run so the cpuset hard-floor exists."
  type        = number
  default     = 0
}

variable "sandbox_max_size" {
  description = "Max concurrent contestants (one algo pod per sandbox node)."
  type        = number
  default     = 10
}

variable "sandbox_desired_size" {
  type    = number
  default = 1
}

variable "sandbox_disk_size" {
  description = "gp3 root volume (GiB) for sandbox nodes."
  type        = number
  default     = 80
}

variable "sandbox_reserved_system_cpus" {
  description = "Cores handed to the kubelet/system so they never collide with the algo pod's pinned cpuset. 0,1 on an 8-vCPU node."
  type        = string
  default     = "0,1"
}


variable "botworker_instance_type" {
  description = <<-EOT
    Instance type for the dedicated bot-fleet load-generator node group. Compute-
    optimized (load generation is CPU + outbound-network bound, not memory bound).
    Isolated from both the measurement plane (general) and the system-under-test
    (sandbox) so the load generators never steal cores from Kafka/ingester or the
    contestant algo — that isolation is what makes the scaling sweep meaningful.
  EOT
  type        = string
  default     = "c6i.xlarge"
}

variable "botworker_min_size" {
  description = "Scale-to-zero floor in prod (KEDA + cluster-autoscaler bring nodes up on Kafka lag). Pin to the sweep's low end (1) for the benchmark."
  type        = number
  default     = 0
}

variable "botworker_max_size" {
  description = "Upper bound of the load-generator scaling sweep (e.g. 1 node -> 2 nodes)."
  type        = number
  default     = 2
}

variable "botworker_desired_size" {
  type    = number
  default = 0
}

variable "botworker_disk_size" {
  description = "gp3 root volume (GiB) for botworker nodes (stateless load gen — small)."
  type        = number
  default     = 50
}


variable "enable_spawner_irsa" {
  description = <<-EOT
    Annotate the build-spawner ServiceAccount with its IRSA role via Terraform.
    Default false: that SA is created by the platform manifests (applied after
    Terraform), so annotating it here errors on first apply. The IAM role is still
    created regardless; enable this only after the platform is deployed, or annotate
    the SA with kubectl post-deploy. The load-gen benchmark doesn't use the spawner.
  EOT
  type        = bool
  default     = false
}


variable "enable_sandbox_cpuset" {
  description = <<-EOT
    Apply the static-CPU-manager NodeConfig to the sandbox node group (exclusive
    integer cpusets for contestant algos — a contest-fairness feature). Default
    false: not needed for the load-gen benchmark (drain-sink contestant), and the
    current NodeConfig prevents the node from joining (reservedSystemCPUs conflicts
    with EKS's default kube/system-reserved CPU → kubelet won't start). Re-enable
    only after that conflict is resolved and validated.
  EOT
  type        = bool
  default     = false
}


variable "enable_gvisor" {
  description = <<-EOT
    Create the gVisor (runsc) RuntimeClass. Off by default: the benchmark + minimal
    deploy run RUNTIME_CLASS="" (runc), and this is a kubernetes_manifest resource
    that requires a live cluster at PLAN time (it would break the first plan before
    the cluster exists). Enable only after the cluster is up and runsc is installed
    on the sandbox nodes.
  EOT
  type        = bool
  default     = false
}


variable "service_images" {
  description = <<-EOT
    The 12 service image short-names. One ECR repository (iicpc/<name>) is created
    per entry. This list is the single source of truth shared with the Makefile's
    `images` target — keep it in sync. 11 repo-root-context Go/Rust services +
    the frontend (frontend/ build context).
  EOT
  type        = list(string)
  default = [
    "auth-api",
    "submission-api",
    "leaderboard-api",
    "spawner",
    "sandbox-orchestrator",
    "bot-fleet-controller",
    "bot-fleet",
    "ebpf-latency",
    "telemetry-ingester",
    "correctness-validator",
    "score-computer",
    "frontend",
  ]
}

variable "ecr_image_tag_mutability" {
  description = <<-EOT
    IMMUTABLE makes a git-SHA tag un-overwritable so two contestants can never be
    scored against different platform binaries under the same tag (DEPLOYMENT_EKS
    §1.2 'a benchmark is a scientific measurement'). Use MUTABLE only if your CI
    retags.
  EOT
  type        = string
  default     = "IMMUTABLE"
}

variable "tags" {
  description = "Tags applied to all taggable resources."
  type        = map(string)
  default = {
    Project   = "iicpc"
    ManagedBy = "terraform"
  }
}
