# variables.tf — every operator-tunable input.
#
# Problem: the DEPLOYMENT_EKS.md runbook hard-codes region/cluster/account into
# shell exports; that does not compose with Terraform and makes a second
# environment (staging) a copy-paste hazard.
#
# Decision: lift exactly the knobs the runbook exports (region, cluster name,
# account id, instance types, node-group sizes) into variables with defaults that
# match the documented prod values, so `terraform apply` with no -var reproduces
# the runbook's cluster, and overriding a single -var produces a smaller smoke
# cluster (see the MEMORY "EKS minimal first" note).
#
# Why defaults mirror DEPLOYMENT_EKS.md §2.3 exactly: the manifests, kubelet
# tuning, and ALGO_CPU math all assume an 8-vCPU sandbox node with cores 0,1
# reserved; changing the instance family without re-deriving ALGO_CPU breaks the
# cpuset pinning. The defaults are the validated combination.

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

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# general node group — data tier, APIs, controllers, build, telemetry, KEDA
# ---------------------------------------------------------------------------

variable "general_instance_type" {
  description = "Instance type for the general node group."
  type        = string
  # DEPLOYMENT_EKS.md §2.2 suggests m6i.2xlarge; the TASK + MEMORY 'EKS minimal
  # first' smoke plan uses m6i.xlarge. Default to the smoke size; bump for prod.
  default = "m6i.xlarge"
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

# ---------------------------------------------------------------------------
# sandbox node group — one algo pod + its capture Job per node
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# ECR
# ---------------------------------------------------------------------------

variable "service_images" {
  description = <<-EOT
    The 12 service image short-names. One ECR repository (iicpc/<name>) is created
    per entry. This list is the single source of truth shared with the Makefile's
    `images` target — keep it in sync. 11 repo-root-context Go/Rust services +
    the frontend (frontend/ build context).
  EOT
  type = list(string)
  default = [
    "auth-api",
    "submission-api",
    "leaderboard-api",
    "spawner", # services/build-worker -> image name 'spawner'
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
  type    = string
  default = "IMMUTABLE"
}

variable "tags" {
  description = "Tags applied to all taggable resources."
  type        = map(string)
  default = {
    Project   = "iicpc"
    ManagedBy = "terraform"
  }
}
