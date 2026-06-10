# main.tf — provider wiring, VPC, and the EKS cluster + node groups.
#
# This file builds the cluster body. ECR repos live in ecr.tf; the application
# IRSA roles (ALB controller, EBS CSI, build-spawner) live in irsa.tf; the
# in-cluster pieces (gp3 default StorageClass, KEDA, ALB controller, gVisor
# RuntimeClass) live in addons.tf. They are split by concern so each file states
# one problem/decision/why.

# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------
# Problem: the kubernetes/helm providers need cluster credentials, but those only
# exist *after* the EKS module creates the cluster. Decision: authenticate the k8s
# providers with the module's endpoint/CA plus a freshly-minted token from the
# aws CLI exec plugin, so a single `terraform apply` can create the cluster and
# then populate it. Why exec (not a static token): EKS tokens expire in 15 min;
# the exec plugin re-mints on every provider call.

provider "aws" {
  region = var.region
  default_tags {
    tags = var.tags
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = var.account_id != "" ? var.account_id : data.aws_caller_identity.current.account_id
  azs        = slice(data.aws_availability_zones.available.names, 0, var.az_count)
}

data "aws_availability_zones" "available" {
  state = "available"
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}

# ---------------------------------------------------------------------------
# VPC
# ---------------------------------------------------------------------------
# Problem: the VPC CNI assigns pod IPs from the VPC, and the ALB controller needs
# subnets discoverable by tag. Decision: a standard 3-AZ public+private VPC with a
# single NAT gateway, carrying the kubernetes.io/role/elb (public) and
# .../internal-elb (private) discovery tags the ALB controller requires. Why one
# NAT (not per-AZ): cost; egress from sandbox/build pods is not latency-sensitive,
# and the measured path (bot->algo) is in-cluster.

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = ">= 5.8.0, < 6.0.0"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  # Subnet discovery tags for the AWS Load Balancer Controller (DEPLOYMENT §3.3).
  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# EKS cluster + managed node groups + addons
# ---------------------------------------------------------------------------
# Problem: hand-rolling the control plane, OIDC provider, node IAM roles, and the
# four addons is ~600 lines of boilerplate that drifts from EKS defaults.
# Decision: use terraform-aws-modules/eks v20, which gives us the cluster, the
# IRSA OIDC provider (enable_irsa), managed node groups with kubelet extra args,
# and cluster_addons in one readable block — exactly the surface DEPLOYMENT §2.3
# expresses in eksctl YAML. Why v20: it is the first major to default to EKS
# Access Entries and to support per-addon configuration_values (needed for the
# vpc-cni enableNetworkPolicy=true toggle, DEPLOYMENT §3.1).

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = ">= 20.24.0, < 21.0.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  # IRSA: mint the OIDC provider so ServiceAccounts can assume IAM roles
  # (ALB controller, EBS CSI, build-spawner). DEPLOYMENT §2.3 iam.withOIDC.
  enable_irsa = true

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # Give the identity running `terraform apply` cluster-admin via an access entry,
  # so the kubernetes/helm providers (and your kubectl) can reach the API
  # immediately after create without a separate aws-auth ConfigMap edit.
  enable_cluster_creator_admin_permissions = true

  # ----- Addons (DEPLOYMENT §2.3) -----
  # vpc-cni MUST enable NetworkPolicy or every isolation policy is a silent no-op
  # (DEPLOYMENT §3.1, the highest-severity divergence). The before_compute flag
  # ensures the CNI is configured before nodes join so the nodeagent is present.
  cluster_addons = {
    vpc-cni = {
      most_recent    = true
      before_compute = true
      configuration_values = jsonencode({
        enableNetworkPolicy = "true"
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
        }
      })
    }
    kube-proxy = {
      most_recent = true
    }
    coredns = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  # ----- Managed node groups -----
  eks_managed_node_groups = {
    # general: data tier, APIs, controllers, build, telemetry, KEDA. Default
    # kubelet (bin-packing matters more than jitter here). DEPLOYMENT §2.2.
    general = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.general_instance_type]
      min_size       = var.general_min_size
      max_size       = var.general_max_size
      desired_size   = var.general_desired_size

      labels = {
        role = "general"
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.general_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }

    # sandbox: one algo pod + its capture Job per node. Tainted so ONLY tolerating
    # pods land here (the isolation boundary, DEPLOYMENT §2.1). AL2023 ships a
    # modern kernel for the eBPF capture (DEPLOYMENT §3.4). Static CPU manager +
    # full-pcpus-only + reservedSystemCPUs give the algo pod an exclusive cpuset
    # (DEPLOYMENT §2.4).
    sandbox = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.sandbox_instance_type]
      min_size       = var.sandbox_min_size
      max_size       = var.sandbox_max_size
      desired_size   = var.sandbox_desired_size

      labels = {
        pool = "sandbox"
      }

      taints = {
        sandbox = {
          key    = "sandbox"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.sandbox_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }

      # AL2023 uses nodeadm; kubelet tuning goes through the NodeConfig in the
      # cloudinit user data. This is the AL2023 equivalent of DEPLOYMENT §2.3's
      # eksctl kubeletExtraConfig block (cpuManagerPolicy=static,
      # full-pcpus-only=true, topology single-numa-node, reservedSystemCPUs=0,1).
      cloudinit_pre_nodeadm = [
        {
          content_type = "application/node.eks.aws"
          content      = <<-EOT
            ---
            apiVersion: node.eks.aws/v1alpha1
            kind: NodeConfig
            spec:
              kubelet:
                config:
                  cpuManagerPolicy: static
                  cpuManagerPolicyOptions:
                    full-pcpus-only: "true"
                  topologyManagerPolicy: single-numa-node
                  reservedSystemCPUs: "${var.sandbox_reserved_system_cpus}"
          EOT
        }
      ]
    }
  }

  tags = var.tags
}
