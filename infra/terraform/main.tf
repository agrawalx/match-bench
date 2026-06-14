# infra/terraform/main.tf
#
# This Terraform file wires providers, networking, the EKS cluster, and node groups.
# It belongs to the IICPC AWS infrastructure layer and should remain
# aligned with infra/README.md and the Kubernetes manifests under k8s/.
# Keep explanatory comments at this file header so resource blocks stay declarative.

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


module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = ">= 20.24.0, < 21.0.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  enable_irsa = true

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  enable_cluster_creator_admin_permissions = true

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

  eks_managed_node_groups = {
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

      # Exclusive-core cpuset pinning for contestant algos (a contest-fairness
      # feature). OFF for the load-gen benchmark (the contestant is a drain sink,
      # not a latency-measured algo) and because this NodeConfig currently blocks
      # the node from joining: setting reservedSystemCPUs is mutually exclusive with
      # EKS's default kube/system-reserved CPU, so kubelet refuses to start and the
      # node never registers. Re-enable (enable_sandbox_cpuset=true) only after that
      # conflict is resolved and validated on a node.
      cloudinit_pre_nodeadm = var.enable_sandbox_cpuset ? [
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
      ] : []
    }

    # Dedicated bot-fleet load-generator pool — the scaling variable for the
    # throughput benchmark. Tainted (botworker=true:NoSchedule) + labelled so ONLY
    # bot-fleet-worker pods land here (they carry the matching nodeSelector +
    # toleration); nothing from the data/measurement plane or the contestant can
    # contend for these cores. Scale the sweep by bumping desired_size (e.g. 1 -> 2).
    botworker = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.botworker_instance_type]
      min_size       = var.botworker_min_size
      max_size       = var.botworker_max_size
      desired_size   = var.botworker_desired_size

      labels = {
        pool = "botworker"
      }

      taints = {
        botworker = {
          key    = "botworker"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.botworker_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }

    # Dedicated Kafka broker pool for the 2M/s BENCH suite. Tainted + labelled so the
    # 3-broker StatefulSet (podAntiAffinity, one per node) lands here, I/O-isolated
    # from the data/measurement plane. Defaults to 0 nodes so the e2e cluster doesn't
    # pay for it; the bench tfvars sets kafka_desired_size=3. Broker LOG data lives on
    # a gp3 PVC (throughput bumped via StorageClass), not this node root volume.
    kafka = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.kafka_instance_type]
      min_size       = var.kafka_min_size
      max_size       = var.kafka_max_size
      desired_size   = var.kafka_desired_size

      labels = {
        pool = "kafka"
      }

      taints = {
        kafka = {
          key    = "kafka"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.kafka_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }
  }

  tags = var.tags
}
