# versions.tf — provider and module version pins.
#
# Problem: an HFT benchmark is a measurement instrument; an unpinned provider or
# module can silently change a node-group kubelet flag, an addon version, or an
# IAM trust policy between applies and invalidate the measurement contract.
#
# Decision: pin Terraform >= 1.6 (for the `moved`/`import` block ergonomics the
# eks module assumes) and floor-pin every provider to a known-good minor. The AWS
# provider is floored at 5.40 because the eks module v20 uses the
# `cluster_addons` + access-entry surface that landed in AWS provider 5.x.
#
# Why these versions: EKS 1.32 (set in main.tf) requires terraform-aws-modules/eks
# >= 20.24; we floor at 20.24 and allow 20.x patch/minor so addon defaults track
# the cluster version without a major-version surprise.

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.40.0, < 6.0.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.27.0, < 3.0.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.13.0, < 3.0.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0.0"
    }
  }

  # Uncomment and fill to use a remote state backend (recommended for shared use).
  # backend "s3" {
  #   bucket         = "iicpc-tfstate"
  #   key            = "eks/prod/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "iicpc-tflock"
  #   encrypt        = true
  # }
}
