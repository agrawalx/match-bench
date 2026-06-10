# infra/terraform/versions.tf
#
# This Terraform file pins Terraform and provider versions.
# It belongs to the IICPC AWS infrastructure layer and should remain
# aligned with infra/README.md and the Kubernetes manifests under k8s/.
# Keep explanatory comments at this file header so resource blocks stay declarative.

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

}
