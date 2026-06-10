# outputs.tf — everything the Makefile and the operator need post-apply.
#
# Problem: the Makefile's ecr-login/images/deploy targets and the operator's
# kubectl all need values that only exist after apply (cluster endpoint, the 12
# ECR URLs, the kubeconfig command). Decision: surface them as outputs so the
# Makefile reads them with `terraform output -raw/-json` instead of re-deriving
# ARNs by hand. Why -json for the map: the images target iterates the repo URLs.

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = module.eks.cluster_endpoint
}

output "cluster_version" {
  description = "EKS control-plane version."
  value       = module.eks.cluster_version
}

output "region" {
  description = "AWS region."
  value       = var.region
}

output "account_id" {
  description = "Resolved AWS account id."
  value       = local.account_id
}

output "ecr_registry" {
  description = "ECR registry host (<account>.dkr.ecr.<region>.amazonaws.com)."
  value       = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

output "ecr_repository_urls" {
  description = "Map of service short-name -> ECR repository URL. Consumed by the Makefile images/deploy targets."
  value       = { for name, repo in aws_ecr_repository.service : name => repo.repository_url }
}

output "kubeconfig_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "oidc_provider_arn" {
  description = "IRSA OIDC provider ARN."
  value       = module.eks.oidc_provider_arn
}

output "irsa_role_arns" {
  description = "IRSA role ARNs by component (for verification / cross-referencing in manifests)."
  value = {
    ebs_csi        = module.ebs_csi_irsa.iam_role_arn
    alb_controller = module.alb_controller_irsa.iam_role_arn
    build_spawner  = module.spawner_irsa.iam_role_arn
  }
}
