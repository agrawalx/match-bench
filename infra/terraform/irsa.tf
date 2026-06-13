# infra/terraform/irsa.tf
#
# This Terraform file declares IAM roles for Kubernetes service accounts.
# It belongs to the IICPC AWS infrastructure layer and should remain
# aligned with infra/README.md and the Kubernetes manifests under k8s/.
# Keep explanatory comments at this file header so resource blocks stay declarative.

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name             = "${var.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }

  tags = var.tags
}

module "alb_controller_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name                              = "${var.cluster_name}-alb-controller"
  attach_load_balancer_controller_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-load-balancer-controller"]
    }
  }

  tags = var.tags
}


data "aws_iam_policy_document" "spawner_ecr" {
  statement {
    sid       = "EcrAuth"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "EcrManageRepos"
    effect = "Allow"
    actions = [
      "ecr:CreateRepository",
      "ecr:DescribeRepositories",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EcrPush"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]
    resources = ["arn:aws:ecr:${var.region}:${local.account_id}:repository/iicpc/*"]
  }
}

resource "aws_iam_policy" "spawner_ecr" {
  name        = "${var.cluster_name}-spawner-ecr"
  description = "build-spawner: ECR auth + create/describe + push to iicpc/*"
  policy      = data.aws_iam_policy_document.spawner_ecr.json
  tags        = var.tags
}

module "spawner_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name = "${var.cluster_name}-build-spawner"
  role_policy_arns = {
    ecr = aws_iam_policy.spawner_ecr.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["build:build-spawner"]
    }
  }

  tags = var.tags
}

# Annotate the spawner ServiceAccount with its IRSA role so the build pipeline can
# push contestant images to ECR. Off by default: the SA is created by the platform
# manifests (k8s/build/spawner), which are applied AFTER terraform — so terraform
# can't annotate it yet (it errors "ServiceAccount build-spawner does not exist").
# The load-gen benchmark doesn't use the build pipeline (the drain image is
# pre-pushed). When you DO need the spawner, deploy the platform first, then run:
#   kubectl -n build annotate sa build-spawner \
#     eks.amazonaws.com/role-arn=<module.spawner_irsa.iam_role_arn> --overwrite
# (or set enable_spawner_irsa=true and re-apply once the SA exists). The IAM role
# itself (module.spawner_irsa) is always created above; only the SA wiring is gated.
resource "kubernetes_annotations" "spawner_sa_irsa" {
  count = var.enable_spawner_irsa ? 1 : 0

  api_version = "v1"
  kind        = "ServiceAccount"
  metadata {
    name      = "build-spawner"
    namespace = "build"
  }
  annotations = {
    "eks.amazonaws.com/role-arn" = module.spawner_irsa.iam_role_arn
  }
  force = true
}
