# irsa.tf — IAM Roles for Service Accounts (IRSA).
#
# Problem: three in-cluster components need AWS permissions that must NOT be
# granted to the broad node role: the EBS CSI driver (create/attach volumes), the
# AWS Load Balancer Controller (create ALBs/target groups/SGs), and the
# build-spawner ServiceAccount (push images to ECR + create repos on demand).
# Granting these on the node role would hand them to every pod on the node,
# including untrusted contestant algo pods.
#
# Decision: mint one IAM role per component, each with a trust policy scoped to a
# single ServiceAccount via the cluster OIDC provider, using the
# iam-role-for-service-accounts-eks module's well-known-policy presets where they
# exist (EBS CSI, ALB controller) and a hand-written least-privilege policy for
# the spawner (the only custom one). Why the module presets: AWS maintains the
# exact managed-policy attachments those controllers need; re-deriving them by
# hand drifts.

# ---------------------------------------------------------------------------
# EBS CSI driver — wired into the aws-ebs-csi-driver addon in main.tf.
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# AWS Load Balancer Controller — assumed by the SA the helm chart creates.
# DEPLOYMENT §3.3: fronts the `frontend` (the single public URL) via an ALB.
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# build-spawner — pushes built contestant images to ECR and lazily creates the
# per-contestant repo on first build (the spawner pre-creates ECR repos,
# DEPLOYMENT §1.2 / the 'ECR repo pre-create in spawner' recent work).
# ---------------------------------------------------------------------------
# Problem: kaniko build Jobs (and the spawner that orchestrates them) need to
# authenticate to ECR, create a repository if missing, and push layers. They run
# in the `build` namespace under the `build-spawner` ServiceAccount
# (k8s/build/serviceaccount.yaml). Decision: a least-privilege policy granting
# exactly Get/Create/Describe + the push verbs, scoped to repos under the
# iicpc/* prefix in this account/region. Why scoped to iicpc/*: a contestant
# build must never be able to push over a platform service image.

data "aws_iam_policy_document" "spawner_ecr" {
  # Auth token is account-wide (no resource scoping possible on this action).
  statement {
    sid       = "EcrAuth"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  # Create/describe repos. CreateRepository cannot be resource-scoped to a prefix
  # (the repo does not exist yet), so it is account-wide; the push/pull verbs
  # below ARE scoped to iicpc/*.
  statement {
    sid    = "EcrManageRepos"
    effect = "Allow"
    actions = [
      "ecr:CreateRepository",
      "ecr:DescribeRepositories",
    ]
    resources = ["*"]
  }

  # Push + describe layers, scoped to iicpc/* repositories only.
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
      # Matches k8s/build/serviceaccount.yaml (namespace build, name build-spawner).
      namespace_service_accounts = ["build:build-spawner"]
    }
  }

  tags = var.tags
}

# Annotate the existing build-spawner ServiceAccount with the IRSA role ARN.
# Problem: the SA manifest (k8s/build/serviceaccount.yaml) has no role annotation;
# without it the spawner pod gets the node role, not this scoped role. Decision:
# patch the annotation from Terraform so the IAM<->SA binding is declared in one
# place. Why not edit the YAML: keeps the repo manifests cloud-agnostic (they also
# run on k3s); the EKS-specific binding lives only here.
resource "kubernetes_annotations" "spawner_sa_irsa" {
  api_version = "v1"
  kind        = "ServiceAccount"
  metadata {
    name      = "build-spawner"
    namespace = "build"
  }
  annotations = {
    "eks.amazonaws.com/role-arn" = module.spawner_irsa.iam_role_arn
  }
  # The SA is created by `kubectl apply -f k8s/build` (Makefile deploy target),
  # which may run after apply. force lets Terraform own just this annotation.
  force = true
}
