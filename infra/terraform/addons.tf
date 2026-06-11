# infra/terraform/addons.tf
#
# This Terraform file installs cluster addons and Kubernetes support resources.
# It belongs to the IICPC AWS infrastructure layer and should remain
# aligned with infra/README.md and the Kubernetes manifests under k8s/.
# Keep explanatory comments at this file header so resource blocks stay declarative.

resource "kubernetes_storage_class" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }
  storage_provisioner    = "ebs.csi.aws.com"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true
  reclaim_policy         = "Retain"
  parameters = {
    type       = "gp3"
    fsType     = "ext4"
    iops       = "3000"
    throughput = "125"
  }

  depends_on = [module.eks]
}


resource "helm_release" "alb_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  version    = "1.8.1"
  namespace  = "kube-system"

  set {
    name  = "clusterName"
    value = var.cluster_name
  }
  set {
    name  = "region"
    value = var.region
  }
  set {
    name  = "vpcId"
    value = module.vpc.vpc_id
  }
  set {
    name  = "serviceAccount.create"
    value = "true"
  }
  set {
    name  = "serviceAccount.name"
    value = "aws-load-balancer-controller"
  }
  set {
    name  = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = module.alb_controller_irsa.iam_role_arn
  }

  depends_on = [module.eks, module.alb_controller_irsa]
}

resource "helm_release" "keda" {
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = "2.14.0"
  namespace        = "keda"
  create_namespace = true

  depends_on = [module.eks]
}

resource "kubernetes_manifest" "gvisor_runtimeclass" {
  manifest = {
    apiVersion = "node.k8s.io/v1"
    kind       = "RuntimeClass"
    metadata = {
      name = "gvisor"
    }
    handler = "runsc"
    scheduling = {
      nodeSelector = {
        pool = "sandbox"
      }
      tolerations = [
        {
          key      = "sandbox"
          operator = "Equal"
          value    = "true"
          effect   = "NoSchedule"
        }
      ]
    }
  }

  depends_on = [module.eks]
}
