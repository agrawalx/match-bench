# addons.tf — in-cluster pieces that are not EKS managed addons.
#
# Problem: four cluster-level prerequisites are NOT EKS managed addons and the
# runbook installs them by hand (DEPLOYMENT §3.2 storageclass, §3.3 ALB
# controller, §5 KEDA, §3.6 gVisor RuntimeClass). Hand steps drift and are easy
# to forget. Decision: declare the clean ones as Terraform resources here
# (gp3 StorageClass via the kubernetes provider; ALB controller + KEDA via
# helm_release) and leave the genuinely host-level one (runsc install on sandbox
# nodes) to a documented manual step, because it needs a privileged installer
# DaemonSet or a custom AMI that Terraform should not silently bake.
#
# Why helm_release (not raw manifests): the ALB controller and KEDA ship as
# charts with CRDs and webhooks; reproducing them as kubernetes_manifest is
# brittle. helm_release pins a chart version and is idempotent.

# ---------------------------------------------------------------------------
# gp3 default StorageClass (DEPLOYMENT §3.2)
# ---------------------------------------------------------------------------
# Problem: EKS has no default StorageClass, so the data-tier volumeClaimTemplates
# (postgres/timescaledb/minio/kafka) sit Pending forever. Decision: a gp3 class
# marked default, provisioned by the EBS CSI driver, WaitForFirstConsumer so the
# EBS volume is created in the pod's AZ (EBS is AZ-bound, RWO). Why Retain: keep
# contest data if a PVC is deleted by accident.
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

  # The EBS CSI addon must be installed (and its IRSA role attached) before this
  # class is usable.
  depends_on = [module.eks]
}

# EKS ships a gp2 class as default on some versions; un-default it so gp3 wins.
# This is best-effort — if gp2 is absent the apply is a no-op patch target, so we
# only manage it when present by ignoring not-found via a null check is not
# possible declaratively; document instead (see README). We leave gp2 alone and
# rely on gp3 carrying the is-default annotation; if both are default, gp3's
# explicit parameters still apply when named. To be safe, name gp3 explicitly in
# any new PVCs that must not pick up gp2.

# ---------------------------------------------------------------------------
# AWS Load Balancer Controller (DEPLOYMENT §3.3)
# ---------------------------------------------------------------------------
# Fronts the `frontend` Service with an internet-facing ALB (the single public
# URL). serviceAccount.create=true here, but we hand it the IRSA role ARN from
# irsa.tf so it can call ELB/EC2 APIs without node-role escalation.
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

# ---------------------------------------------------------------------------
# KEDA (DEPLOYMENT §5) — autoscales bot-fleet-worker on Kafka lag.
# ---------------------------------------------------------------------------
# Problem: bot-fleet workers must scale up before a large run so each WorkloadSpec
# shard gets its own pod. Decision: install KEDA via its chart into its own
# namespace; the bot-fleet ScaledObject (k8s/benchmark/bot-fleet/scaledobject.yaml)
# is applied by the deploy step. Why a separate ns: KEDA's operator +
# metrics-apiserver are cluster infra, not part of the benchmark namespace.
resource "helm_release" "keda" {
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = "2.14.0"
  namespace        = "keda"
  create_namespace = true

  depends_on = [module.eks]
}

# ---------------------------------------------------------------------------
# gVisor RuntimeClass — DECLARED here, but only WORKS after runsc is installed on
# the sandbox nodes (a host-level step; see README "Manual steps").
# ---------------------------------------------------------------------------
# Problem: EKS does not ship a gvisor RuntimeClass, and runsc must be present on
# the node before a pod can use handler=runsc. Decision: register the
# RuntimeClass object from Terraform (it is harmless if unused — the orchestrator
# only sets runtimeClassName when RUNTIME_CLASS is non-empty, DEPLOYMENT §3.6),
# pinned to the sandbox pool. Why gate it behind a variable-less always-create: it
# costs nothing and means the only remaining manual action is the host install.
# If you are not running gVisor, leave RUNTIME_CLASS="" on the orchestrator and
# this object is simply never referenced.
#
# Caveat: kubernetes_manifest does a plan-time API read, which fails on the very
# first apply (cluster not yet reachable). Two clean options: (a) run
# `terraform apply -target=module.eks` once, then a full apply; or (b) apply the
# RuntimeClass out of band via scripts/install-addons.sh and comment this resource
# out. The README documents (a) as the default flow.
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
