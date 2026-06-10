#!/usr/bin/env bash
# install-addons.sh — FALLBACK installer for the non-Terraform cluster addons.
#
# Terraform already installs the gp3 StorageClass, the AWS Load Balancer
# Controller, KEDA, and the gVisor RuntimeClass object (infra/terraform/addons.tf).
# Use this script only if you ran Terraform with the helm provider disabled, or to
# (re)install a single addon out of band. It is idempotent.
#
# It does NOT install runsc on the sandbox nodes — that is a host-level step
# (privileged DaemonSet or custom AMI); see infra/README.md "Manual steps".
#
# Requires: kubectl pointed at the cluster, helm v3, and these env vars:
#   CLUSTER, AWS_REGION, VPC_ID, ALB_ROLE_ARN  (from `terraform output`)
set -euo pipefail

: "${CLUSTER:?set CLUSTER}"
: "${AWS_REGION:?set AWS_REGION}"

echo ">> AWS Load Balancer Controller"
helm repo add eks https://aws.github.io/eks-charts >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --version 1.8.1 -n kube-system \
  --set clusterName="$CLUSTER" \
  --set region="$AWS_REGION" \
  ${VPC_ID:+--set vpcId="$VPC_ID"} \
  --set serviceAccount.create=true \
  --set serviceAccount.name=aws-load-balancer-controller \
  ${ALB_ROLE_ARN:+--set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="$ALB_ROLE_ARN"}

echo ">> KEDA"
helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install keda kedacore/keda --version 2.14.0 \
  -n keda --create-namespace

echo ">> gp3 default StorageClass"
kubectl apply -f - <<'YAML'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  fsType: ext4
  iops: "3000"
  throughput: "125"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: Retain
YAML

# If a gp2 default exists, demote it so gp3 is the sole default.
if kubectl get storageclass gp2 >/dev/null 2>&1; then
  kubectl patch storageclass gp2 \
    -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}' || true
fi

echo ">> gVisor RuntimeClass (object only — runsc must be installed on sandbox nodes separately)"
kubectl apply -f - <<'YAML'
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
scheduling:
  nodeSelector:
    pool: sandbox
  tolerations:
    - key: sandbox
      operator: Equal
      value: "true"
      effect: NoSchedule
YAML

echo ">> done. Verify: kubectl get sc,runtimeclass; kubectl -n keda get pods"
