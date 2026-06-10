#!/usr/bin/env bash
# infra/scripts/install-addons.sh
#
# This script automates infrastructure operations for install addons.
# It belongs to the IICPC operational toolchain and should keep
# setup, validation, and deployment behavior explicit at entry points.
# Function-level comments describe reusable shell routines below.

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
