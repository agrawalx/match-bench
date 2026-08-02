#!/usr/bin/env bash
# infra/terraform-v2/precheck.sh — fail BEFORE apply, not mid-nodegroup.
#
# Validates the traps that stalled previous bring-ups:
#   1. x86 on-demand vCPU quota (L-1216C47A)  — contest-day needs ~60, ask 64.
#   2. Graviton on-demand vCPU quota (L-DB2E81BA) — 16 botworker nodes x 4 vCPU, ask 64.
#   3. AWS credentials + region sanity.
#   4. Live spot prices printed for reference (informational only; no spot).
#
#   ./precheck.sh [region]
set -euo pipefail
REGION="${1:-us-east-1}"
X86_NEED="${X86_NEED:-64}"
ARM_NEED="${ARM_NEED:-64}"

fail=0

echo "== identity =="
aws sts get-caller-identity --output table --region "$REGION" || { echo "!! no AWS credentials"; exit 1; }

quota() { # quota <code> — current value or 0
  aws service-quotas get-service-quota \
    --service-code ec2 --quota-code "$1" \
    --region "$REGION" --query 'Quota.Value' --output text 2>/dev/null || echo 0
}

echo "== quotas =="
x86=$(quota L-1216C47A)   # Running On-Demand Standard (A,C,D,H,I,M,R,T,Z)
arm=$(quota L-DB2E81BA)   # Running On-Demand G (Graviton)
printf "   x86 standard on-demand vCPU: %.0f (need >= %s)\n" "$x86" "$X86_NEED"
printf "   graviton (G) on-demand vCPU: %.0f (need >= %s)\n" "$arm" "$ARM_NEED"
awk -v have="$x86" -v need="$X86_NEED" 'BEGIN{exit !(have+0 < need+0)}' && {
  echo "!! x86 quota too low — request an increase (Service Quotas -> EC2 -> L-1216C47A)"; fail=1; }
awk -v have="$arm" -v need="$ARM_NEED" 'BEGIN{exit !(have+0 < need+0)}' && {
  echo "!! Graviton quota too low — request an increase (Service Quotas -> EC2 -> L-DB2E81BA)"; fail=1; }

echo "== instance type availability in $REGION =="
for t in m6i.2xlarge m6i.xlarge c6i.2xlarge c7g.xlarge; do
  n=$(aws ec2 describe-instance-type-offerings --region "$REGION" \
      --filters "Name=instance-type,Values=$t" \
      --query 'length(InstanceTypeOfferings)' --output text 2>/dev/null || echo 0)
  if [ "${n:-0}" -ge 1 ]; then echo "   $t: available"; else echo "!! $t: NOT offered in $REGION"; fail=1; fi
done

if [ "$fail" -ne 0 ]; then
  echo
  echo "############ PRECHECK FAILED — fix the above before terraform apply ############"
  exit 1
fi
echo
echo "############ PRECHECK OK ############"
