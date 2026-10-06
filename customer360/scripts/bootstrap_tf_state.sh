#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Terraform State Bootstrap Script
#
# Run ONCE before the first `terraform init` in any environment.
# Requires: AWS CLI configured with credentials that can create S3 and DynamoDB.
#
# Usage:
#   chmod +x scripts/bootstrap_tf_state.sh
#   ./scripts/bootstrap_tf_state.sh staging
#   ./scripts/bootstrap_tf_state.sh prod
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ENV="${1:-staging}"
REGION="us-east-1"
BUCKET="c360-${ENV}-tfstate"
TABLE="c360-${ENV}-tflock"

echo "=========================================="
echo "Bootstrapping Terraform state for: ${ENV}"
echo "  S3 bucket  : ${BUCKET}"
echo "  DynamoDB   : ${TABLE}"
echo "  Region     : ${REGION}"
echo "=========================================="

# ── S3 State Bucket ──────────────────────────────────────────────────────────

echo ""
echo "[1/5] Creating S3 state bucket: ${BUCKET}"
if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "  ✓ Bucket already exists — skipping creation"
else
  aws s3api create-bucket \
    --bucket "${BUCKET}" \
    --region "${REGION}"
  echo "  ✓ Created"
fi

echo "[2/5] Enabling versioning on ${BUCKET}"
aws s3api put-bucket-versioning \
  --bucket "${BUCKET}" \
  --versioning-configuration Status=Enabled
echo "  ✓ Versioning enabled"

echo "[3/5] Enabling server-side encryption on ${BUCKET}"
aws s3api put-bucket-encryption \
  --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'
echo "  ✓ AES-256 encryption enabled"

echo "[4/5] Blocking public access on ${BUCKET}"
aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
echo "  ✓ Public access blocked"

# ── DynamoDB Lock Table ───────────────────────────────────────────────────────

echo "[5/5] Creating DynamoDB lock table: ${TABLE}"
if aws dynamodb describe-table --table-name "${TABLE}" --region "${REGION}" 2>/dev/null; then
  echo "  ✓ Table already exists — skipping creation"
else
  aws dynamodb create-table \
    --table-name "${TABLE}" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "${REGION}"
  echo "  ✓ Created"
fi

echo ""
echo "=========================================="
echo "Bootstrap complete for: ${ENV}"
echo "You can now run:"
echo "  cd terraform/environments/${ENV}"
echo "  terraform init"
echo "  terraform plan -var-file=terraform.tfvars"
echo "=========================================="
