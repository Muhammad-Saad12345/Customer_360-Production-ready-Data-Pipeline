#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — GitHub OIDC & IAM Pre-flight Check
#
# Verifies that all AWS-side prerequisites for GitHub Actions OIDC are in
# place before the first CI/CD run.
#
# Usage (run from any directory with AWS CLI + deploy credentials):
#   chmod +x scripts/verify_oidc.sh
#   ./scripts/verify_oidc.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ACCOUNT_ID="734143568343"
REGION="us-east-1"
GITHUB_REPO="Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline"
DEPLOY_ROLE="github-mwaa-deploy-role"
MWAA_ROLE="C360-MWAA-Execution-Role"
SECRET_NAME="c_360_secret_manager"
PASS=0; FAIL=0

check() {
  local label="$1"; local result="$2"
  if [[ "${result}" == "PASS" ]]; then
    echo "  ✅ PASS  ${label}"; ((PASS++))
  else
    echo "  ❌ FAIL  ${label}: ${result}"; ((FAIL++))
  fi
}

echo "=================================================="
echo "Customer 360 — AWS Pre-flight Check"
echo "Account : ${ACCOUNT_ID}"
echo "Region  : ${REGION}"
echo "=================================================="

# ── 1. OIDC Provider ─────────────────────────────────────────────────────────
echo ""
echo "[1] GitHub OIDC Provider"
OIDC_URL="https://token.actions.githubusercontent.com"
OIDC_EXISTS=$(aws iam list-open-id-connect-providers \
  --query "OpenIDConnectProviderList[?contains(Arn,'token.actions.githubusercontent.com')].Arn" \
  --output text 2>/dev/null || echo "")
if [[ -n "${OIDC_EXISTS}" ]]; then
  check "OIDC provider exists" "PASS"
else
  check "OIDC provider exists" "NOT FOUND — run: aws iam create-open-id-connect-provider --url ${OIDC_URL} --client-id-list sts.amazonaws.com --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1"
fi

# ── 2. GitHub Deploy Role ────────────────────────────────────────────────────
echo ""
echo "[2] GitHub Deploy Role: ${DEPLOY_ROLE}"
ROLE_EXISTS=$(aws iam get-role --role-name "${DEPLOY_ROLE}" \
  --query "Role.RoleName" --output text 2>/dev/null || echo "")
if [[ "${ROLE_EXISTS}" == "${DEPLOY_ROLE}" ]]; then
  check "Deploy role exists" "PASS"

  # Check trust policy references the correct repo
  TRUST=$(aws iam get-role --role-name "${DEPLOY_ROLE}" \
    --query "Role.AssumeRolePolicyDocument" --output json 2>/dev/null || echo "{}")
  if echo "${TRUST}" | grep -q "${GITHUB_REPO}"; then
    check "Trust policy references correct repo" "PASS"
  else
    check "Trust policy references correct repo" "FAIL — repo '${GITHUB_REPO}' not found in trust policy. Edit role trust in IAM console."
  fi
else
  check "Deploy role exists" "NOT FOUND — create the role with OIDC trust for ${GITHUB_REPO}"
fi

# ── 3. MWAA Execution Role ───────────────────────────────────────────────────
echo ""
echo "[3] MWAA Execution Role: ${MWAA_ROLE}"
MWAA_ROLE_EXISTS=$(aws iam get-role --role-name "${MWAA_ROLE}" \
  --query "Role.RoleName" --output text 2>/dev/null || echo "")
if [[ "${MWAA_ROLE_EXISTS}" == "${MWAA_ROLE}" ]]; then
  check "MWAA execution role exists" "PASS"
else
  check "MWAA execution role exists" "NOT FOUND"
fi

# ── 4. Secrets Manager Secret ────────────────────────────────────────────────
echo ""
echo "[4] Secrets Manager: ${SECRET_NAME}"
SECRET_EXISTS=$(aws secretsmanager describe-secret \
  --secret-id "${SECRET_NAME}" --region "${REGION}" \
  --query "Name" --output text 2>/dev/null || echo "")
if [[ "${SECRET_EXISTS}" == "${SECRET_NAME}" ]]; then
  check "Secret exists" "PASS"

  # Check required keys exist in the secret (without displaying values)
  REQUIRED_KEYS=("HUBSPOT_API_KEY" "MAILCHIMP_API_KEY" "ZENDESK_API_KEY" "ZENDESK_SECRET_KEY" "LOYALTY_API_KEY")
  SECRET_VALUE=$(aws secretsmanager get-secret-value \
    --secret-id "${SECRET_NAME}" --region "${REGION}" \
    --query "SecretString" --output text 2>/dev/null || echo "{}")
  for key in "${REQUIRED_KEYS[@]}"; do
    if echo "${SECRET_VALUE}" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if '${key}' in d and d['${key}'] else 1)" 2>/dev/null; then
      check "Secret key '${key}' present and non-empty" "PASS"
    else
      check "Secret key '${key}' present and non-empty" "MISSING — add to Secrets Manager console (do NOT put value here)"
    fi
  done
else
  check "Secret '${SECRET_NAME}' exists" "NOT FOUND — create it in Secrets Manager with required keys"
fi

# ── 5. S3 Buckets ────────────────────────────────────────────────────────────
echo ""
echo "[5] S3 Buckets"
for bucket in "c360-bucket786" "c360-mwaa-prod-786" "c360-staging-tfstate"; do
  EXISTS=$(aws s3api head-bucket --bucket "${bucket}" 2>/dev/null && echo "PASS" || echo "MISSING")
  check "Bucket: ${bucket}" "${EXISTS}"
done

# ── 6. DynamoDB Lock Table ───────────────────────────────────────────────────
echo ""
echo "[6] Terraform State Lock Table"
TABLE_EXISTS=$(aws dynamodb describe-table --table-name "c360-staging-tflock" \
  --region "${REGION}" --query "Table.TableName" --output text 2>/dev/null || echo "")
if [[ "${TABLE_EXISTS}" == "c360-staging-tflock" ]]; then
  check "DynamoDB table c360-staging-tflock" "PASS"
else
  check "DynamoDB table c360-staging-tflock" "MISSING — run: ./scripts/bootstrap_tf_state.sh staging"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo "Pre-flight Result: ${PASS} PASS  /  ${FAIL} FAIL"
if [[ ${FAIL} -eq 0 ]]; then
  echo "STATUS: ✅ READY — proceed with terraform init"
else
  echo "STATUS: ❌ NOT READY — resolve FAIL items above before deploying"
fi
echo "=================================================="
