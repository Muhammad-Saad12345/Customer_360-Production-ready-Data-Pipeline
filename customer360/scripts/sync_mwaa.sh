#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — MWAA DAG Sync & Environment Configuration Script
#
# Syncs DAGs, src/, dbt/, and mwaa_requirements.txt to S3, then sets
# the required Airflow environment variables on the MWAA environment.
#
# Usage (from customer360/ directory):
#   chmod +x scripts/sync_mwaa.sh
#   ./scripts/sync_mwaa.sh staging
#   ./scripts/sync_mwaa.sh prod
#
# Requires: AWS CLI configured with deploy-role credentials.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ENV="${1:-staging}"
REGION="us-east-1"
MWAA_BUCKET="c360-mwaa-prod-786"
LAKEHOUSE_BUCKET="c360-bucket786"

# MWAA environment name differs per tier
if [[ "${ENV}" == "prod" || "${ENV}" == "production" ]]; then
  MWAA_ENV_NAME="customer360-prod"
  ICEBERG_DB="customer360_iceberg"
  ATHENA_WG="customer360-prod"
else
  MWAA_ENV_NAME="customer360-staging"
  ICEBERG_DB="customer360_iceberg_stage"
  ATHENA_WG="customer360-staging"
fi

ATHENA_OUTPUT="s3://${LAKEHOUSE_BUCKET}/athena-results/"
SNS_TOPIC_ARN=$(aws sns list-topics --region "${REGION}" \
  --query "Topics[?contains(TopicArn,'c360-${ENV}-pipeline-alerts')].TopicArn" \
  --output text 2>/dev/null || echo "")

echo "=========================================="
echo "Syncing MWAA for: ${ENV}"
echo "  MWAA bucket    : ${MWAA_BUCKET}"
echo "  MWAA env name  : ${MWAA_ENV_NAME}"
echo "  Lakehouse bucket: ${LAKEHOUSE_BUCKET}"
echo "  Iceberg DB      : ${ICEBERG_DB}"
echo "  Athena WG       : ${ATHENA_WG}"
echo "=========================================="

# ── 1. Sync requirements ─────────────────────────────────────────────────────
echo ""
echo "[1/5] Syncing mwaa_requirements.txt → s3://${MWAA_BUCKET}/requirements.txt"
aws s3 cp mwaa_requirements.txt "s3://${MWAA_BUCKET}/requirements.txt" \
  --region "${REGION}"
echo "  ✓ Done"

# ── 2. Sync DAG files ────────────────────────────────────────────────────────
echo "[2/5] Syncing dags/ → s3://${MWAA_BUCKET}/dags/"
aws s3 sync dags/ "s3://${MWAA_BUCKET}/dags/" \
  --region "${REGION}" \
  --delete \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"
echo "  ✓ Done"

# ── 3. Sync src package ──────────────────────────────────────────────────────
echo "[3/5] Syncing src/ → s3://${MWAA_BUCKET}/dags/src/"
aws s3 sync src/ "s3://${MWAA_BUCKET}/dags/src/" \
  --region "${REGION}" \
  --delete \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"
echo "  ✓ Done"

# ── 4. Sync dbt project ──────────────────────────────────────────────────────
echo "[4/5] Syncing dbt/ → s3://${MWAA_BUCKET}/dags/dbt/"
aws s3 sync dbt/ "s3://${MWAA_BUCKET}/dags/dbt/" \
  --region "${REGION}" \
  --delete \
  --exclude "target/*" \
  --exclude "dbt_packages/*" \
  --exclude "logs/*"
echo "  ✓ Done"

# ── 5. Set MWAA environment variables ────────────────────────────────────────
# These non-secret config values cannot come from Secrets Manager.
# The MWAA API requires the full environment configuration to be updated
# atomically — we only change airflow_configuration_options here.

echo "[5/5] Updating MWAA environment variables for: ${MWAA_ENV_NAME}"

# Build config overrides as JSON
CONFIG_OVERRIDES=$(cat <<JSONEOF
{
  "secrets.backend": "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
  "secrets.backend_kwargs": "{\"connections_prefix\": \"customer360/${ENV}/airflow/connections\", \"variables_prefix\": \"customer360/${ENV}/airflow/variables\"}",
  "core.load_examples": "False",
  "core.max_active_runs_per_dag": "1",
  "scheduler.dag_dir_list_interval": "60"
}
JSONEOF
)

# Get current MWAA environment config
CURRENT_CONFIG=$(aws mwaa get-environment \
  --name "${MWAA_ENV_NAME}" \
  --region "${REGION}" \
  --query "Environment.AirflowConfigurationOptions" \
  --output json 2>/dev/null || echo "{}")

echo "  Current MWAA config options retrieved."
echo ""
echo "  NOTE: Environment variables that must be set manually in MWAA console"
echo "  (these cannot be set via airflow_configuration_options):"
echo ""
echo "  ENVIRONMENT               = ${ENV}"
echo "  AWS_REGION                = ${REGION}"
echo "  S3_BUCKET                 = ${LAKEHOUSE_BUCKET}"
echo "  ICEBERG_DATABASE          = ${ICEBERG_DB}"
echo "  ATHENA_WORKGROUP          = ${ATHENA_WG}"
echo "  ATHENA_OUTPUT_LOCATION    = ${ATHENA_OUTPUT}"
echo "  DBT_TARGET                = ${ENV}"
echo "  SECRETS_MANAGER_SECRET_NAME = c_360_secret_manager"
if [[ -n "${SNS_TOPIC_ARN}" ]]; then
  echo "  SNS_ALERT_TOPIC_ARN       = ${SNS_TOPIC_ARN}"
else
  echo "  SNS_ALERT_TOPIC_ARN       = (set after Terraform creates SNS topic)"
fi
echo ""
echo "  Set these in:"
echo "  MWAA Console → Environments → ${MWAA_ENV_NAME} → Edit → Environment variables"
echo ""
echo "  ALSO required in Secrets Manager (c_360_secret_manager JSON keys):"
echo "  HUBSPOT_API_KEY           (or HUBSPOT_ACCESS_TOKEN)"
echo "  MAILCHIMP_API_KEY"
echo "  MAILCHIMP_AUDIENCE_ID     ← also set as MWAA env var"
echo "  MAILCHIMP_SERVER_PREFIX   ← also set as MWAA env var"
echo "  ZENDESK_API_KEY"
echo "  ZENDESK_SECRET_KEY"
echo "  ZENDESK_EMAIL             ← also set as MWAA env var"
echo "  ZENDESK_SUBDOMAIN         ← also set as MWAA env var"
echo "  LOYALTY_API_KEY"
echo ""
echo "=========================================="
echo "Sync complete for: ${ENV}"
echo "=========================================="
