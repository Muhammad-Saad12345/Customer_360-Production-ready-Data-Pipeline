#!/usr/bin/env bash
# Start a dry-run execution and wait for it. A dry run invokes every Lambda in
# the batch path (resolving each source's mode) but writes no data and does
# not start dbt. Exit code 0 only if the execution SUCCEEDED.
#
#   bash scripts/smoke_dry_run.sh <state-machine-arn> [timeout-seconds]
set -euo pipefail
SM_ARN="$1"
TIMEOUT="${2:-600}"
NAME="smoke-$(date -u +%Y%m%dT%H%M%SZ)"

EXEC_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn "$SM_ARN" --name "$NAME" \
  --input '{"dry_run": true}' --query executionArn --output text)
echo "Started $EXEC_ARN"

DEADLINE=$(( $(date +%s) + TIMEOUT ))
while true; do
  STATUS=$(aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --query status --output text)
  [[ "$STATUS" != "RUNNING" ]] && break
  if (( $(date +%s) > DEADLINE )); then
    echo "Timed out after ${TIMEOUT}s; stopping $EXEC_ARN"
    aws stepfunctions stop-execution --execution-arn "$EXEC_ARN" --cause "smoke test timeout" >/dev/null
    exit 1
  fi
  sleep 10
done

echo "Status: $STATUS"
if [[ "$STATUS" != "SUCCEEDED" ]]; then
  aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --query '{error:error,cause:cause}' --output json
  exit 1
fi
