#!/usr/bin/env bash
# Customer 360 AWS doctor: read-only checks that say what exists, what is
# missing, and the command that fixes it. It never changes anything.
#
#   bash scripts/aws_doctor.sh account            # before/after bootstrap (run as c360-admin in CloudShell)
#   bash scripts/aws_doctor.sh env staging        # after a deploy (CloudShell, or CI with the deploy role)
#   bash scripts/aws_doctor.sh env prod
#
# Exit code: 0 = no FAIL, 1 = at least one FAIL. WARN means "look at this".
set -uo pipefail

REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
export AWS_REGION="$REGION" AWS_PAGER=""
FAILS=0 WARNS=0

pass() { printf '  PASS  %s\n' "$1"; }
warn() { printf '  WARN  %s\n' "$1"; [ -n "${2:-}" ] && printf '        fix: %s\n' "$2"; WARNS=$((WARNS + 1)); }
fail() { printf '  FAIL  %s\n' "$1"; [ -n "${2:-}" ] && printf '        fix: %s\n' "$2"; FAILS=$((FAILS + 1)); }
section() { printf '\n== %s\n' "$1"; }
q() { aws "$@" --output text 2>/dev/null; }   # quiet query; empty output on error

ACCOUNT_ID=$(q sts get-caller-identity --query Account)
CALLER=$(q sts get-caller-identity --query Arn)
if [ -z "$ACCOUNT_ID" ]; then
  echo "Cannot call sts:GetCallerIdentity: no AWS credentials in this shell."; exit 1
fi
echo "Account $ACCOUNT_ID  region $REGION  caller $CALLER"
echo "(A FAIL can also mean this caller may not read the resource; c360-admin gives the authoritative answer.)"

# ─────────────────────────────────────────────────────────────────────────────
account_checks() {
  section "Identity hygiene"
  case "$CALLER" in
    *":root") fail "running as root" "sign in as c360-admin (MANUAL_AWS_SETUP_GUIDE.md M1)";;
    *) pass "not root ($CALLER)";;
  esac
  local root_keys root_mfa
  root_keys=$(q iam get-account-summary --query 'SummaryMap.AccountAccessKeysPresent')
  root_mfa=$(q iam get-account-summary --query 'SummaryMap.AccountMFAEnabled')
  [ "$root_keys" = "0" ] && pass "root has no access keys" || fail "root access keys present (=$root_keys)" "root → Security credentials → deactivate, then delete"
  [ "$root_mfa" = "1" ] && pass "root MFA enabled" || fail "root MFA not enabled" "root → Security credentials → Assign MFA device"
  if [ -n "$(q iam get-user --user-name c360-admin --query User.UserName)" ]; then
    [ -n "$(q iam list-mfa-devices --user-name c360-admin --query 'MFADevices[0].SerialNumber')" ] && pass "c360-admin has MFA" || fail "c360-admin has no MFA" "IAM → Users → c360-admin → Assign MFA device"
    [ "$(q iam list-access-keys --user-name c360-admin --query 'length(AccessKeyMetadata)')" = "0" ] && pass "c360-admin has no access keys" || warn "c360-admin has access keys" "use CloudShell; aws iam delete-access-key --user-name c360-admin --access-key-id <id>"
  else
    fail "user c360-admin does not exist" "MANUAL_AWS_SETUP_GUIDE.md M1"
  fi
  local c360_keys
  c360_keys=$(q iam list-access-keys --user-name C360 --query 'length(AccessKeyMetadata)')
  if [ -n "$c360_keys" ]; then
    local pols; pols=$(q iam list-attached-user-policies --user-name C360 --query 'AttachedPolicies[].PolicyName')
    echo "        C360: $c360_keys access key(s); attached policies: ${pols:-none}"
    echo "$pols" | grep -qvE '^(c360-operator-assume)?$' && warn "operator user C360 still has broad policies" "M9 then M10"
  fi

  section "GitHub OIDC"
  local oidc; oidc=$(q iam list-open-id-connect-providers --query "OpenIDConnectProviderList[?contains(Arn,'token.actions.githubusercontent.com')].Arn")
  if [ -n "$oidc" ]; then
    pass "OIDC provider $oidc"
    q iam get-open-id-connect-provider --open-id-connect-provider-arn "$oidc" --query ClientIDList | grep -q sts.amazonaws.com \
      && pass "OIDC audience sts.amazonaws.com" || fail "OIDC provider lacks audience sts.amazonaws.com" "aws iam add-client-id-to-open-id-connect-provider --open-id-connect-provider-arn $oidc --client-id sts.amazonaws.com"
  else
    fail "no GitHub OIDC provider" "aws iam create-open-id-connect-provider --url https://token.actions.githubusercontent.com --client-id-list sts.amazonaws.com"
  fi

  section "Terraform state buckets"
  for b in c360-staging-tfstate c360-prod-tfstate; do
    local v; v=$(q s3api get-bucket-versioning --bucket "$b" --query Status)
    if aws s3api head-bucket --bucket "$b" >/dev/null 2>&1; then
      [ "$v" = "Enabled" ] && pass "$b exists, versioning on" || fail "$b versioning '$v'" "aws s3api put-bucket-versioning --bucket $b --versioning-configuration Status=Enabled"
    else
      [ "$b" = "c360-staging-tfstate" ] && fail "$b missing" "aws s3api create-bucket --bucket $b && aws s3api put-bucket-versioning --bucket $b --versioning-configuration Status=Enabled" \
                                       || warn "$b missing (created by terraform/bootstrap)" "terraform -chdir=terraform/bootstrap apply"
    fi
  done
  for b in c360-prod-lakehouse c360-prod-artifacts; do
    local out; out=$(aws s3api head-bucket --bucket "$b" 2>&1)
    if [ -z "$out" ]; then pass "$b exists and is yours"
    elif echo "$out" | grep -q '404'; then pass "$b name is free (created by the first prod deploy)"
    else fail "$b exists in another account (name taken)" "choose new names in terraform/modules/core and bootstrap deploy_prod"; fi
  done

  section "Bootstrap identities"
  for r in c360-github-plan c360-github-deploy-staging c360-github-deploy-prod c360-operator; do
    local sub; sub=$(q iam get-role --role-name "$r" --query 'Role.AssumeRolePolicyDocument.Statement[0].Condition.StringEquals."token.actions.githubusercontent.com:sub"')
    if [ -n "$(q iam get-role --role-name "$r" --query Role.RoleName)" ]; then pass "$r exists ${sub:+(sub $sub)}"
    else fail "$r missing" "terraform -chdir=terraform/bootstrap apply (M3)"; fi
  done
  [ -n "$(q iam get-policy --policy-arn "arn:aws:iam::$ACCOUNT_ID:policy/c360-workload-boundary" --query Policy.PolicyName)" ] \
    && pass "c360-workload-boundary exists" || fail "c360-workload-boundary missing" "terraform -chdir=terraform/bootstrap apply (M3)"

  section "Legacy roles (must be removed or adopted before CI can manage staging)"
  local roles; roles=$(q iam list-roles --query "Roles[?starts_with(RoleName,'c360-') || starts_with(RoleName,'customer360') || RoleName=='GitHubActionsStagingRole'].RoleName")
  for r in $roles; do
    case "$r" in c360-github-*|c360-operator) continue;; esac
    local pb; pb=$(q iam get-role --role-name "$r" --query Role.PermissionsBoundary.PermissionsBoundaryArn)
    local last; last=$(q iam get-role --role-name "$r" --query Role.RoleLastUsed.LastUsedDate)
    if [ -z "$pb" ] || [ "$pb" = "None" ]; then
      warn "$r has no permissions boundary (last used ${last:-never})" "the first staging apply must run as c360-admin (M5/M6); CI cannot detach policies from it"
    else pass "$r has the boundary"; fi
  done

  section "Lake Formation"
  local admins; admins=$(q lakeformation get-data-lake-settings --query 'DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier')
  local dbdef; dbdef=$(q lakeformation get-data-lake-settings --query 'DataLakeSettings.CreateDatabaseDefaultPermissions[].Principal.DataLakePrincipalIdentifier')
  echo "        data lake admins: ${admins:-none}"
  echo "$dbdef" | grep -q IAM_ALLOWED_PRINCIPALS && pass "IAM-only mode for new databases (IAM_ALLOWED_PRINCIPALS)" \
    || warn "new databases are not in IAM-only mode" "grant the c360 roles in Lake Formation, or restore IAM_ALLOWED_PRINCIPALS defaults (do not change without review)"
}

# ─────────────────────────────────────────────────────────────────────────────
env_checks() {
  local env="$1" p="c360-$1" db wg bucket artifacts
  case "$env" in
    staging) db=customer360_iceberg_stage; wg=customer360-staging; bucket=c360-bucket786; artifacts=c360-bucket786;;
    prod)    db=customer360_prod;          wg=customer360-prod;    bucket=c360-prod-lakehouse; artifacts=c360-prod-artifacts;;
    *) echo "env must be staging or prod"; exit 2;;
  esac
  local sm_arn="arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine:$p-batch-orchestrator"

  section "State machine ($env)"
  if [ "$(q stepfunctions describe-state-machine --state-machine-arn "$sm_arn" --query status)" = "ACTIVE" ]; then
    pass "$p-batch-orchestrator ACTIVE"
    local lvl; lvl=$(q stepfunctions describe-state-machine --state-machine-arn "$sm_arn" --query loggingConfiguration.level)
    [ "$lvl" != "OFF" ] && pass "logging level $lvl" || warn "state machine logging OFF"
  else
    fail "$p-batch-orchestrator not found" "deploy $env (CI deploy job, or first apply as c360-admin: M6)"
  fi

  section "Lambda functions ($env)"
  local versions=""
  for fn in initialize ingest silver_gate quality_check iceberg_load reconcile notify iceberg_maintenance; do
    local name="$p-$fn" st ver role pb
    st=$(q lambda get-function --function-name "$name" --query 'Configuration.[State,LastUpdateStatus,Runtime]')
    if [ -z "$st" ]; then fail "$name missing" "deploy $env"; continue; fi
    ver=$(q lambda get-function-configuration --function-name "$name" --query 'Environment.Variables.C360_ARTIFACT_VERSION')
    role=$(q lambda get-function-configuration --function-name "$name" --query Role); role=${role##*/}
    pb=$(q iam get-role --role-name "$role" --query Role.PermissionsBoundary.PermissionsBoundaryArn)
    versions="$versions $ver"
    if echo "$st" | grep -q "Active" && echo "$st" | grep -q "Successful"; then
      pass "$name $(echo $st) artifact=$ver role=$role"
    else fail "$name state: $st" "aws lambda get-function --function-name $name"; fi
    { [ -z "$pb" ] || [ "$pb" = "None" ]; } && fail "$role has no permissions boundary" "re-apply $env from this branch"
    [ -n "$ver" ] && [ "$ver" != "None" ] && ! aws s3api head-object --bucket "$artifacts" --key "artifacts/$ver/lambdas/$fn.zip" >/dev/null 2>&1 \
      && fail "artifact s3://$artifacts/artifacts/$ver/lambdas/$fn.zip missing" "re-run the deploy job (it uploads artifacts before apply)"
  done
  [ "$(echo $versions | tr ' ' '\n' | sort -u | wc -l)" -le 1 ] && pass "all functions on one artifact version" || warn "functions run different artifact versions:$versions"
  local modes; modes=$(q lambda get-function-configuration --function-name "$p-ingest" --query 'Environment.Variables.C360_SOURCE_MODES')
  [ -n "$modes" ] && echo "        source modes: $modes"
  [ "$env" = prod ] && { [ "$(q lambda get-function-configuration --function-name "$p-ingest" --query 'Environment.Variables.C360_REQUIRED_SOURCE_MODE')" = "api" ] \
    && pass "prod requires api mode" || fail "prod C360_REQUIRED_SOURCE_MODE is not api" "re-apply prod"; }

  section "Glue / Athena ($env)"
  local script; script=$(q glue get-job --job-name "$p-dbt-runner" --query Job.Command.ScriptLocation)
  if [ -n "$script" ]; then
    pass "Glue job $p-dbt-runner ($script)"
    local key=${script#s3://*/}; local b=${script#s3://}; b=${b%%/*}
    aws s3api head-object --bucket "$b" --key "$key" >/dev/null 2>&1 && pass "Glue script object exists" || fail "Glue script $script missing" "re-run the deploy job"
    local last; last=$(q glue get-job-runs --job-name "$p-dbt-runner" --max-results 1 --query 'JobRuns[0].[JobRunState,StartedOn]')
    echo "        last dbt run: ${last:-none}"
  else fail "Glue job $p-dbt-runner missing" "deploy $env"; fi
  [ -n "$(q glue get-database --name "$db" --query Database.Name)" ] && pass "Glue database $db" || fail "Glue database $db missing" "deploy $env"
  [ "$(q athena get-work-group --work-group "$wg" --query WorkGroup.State)" = "ENABLED" ] && pass "Athena workgroup $wg" || fail "Athena workgroup $wg missing/disabled" "deploy $env"
  aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1 && pass "lakehouse bucket $bucket" || fail "lakehouse bucket $bucket not reachable"

  section "Schedules ($env)"
  for s in "$p-daily-batch" "$p-weekly-iceberg-maintenance"; do
    local state; state=$(q scheduler get-schedule --name "$s" --query State)
    if [ -n "$state" ]; then pass "$s $state"; else fail "$s missing" "deploy $env"; fi
  done

  section "Alerting ($env)"
  local topic="arn:aws:sns:$REGION:$ACCOUNT_ID:$p-pipeline-alerts"
  local subs; subs=$(q sns list-subscriptions-by-topic --topic-arn "$topic" --query 'Subscriptions[].[Protocol,Endpoint,SubscriptionArn]')
  if [ -z "$subs" ]; then fail "no subscriptions on $p-pipeline-alerts (or topic missing)" "set GitHub variable ALERT_EMAIL and redeploy"
  else
    while read -r proto ep arn; do
      [ "$arn" = "PendingConfirmation" ] && warn "$proto $ep not confirmed: alerts are not delivered yet" "click the link in the AWS confirmation email (M7)" || pass "$proto $ep confirmed"
    done <<< "$subs"
  fi
  [ "$(q events describe-rule --name "$p-batch-execution-failed" --query State)" = "ENABLED" ] && pass "EventBridge failure rule enabled" || fail "EventBridge rule $p-batch-execution-failed missing/disabled" "deploy $env"
  [ -n "$(q events list-targets-by-rule --rule "$p-batch-execution-failed" --query 'Targets[0].Arn')" ] && pass "failure rule targets SNS" || fail "failure rule has no target" "set ALERT_EMAIL/alert topic and redeploy"
  local alarms; alarms=$(q cloudwatch describe-alarms --alarm-name-prefix "$p-" --query 'MetricAlarms[].[AlarmName,StateValue]')
  if [ -n "$alarms" ]; then
    while read -r n s; do [ "$s" = "ALARM" ] && warn "alarm $n is in ALARM" || pass "alarm $n $s"; done <<< "$alarms"
  else warn "no CloudWatch alarms with prefix $p-"; fi

  section "Recent executions ($env)"
  local ex; ex=$(q stepfunctions list-executions --state-machine-arn "$sm_arn" --max-results 5 --query 'executions[].[name,status,startDate]')
  if [ -n "$ex" ]; then echo "$ex" | sed 's/^/        /'
    local ok; ok=$(echo "$ex" | awk '$2=="SUCCEEDED" && $1 !~ /^smoke-/' | wc -l)
    echo "        successful non-smoke runs in the last 5: $ok (schedule needs 3 in a row)"
  else echo "        none yet"; fi
}

case "${1:-}" in
  account) account_checks;;
  env) env_checks "${2:-}";;
  *) sed -n '2,10p' "$0"; exit 2;;
esac

printf '\nSummary: %d FAIL, %d WARN\n' "$FAILS" "$WARNS"
[ "$FAILS" -eq 0 ]
