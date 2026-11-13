# Production deployment runbook

Phases A–G. Each phase has an exit gate; do not start the next phase until
the gate is met and written down (date, execution ARN, who checked).
Setup steps M1–M11 are in `MANUAL_AWS_SETUP_GUIDE.md`.

Status words used in this repo:
**implemented** (code exists) → **tested** (automated tests pass) →
**deployed** (applied to an AWS environment) → **verified** (checked in that
environment with the commands below).

```bash
export AWS_REGION=us-east-1
SM_STG=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='c360-staging-batch-orchestrator'].stateMachineArn" --output text)
```

## A. Identity and bootstrap
Do M1–M4.
**Gate:** `aws sts get-caller-identity` shows `c360-admin`; `terraform output` in
`terraform/bootstrap` prints five role/policy ARNs; GitHub variables set.

## B. Staging infrastructure
Do M5 (review destroys), then M6 (merge → `deploy-staging`).
**Gate:** the `deploy-staging` job is green, including *Smoke test (dry-run
execution)*; the schedule is `DISABLED`.

## C. Alerts
Do M7.
**Gate:** the M7 test message and the deliberate-failure email both arrived.

## D. First real staging run (synthetic sources)
All six sources are in explicit `synthetic` mode (`source_modes` in
`environments/staging/main.tf`). Every raw record carries
`source_mode: "synthetic"`, so nothing downstream can mistake it for real data.

```bash
NAME=manual-$(date -u +%Y%m%dT%H%M%SZ)
aws stepfunctions start-execution --state-machine-arn "$SM_STG" --name "$NAME" --input '{}'
aws stepfunctions describe-execution --execution-arn "${SM_STG/stateMachine/execution}:$NAME" --query '[status,error,cause]'
```

Then check what was written (as `c360-operator` or `c360-admin`):

```bash
python scripts/validate_run.py --env staging --run-id "c360-$NAME" --dbt
aws s3 ls "s3://c360-bucket786/raw/" --recursive | grep "run_id=c360-$NAME"
```

**Gate:** execution `SUCCEEDED`; `validate_run.py` prints `RESULT: PASS`
(rows from this run > 0 for each source, no duplicate keys, revenue equal in
`fact_order`, `customer_360` and `mart_executive_kpis`).

Test T6 (DQ gate blocks a bad batch): redeploy with
`synthetic_include_invalid = true` on the lambda module (or set
`C360_SYNTHETIC_INCLUDE_INVALID=true` on `c360-staging-ingest` in the console
for the test), run once, and confirm the `oms` and `pim` branches fail at
`QualityCheck`, the run stops at `ReconcileRun` (oms is mandatory), nothing is
merged for this run_id, and the failure email arrives. Set it back afterwards.

## E. Move sources to real data, one at a time
For each source: create its secret (M8), change its mode to `"api"` (or
`"dataset"` for files under `s3://c360-bucket786/datasets/<source>/<entity>/`)
in a PR, deploy, run, validate. OMS and PIM: follow
`OMS_PIM_API_INTEGRATION_GUIDE.md` first.
**Gate:** one successful run per switched source with `source_mode` = `api`
in the raw records:

```bash
aws s3 cp "s3://c360-bucket786/<raw key from the Ingest step output>" - | head -1 | python -m json.tool | grep source_mode
```

## F. Three consecutive successful manual staging runs, then the schedule
Run D's commands on three different days (so incremental watermarks are
exercised) with the final source modes. Record each:

| # | Date | Execution ARN | Status | validate_run.py | Notes |
|---|---|---|---|---|---|
| 1 | | | | | |
| 2 | | | | | |
| 3 | | | | | |

**Gate:** three `SUCCEEDED` + `PASS` in a row, no unexplained warnings email.
Then set the staging environment variable `SCHEDULE_ENABLED=true` and re-run
the workflow for staging. Verify `aws scheduler get-schedule --name c360-staging-daily-batch --query State` is `ENABLED`.

## G. Production
Prerequisites: F complete; prod secrets created for every source in
`ENABLED_SOURCES` (prod requires `api` mode for every source, enforced by
`C360_REQUIRED_SOURCE_MODE=api`); `production` environment protected (M4).

1. Actions → *Customer 360 CI/CD* → *Run workflow* on `main` → target `prod`.
   Approve the `production` deployment. The job applies `module.core` first
   (buckets, KMS, Glue DB, workgroup), uploads artifacts, applies the rest and
   runs a dry run.
2. Confirm the prod alert subscription (same as M7 with `c360-prod-...`).
3. One manual prod run, `validate_run.py --env prod --dbt` → PASS.
4. Three consecutive successful manual prod runs (same table as F), then set
   `SCHEDULE_ENABLED=true` on the `production` environment and redeploy.

**Gate:** prod is called *verified* only after step 4.

## Daily operation

| Task | Command |
|---|---|
| Last runs | `aws stepfunctions list-executions --state-machine-arn "$SM" --max-results 5` |
| Why did it fail | `aws stepfunctions get-execution-history --execution-arn <arn> --reverse-order --max-results 20` |
| Re-run only failed steps | `aws stepfunctions redrive-execution --execution-arn <arn>` (same run_id, idempotent ingest/merge) |
| Rerun everything | start a new execution (new run_id; ingest continues from the checkpoint) |
| Pause the schedule | set `SCHEDULE_ENABLED=false` and redeploy, or `aws scheduler update-schedule` with `--state DISABLED` (re-apply restores code state) |
| Weekly compaction | `c360-<env>-weekly-iceberg-maintenance` (Sunday 04:00 UTC, disabled with the daily schedule) |
