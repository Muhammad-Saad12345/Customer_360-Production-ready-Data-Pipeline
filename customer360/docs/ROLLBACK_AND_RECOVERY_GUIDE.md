# Rollback and recovery guide

## 1. Bad deploy (code or Terraform)

Every deploy is tied to one artifact version (the 12-character commit SHA) and
artifacts are never overwritten (`artifacts/<version>/...`). To roll back,
deploy the previous good commit:

* Preferred: revert the commit on `main` (`git revert <sha>` in a PR). The
  push redeploys staging; prod is redeployed with *Run workflow → prod*.
* Fast path (staging): Actions → the last green run of *Customer 360 CI/CD* →
  *Re-run all jobs*. It re-applies that commit's Terraform with that commit's
  artifacts, which are still in S3.

Check: `aws lambda get-function --function-name c360-staging-ingest --query 'Configuration.Environment.Variables.C360_ARTIFACT_VERSION'`.

## 2. A run failed

The state machine does not catch failures at the top level, so a failed
execution can be **redriven** from the failed state with the same input and run_id:

```bash
aws stepfunctions describe-execution --execution-arn <arn> --query '[status,error,cause]'
aws stepfunctions redrive-execution --execution-arn <arn>
```

Safe because: ingest returns its earlier result when its checkpoint already
records this run_id; the silver gate rewrites the same run_id paths; the
Iceberg load is a MERGE on the primary key; dbt rebuilds tables.

| Failure | What to do |
|---|---|
| `ConcurrentRunError` | another execution is running; wait, do not stop it unless it is stuck |
| `SourceModeError` | configuration: missing/invalid mode, missing secret, or a non-`api` mode in prod |
| `SourceRequestError` | the source API rejected the request (bad credentials, 4xx); fix the secret and redrive |
| `TransientError` (after retries) | source or Athena throttled/unavailable; redrive later |
| `DataQualityGateFailed` | ≥10% of the batch rejected; inspect `quarantine/<source>/<entity>/.../run_id=<id>/invalid_records.json` before anything else |
| `ReconciliationFailed` | a mandatory source (hubspot, oms) failed; the cause lists which one |
| `RawWriteError` | S3 write failed (permissions/KMS); the checkpoint did not move, redrive after fixing |
| DbtRun/DbtTest failed | Glue job logs: `/aws-glue/python-jobs/output` and `/error`; the Iceberg data is already loaded and correct |

## 3. Bad data was loaded

1. Stop the schedule (`SCHEDULE_ENABLED=false` + redeploy, or disable it in the console).
2. Find the run: every table has `run_id`.
3. Iceberg time travel to inspect: `select * from <db>.<table> for timestamp as of timestamp '2026-11-13 00:00:00 UTC' limit 10`.
4. Remove the run's rows: `delete from <db>.<table> where run_id = '<run_id>'`
   (Athena supports DELETE on Iceberg). Then rebuild the dbt models by running
   the Glue job `c360-<env>-dbt-runner` from the console with
   `--dbt-action both` and `--dbt-target <env>`.
5. If the checkpoint moved past data you want to re-ingest, edit
   `s3://<bucket>/checkpoints/<source>/<entity>.json` (copy it first) and set
   `last_successful_watermark` back.

Snapshots older than the maintenance retention (7 days) are expired, so time
travel only covers the last week.

## 4. Infrastructure state problems

* Never `terraform force-unlock` a lock you did not create; the S3 lock file
  (`<key>.tflock`) belongs to a running job.
* State buckets are versioned: a corrupted state can be restored with
  `aws s3api list-object-versions --bucket c360-staging-tfstate --prefix customer360/staging/` and
  copying the previous version over the current object.
* Bootstrap state is separate (`bootstrap/terraform.tfstate`) and CI cannot touch it.

## 5. Credentials

* Suspected key leak for `C360`: `aws iam update-access-key --user-name C360 --access-key-id <id> --status Inactive`, then rotate.
* GitHub compromise: delete the variables `AWS_*_ROLE_ARN` (stops CI), or add a
  `Deny` statement to the deploy roles' trust policy.
