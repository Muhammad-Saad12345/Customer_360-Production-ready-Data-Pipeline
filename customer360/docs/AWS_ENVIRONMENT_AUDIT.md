# AWS environment audit — 18 Nov 2026

Read-only inspection of account `734143568343`, `us-east-1`, through the AWS
connector (signed in as root; nothing was changed). Re-check any time with
`bash scripts/aws_doctor.sh account` and `... env staging`.

## What exists

| Area | Found | Expected by this repo | Status |
|---|---|---|---|
| Terraform state `c360-staging-tfstate` | versioned, **empty**, no bucket policy | staging state | Nothing was ever applied by Terraform |
| `c360-prod-tfstate`, `c360-prod-lakehouse`, `c360-prod-artifacts` | do not exist; names free (404) | created by bootstrap / first prod deploy | OK |
| GitHub OIDC provider | exists, audience `sts.amazonaws.com` | reused by bootstrap | OK |
| Bootstrap roles and boundary (`c360-github-*`, `c360-operator`, `c360-workload-boundary`) | none | M3 | **Missing** |
| `c360-admin` user | none | M1 | **Missing** |
| Lambda functions, state machine, Glue job, schedules, EventBridge rules, alarms, KMS keys | none | created by the staging deploy | **Missing** (never deployed) |
| Glue DB `customer360_iceberg_stage` | 6 Iceberg tables at `s3://c360-bucket786/iceberg/<table>`, default permission `IAM_ALLOWED_PRINCIPALS: ALL` | imported by staging Terraform | OK, matches |
| Athena workgroup `customer360-staging` | engine v3, results `staging-athena-results/`, not enforced | imported; Terraform sets `athena-results/`, enforced, 10 GiB cutoff | Will be updated in place |
| Bucket `c360-bucket786` | versioned, SSE-S3, public access blocked | lakehouse bucket | OK |
| Secret `c_360_secret_manager` | exists (last read 25 Oct) | legacy shared secret, staging only | OK |
| Lake Formation | IAM-only defaults; only admin is `AmazonSageMakerAdminIAMExecutionRole` | leave unchanged | OK |
| Checkpoints `checkpoints/*/*.json` | moved on 11 Nov by the **old synthetic fallback** (raw OMS/PIM files contain `OMS-…`/`PIM-…` records) | — | **Reset before a source goes to `api`** |

Conclusion: because the staging state is empty and no `c360-staging-*` roles
exist, there is nothing to migrate. The first staging apply can be done by CI
with the bounded deploy role (M6 path A).

## Security findings (fix in this order)

| # | Finding | Risk | Fix |
|---|---|---|---|
| S1 | User `C360` (active key, last used 11 Nov, no MFA) has inline policy `Antigravity_grant_permission_for_autonomous_capability` allowing `iam:PutRolePolicy`/`AttachRolePolicy` on `role/C360-*`, and `C360-Antigravity-DeployRole` trusts `C360` without MFA | **privilege escalation to admin** from one access key | C1 below, today |
| S2 | `GitHubActionsStagingRole`: 6 `*FullAccess` policies (S3, Lambda, Step Functions, Glue console, SNS, Logs), trusts any ref/workflow of this repository (`sub` = `repo:…:*`), never used | any branch or workflow in the repo could act broadly | C2 |
| S3 | `github-mwaa-deploy-role`: `AmazonS3FullAccess` + `AWSCloudFormationFullAccess`, trust points at a malformed provider ARN, never used | dead, over-privileged | C2 |
| S4 | `C360-Antigravity-DeployRole`: can delete state machines, write `c360-staging-tfstate/*` and the data bucket, pass the MWAA role; last used 22 Oct | can corrupt Terraform state | C2 |
| S5 | `C360-MWAA-Execution-Role`, bucket `c360-mwaa-prod-786`, topic `c360-pipeline-alerts`: MWAA has no environments | leftovers | C3 |
| S6 | `C360` has `AmazonS3FullAccess`, `AmazonAthenaFullAccess`, `AWSGlueConsoleFullAccess` and 4 Glue inline policies | broad standing access on a long-lived key | M9–M10 |

Why the old OIDC test failed (21 Oct): the error was `Not authorized to perform
sts:AssumeRoleWithWebIdentity` while the role's trust is correct for the
repository's **current** name. GitHub reports the repository as moved from
`customer_360-production-ready-data-pipeline`; the `sub` claim is
case-sensitive, so the most likely cause is that the repository had the
lower-case name at the time. Keep the repository name stable: renaming it
breaks every OIDC trust in `terraform/bootstrap` until `github_repository` is
updated and bootstrap is re-applied.

## Cleanup commands (run as c360-admin in CloudShell, after M1)

Each block saves what it deletes into `~/c360-iam-backup/` first.

**C1 — close the escalation path (today, 2 minutes).** MWAA has no
environments, so the Airflow and log permissions in these two policies have
nothing left to act on.

```bash
mkdir -p ~/c360-iam-backup && cd ~/c360-iam-backup
for p in Antigravity_grant_permission_for_autonomous_capability cloudwatch_mwaa_permission; do
  aws iam get-user-policy --user-name C360 --policy-name "$p" > "C360-$p.json"
  aws iam delete-user-policy --user-name C360 --policy-name "$p"
done
aws iam list-user-policies --user-name C360
```

**C2 — delete unused or over-privileged roles** (check `RoleLastUsed` again
right before; stop if anything used it in the last few days):

```bash
cd ~/c360-iam-backup
delete_role() {
  local r="$1"
  aws iam get-role --role-name "$r" > "role-$r.json"
  aws iam get-role --role-name "$r" --query 'Role.RoleLastUsed'
  for arn in $(aws iam list-attached-role-policies --role-name "$r" --query 'AttachedPolicies[].PolicyArn' --output text); do
    echo "$arn" >> "role-$r.attached.txt"; aws iam detach-role-policy --role-name "$r" --policy-arn "$arn"
  done
  for p in $(aws iam list-role-policies --role-name "$r" --query 'PolicyNames' --output text); do
    aws iam get-role-policy --role-name "$r" --policy-name "$p" > "role-$r-$p.json"
    aws iam delete-role-policy --role-name "$r" --policy-name "$p"
  done
  for ip in $(aws iam list-instance-profiles-for-role --role-name "$r" --query 'InstanceProfiles[].InstanceProfileName' --output text); do
    aws iam remove-role-from-instance-profile --instance-profile-name "$ip" --role-name "$r"
  done
  aws iam delete-role --role-name "$r" && echo "deleted $r"
}
delete_role GitHubActionsStagingRole
delete_role github-mwaa-deploy-role
delete_role C360-Antigravity-DeployRole      # if a tool still needs AWS access, give it c360-operator instead
```

Restore, if ever needed: `aws iam create-role --role-name <r> --assume-role-policy-document "$(jq '.Role.AssumeRolePolicyDocument' role-<r>.json)"`,
then re-attach the ARNs in `role-<r>.attached.txt` and `put-role-policy` the saved inline JSON (`.PolicyDocument`).

**C3 — MWAA leftovers** (optional; storage cost only):

```bash
aws mwaa list-environments                                  # must be []
delete_role C360-MWAA-Execution-Role
aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:us-east-1:734143568343:c360-pipeline-alerts
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:734143568343:c360-pipeline-alerts
aws s3 ls s3://c360-mwaa-prod-786 --recursive --summarize | tail -2
# keep a copy, then remove: aws s3 sync s3://c360-mwaa-prod-786 ./mwaa-bucket-backup && aws s3 rb s3://c360-mwaa-prod-786 --force
```

If `rb --force` fails with `BucketNotEmpty`, the bucket keeps old object
versions: empty it in the console (*Empty*) first.

**C4 — reset a checkpoint before switching that source to `api` or `dataset`**
(the 11 Nov watermarks came from synthetic data, so a real backfill would
otherwise skip everything older):

```bash
SRC=oms; ENT=orders
aws s3 cp s3://c360-bucket786/checkpoints/$SRC/$ENT.json ~/c360-iam-backup/checkpoint-$SRC-$ENT.json
aws s3 rm s3://c360-bucket786/checkpoints/$SRC/$ENT.json   # next run starts from 1970 (full backfill)
```

HubSpot's November raw file looks like real CRM data (no synthetic markers), so
its checkpoint can stay.
