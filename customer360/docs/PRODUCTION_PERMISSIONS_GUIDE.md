# Production permissions guide

Every identity, what it can do, where it is defined, and how to check it.
All workload roles are created by Terraform; the policies are in code, so
this file explains them rather than repeating the JSON.

## Identities

| Identity | Kind | Defined in | Used by | Can | Cannot |
|---|---|---|---|---|---|
| root | account root | — | nobody after M1 | everything | — (MFA on, no keys) |
| `c360-admin` | IAM user, console + MFA, no keys | manual (M1) | you, in CloudShell | AdministratorAccess | — |
| `c360-github-plan` | role, GitHub OIDC `…:pull_request` | `terraform/bootstrap/roles.tf` | PR `plan` job | read state, Describe/Get on infra | read lakehouse objects, read secrets, write anything except the state lock |
| `c360-github-deploy-staging` | role, OIDC `…:environment:staging` | `bootstrap/modules/deploy_role` | `deploy-staging` job | manage `c360-staging-*` resources, its state prefix, `artifacts/*` | touch prod, create a role without the boundary, edit the boundary |
| `c360-github-deploy-prod` | role, OIDC `…:environment:production` | same module | `deploy-prod` job | manage `c360-prod-*`, create the prod buckets and the tagged KMS key | touch staging, same IAM limits |
| `c360-operator` | role, trusts user `C360` with MFA | `bootstrap/roles.tf` | people running/inspecting the pipeline | start/stop/redrive executions, Athena queries, read lakehouse, logs, metrics, test SNS | IAM, read secrets, change infrastructure |
| `C360` | IAM user | existing | operator laptop | after M9/M10: only `sts:AssumeRole` on `c360-operator` + manage own MFA | everything else |
| `c360-<env>-lambda-<fn>` (8) | Lambda roles | `modules/lambda/main.tf` | each function | see below | anything outside its prefixes |
| `c360-<env>-step-functions-execution` | role | `modules/step_functions` | state machine | invoke the 7 batch Lambdas, run the dbt Glue job (.sync), logs, X-Ray | — |
| `c360-<env>-glue-execution` | role | `modules/glue_jobs` | dbt Glue job | read `artifacts/*`, read/write dbt + Athena prefixes, Glue catalog, Athena in its workgroup, KMS (prod) | read raw/silver, secrets |
| `c360-<env>-scheduler-execution` | role | `modules/eventbridge` | EventBridge Scheduler | `states:StartExecution` on the state machine, invoke the maintenance Lambda | — |

## Guard rails

**Permissions boundary `c360-workload-boundary`** (`bootstrap/boundary.tf`).
Every role CI creates must carry it: the deploy roles may call
`iam:CreateRole`/`PutRolePolicy`/`AttachRolePolicy` only with the condition
`iam:PermissionsBoundary = <boundary ARN>`, and may not delete or edit the
boundary. The boundary allows only Customer 360 buckets, `customer360_*`
Glue databases, `customer360-*` workgroups, `c360-*` jobs/functions/topics,
`customer360/*` secrets and KMS keys tagged `Project=Customer360`, and denies
`iam:*`, `sts:*`, `organizations:*`, `account:*`. So even a compromised deploy
role cannot mint an admin role.

**OIDC subject pinning.** The deploy roles trust only
`repo:<owner>/<repo>:environment:<env>`; the `production` environment only
accepts `main` and (on plans that support it) needs a reviewer. Forks and
other branches cannot assume them.

**No long-lived keys in CI.** GitHub holds role ARNs in variables, never keys.

## Lambda roles (least privilege per function)

| Function | S3 read | S3 write | Other |
|---|---|---|---|
| initialize | — | — | `states:ListExecutions` on its own state machine (concurrency guard) |
| ingest | `checkpoints/ raw/ metadata/` | same | `secretsmanager:GetSecretValue` on `customer360/<env>/*` (+ legacy secret in staging) |
| silver_gate | `raw/ silver/` | `silver/ quarantine/` | — |
| quality_check | `silver/ quarantine/` | — | — |
| iceberg_load | `silver/ <iceberg>/ athena-results/` | `<iceberg>/ athena-results/` | Glue catalog + Athena in the env DB/workgroup |
| iceberg_maintenance | same as load | same + delete on `<iceberg>/` | same |
| reconcile | — | — | — |
| notify | — | — | `sns:Publish` on the alert topic |

All of them: own log group, `cloudwatch:PutMetricData` limited to namespace
`Customer360/Pipeline`, KMS on the lakehouse key in prod.

## How to verify

```bash
# who can assume a deploy role
aws iam get-role --role-name c360-github-deploy-prod --query 'Role.AssumeRolePolicyDocument'
# every workload role has the boundary
aws iam list-roles --query "Roles[?starts_with(RoleName,'c360-staging-')].[RoleName,PermissionsBoundary.PermissionsBoundaryArn]" --output table
# simulate: can the ingest role read raw data of prod? (expect implicitDeny)
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::$ACCOUNT_ID:role/c360-staging-lambda-ingest \
  --action-names s3:GetObject --resource-arns arn:aws:s3:::c360-prod-lakehouse/raw/x \
  --query 'EvaluationResults[0].EvalDecision'
# can the staging deploy role create a role without the boundary? (expect implicitDeny)
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::$ACCOUNT_ID:role/c360-github-deploy-staging \
  --action-names iam:CreateRole --resource-arns arn:aws:iam::$ACCOUNT_ID:role/c360-staging-x \
  --query 'EvaluationResults[0].EvalDecision'
```

## Known limits

* IAM Access Analyzer policy validation has not been run on these policies;
  run `aws accessanalyzer validate-policy` on the rendered JSON after M3.
* `cloudwatch:PutMetricData`, `logs:DescribeLogGroups`, `xray:*` and a few
  `List*` calls have no resource-level permissions and use `*`.
* The plan role can list (not read) object keys in the lakehouse buckets,
  because `HeadBucket` needs `s3:ListBucket`.
