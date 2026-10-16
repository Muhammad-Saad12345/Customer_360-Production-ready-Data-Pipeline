# Customer 360 IAM Least-Privilege Permissions Matrix

## 1. Overview & Security Principles

This document specifies the least-privilege IAM architecture implemented for the Customer 360 serverless Lakehouse pipeline.

### Core Security Guarantees:
1. **Zero Wildcard Actions (`Action: "*"`):** All permissions are strictly enumerated to the minimum required AWS API actions.
2. **Narrow Resource Scoping:** Every policy explicitly restricts access to dedicated S3 bucket paths, Glue databases, Athena workgroups, and CloudWatch log groups.
3. **Role Separation by Service:** Rather than a monolithic execution role, each service component operates under its own isolated IAM principal.
4. **Keyless CI/CD Deployment:** GitHub Actions uses OpenID Connect (OIDC) Web Identity Federation to assume temporary AWS credentials without static IAM access keys.

---

## 2. Role Directory

| Role Name | Trusted Service Principal | Managed By | Purpose |
| :--- | :--- | :--- | :--- |
| `c360-{env}-lambda-execution` | `lambda.amazonaws.com` | `modules/lambda` | Executes the 8 pipeline Lambda handlers |
| `c360-{env}-step-functions-execution` | `states.amazonaws.com` | `modules/step_functions` | State Machine orchestration, Lambda invoke, Glue job trigger |
| `c360-{env}-glue-execution` | `glue.amazonaws.com` | `modules/glue_jobs` | Runs dbt transformation models via Glue Python Shell |
| `c360-{env}-scheduler-execution` | `scheduler.amazonaws.com` | `modules/eventbridge` | Triggers daily Step Functions execution |
| `c360-{env}-github-deploy` | `token.actions.githubusercontent.com` | `modules/iam` | CI/CD Terraform apply & artifact syncing |

---

## 3. Detailed Component Permissions

### A. Lambda Execution Role (`c360-{env}-lambda-execution`)

| Policy Name | Action(s) | Scoped Resource(s) | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **`lambda-logging`** | `logs:CreateLogGroup`<br>`logs:CreateLogStream`<br>`logs:PutLogEvents`<br>`logs:DescribeLogStreams` | `arn:aws:logs:{region}:{account}:log-group:/aws/lambda/c360-{env}-*:*` | Standard Lambda logging; scoped strictly to project functions. |
| **`lambda-s3`** | `s3:GetObject`<br>`s3:PutObject`<br>`s3:DeleteObject`<br>`s3:ListBucket`<br>`s3:GetBucketLocation`<br>`s3:GetObjectVersion` | `arn:aws:s3:::{lakehouse_bucket}`<br>`arn:aws:s3:::{lakehouse_bucket}/*` | Read raw/silver data, write parquet/quarantine files, manage Iceberg files. |
| **`lambda-glue-athena`** | `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:DeleteTable`<br>`glue:GetPartition`, `glue:CreatePartition`, `glue:UpdatePartition` | `catalog`<br>`database/{glue_db}`<br>`table/{glue_db}/*` | Iceberg table registration and automated schema evolution via Glue Catalog. |
|  | `athena:StartQueryExecution`<br>`athena:StopQueryExecution`<br>`athena:GetQueryExecution`<br>`athena:GetQueryResults`<br>`athena:GetWorkGroup` | `arn:aws:athena:{region}:{account}:workgroup/{workgroup}` | Executes Iceberg MERGE INTO, OPTIMIZE, and VACUUM queries in Athena. |
| **`lambda-observability`** | `cloudwatch:PutMetricData` | `*` (Condition: `Namespace = Customer360/Pipeline`) | Emits custom DQ gate and task outcome metrics. Namespace condition prevents writing to unauthorized namespaces. |
|  | `sns:Publish` | `arn:aws:sns:{region}:{account}:c360-{env}-pipeline-alerts` | Dispatches failure alerts to team subscribers. |
| **`lambda-secrets-kms`** | `secretsmanager:GetSecretValue`<br>`secretsmanager:DescribeSecret` | `arn:aws:secretsmanager:{region}:{account}:secret:customer360/{env}/*`<br>`arn:aws:secretsmanager:{region}:{account}:secret:c_360_secret_manager*` | Secure retrieval of HubSpot and third-party API credentials. |
|  | `kms:GenerateDataKey`<br>`kms:Decrypt`<br>`kms:DescribeKey` | `{kms_key_arn}` | Decrypts encrypted S3 objects and Secrets Manager values. |

---

### B. Step Functions Execution Role (`c360-{env}-step-functions-execution`)

| Policy Name | Action(s) | Scoped Resource(s) | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **`sfn-lambda-invoke`** | `lambda:InvokeFunction` | Explicit ARNs of the 8 pipeline Lambda functions | Allows state machine tasks to invoke handlers in sequence/parallel. |
| **`sfn-glue-start`** | `glue:StartJobRun`<br>`glue:GetJobRun`<br>`glue:GetJobRuns`<br>`glue:BatchStopJobRun` | `arn:aws:glue:{region}:{account}:job/c360-{env}-dbt-runner` | Starts dbt runner job using the `.sync` task integration and monitors state until completion. |
| **`sfn-logging`** | `logs:CreateLogDelivery`<br>`logs:GetLogDelivery`<br>`logs:UpdateLogDelivery`<br>`logs:DeleteLogDelivery`<br>`logs:ListLogDeliveries`<br>`logs:PutResourcePolicy`<br>`logs:DescribeResourcePolicies`<br>`logs:DescribeLogGroups`<br>`logs:PutLogEvents` | `*` (AWS-managed CloudWatch Vended Logs requirement) | Streams full execution history to `/aws/vendedlogs/states/`. |
|  | `xray:PutTraceSegments`<br>`xray:PutTelemetryRecords`<br>`xray:GetSamplingRules`<br>`xray:GetSamplingTargets` | `*` | Distributed tracing through AWS X-Ray. |

---

### C. Glue Jobs Execution Role (`c360-{env}-glue-execution`)

| Policy Name | Action(s) | Scoped Resource(s) | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **`glue-s3`** | `s3:GetObject`<br>`s3:PutObject`<br>`s3:DeleteObject`<br>`s3:ListBucket`<br>`s3:GetBucketLocation` | `{lakehouse_bucket}` & `/*`<br>`{artifacts_bucket}` & `/*` | Downloads dbt project package and runner scripts; writes dbt logs/temp files. |
| **`glue-athena-catalog`** | `athena:StartQueryExecution`<br>`athena:StopQueryExecution`<br>`athena:GetQueryExecution`<br>`athena:GetQueryResults`<br>`athena:GetWorkGroup`<br>`athena:ListQueryExecutions` | `arn:aws:athena:{region}:{account}:workgroup/{workgroup}` | Executes dbt transformation SQL models against Iceberg tables. |
|  | `glue:GetDatabase`<br>`glue:GetTable`<br>`glue:CreateTable`<br>`glue:UpdateTable`<br>`glue:DeleteTable` | `database/{glue_db}`<br>`table/{glue_db}/*` | dbt updates catalog schema for materialized views and tables. |
|  | `logs:CreateLogGroup`<br>`logs:CreateLogStream`<br>`logs:PutLogEvents` | `arn:aws:logs:{region}:{account}:log-group:/aws-glue/python-jobs/*` | CloudWatch logging for Glue Python Shell execution. |

---

### D. EventBridge Scheduler Execution Role (`c360-{env}-scheduler-execution`)

| Policy Name | Action(s) | Scoped Resource(s) | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **`scheduler-sfn-start`** | `states:StartExecution` | `arn:aws:states:{region}:{account}:stateMachine:c360-{env}-batch-orchestrator` | Allows EventBridge Scheduler to trigger the daily state machine run. |

---

### E. GitHub Actions OIDC Deploy Role (`c360-{env}-github-deploy`)

- **Trust Relationship:**
  - Principal: `arn:aws:iam::{account_id}:oidc-provider/token.actions.githubusercontent.com`
  - Condition: `token.actions.githubusercontent.com:sub` matches `repo:Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline:*`
  - Condition: `token.actions.githubusercontent.com:aud` equals `sts.amazonaws.com`
- **Permissions:**
  - Read/Write to Terraform S3 remote state (`c360-{env}-tfstate`) and DynamoDB lock table (`c360-{env}-tflock`).
  - Infrastructure provisioning for S3, Glue, Athena, KMS, IAM, Secrets Manager, CloudWatch, SNS, Lambda, Step Functions, and EventBridge.
