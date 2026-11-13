# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Lambda Module
#
# One IAM role per function, each scoped to the S3 prefixes, Glue/Athena
# resources and secrets that function actually uses. Every role carries the
# workload permissions boundary created by terraform/bootstrap.
#
# Code is always deployed from S3 at artifacts/<artifact_version>/lambdas/<fn>.zip
# (built by scripts/package_artifacts.py). There is deliberately no placeholder
# fallback: deploying a function without real code must fail the plan.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })

  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  # Per-function needs. Prefixes are relative to the lakehouse bucket.
  functions = {
    initialize = {
      description = "Generates the pipeline run_id and resolves execution input defaults"
      memory_size = 256
      timeout     = 60
      s3_read     = []
      s3_write    = []
      s3_delete   = []
      secrets     = false
      athena      = false
      sns         = false
    }
    ingest = {
      description = "Extracts one source/entity and lands the original responses in raw/"
      memory_size = 512
      timeout     = 900
      s3_read     = ["checkpoints/", "raw/", "metadata/"]
      s3_write    = ["checkpoints/", "raw/", "metadata/"]
      s3_delete   = []
      secrets     = true
      athena      = false
      sns         = false
    }
    silver_gate = {
      description = "Validates raw records against the source contract and writes silver Parquet + quarantine"
      memory_size = 1024
      timeout     = 900
      s3_read     = ["raw/", "silver/"]
      s3_write    = ["silver/", "quarantine/"]
      s3_delete   = []
      secrets     = false
      athena      = false
      sns         = false
    }
    quality_check = {
      description = "Enforces rejection-rate thresholds and emits DQ metrics"
      memory_size = 512
      timeout     = 300
      s3_read     = ["silver/", "quarantine/"]
      s3_write    = []
      s3_delete   = []
      secrets     = false
      athena      = false
      sns         = false
    }
    iceberg_load = {
      description = "MERGEs validated silver records into Iceberg via Athena"
      memory_size = 1024
      timeout     = 900
      s3_read     = ["silver/", "${var.iceberg_s3_prefix}/", "athena-results/"]
      s3_write    = ["${var.iceberg_s3_prefix}/", "athena-results/"]
      s3_delete   = []
      secrets     = false
      athena      = true
      sns         = false
    }
    reconcile = {
      description = "Reconciles per-source counts and blocks dbt on mismatch"
      memory_size = 256
      timeout     = 300
      s3_read     = []
      s3_write    = []
      s3_delete   = []
      secrets     = false
      athena      = false
      sns         = false
    }
    notify = {
      description = "Publishes detailed pipeline notifications to SNS"
      memory_size = 256
      timeout     = 60
      s3_read     = []
      s3_write    = []
      s3_delete   = []
      secrets     = false
      athena      = false
      sns         = true
    }
    iceberg_maintenance = {
      description = "Runs Athena OPTIMIZE and VACUUM on the silver Iceberg tables"
      memory_size = 512
      timeout     = 900
      s3_read     = ["${var.iceberg_s3_prefix}/", "athena-results/"]
      s3_write    = ["${var.iceberg_s3_prefix}/", "athena-results/"]
      s3_delete   = ["${var.iceberg_s3_prefix}/"]
      secrets     = false
      athena      = true
      sns         = false
    }
  }

  # Build one IAM policy document per function.
  function_policies = {
    for fn, cfg in local.functions : fn => {
      Version = "2012-10-17"
      Statement = concat(
        [
          {
            Sid      = "WriteOwnLogs"
            Effect   = "Allow"
            Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
            Resource = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.name_prefix}-${fn}:*"]
          },
          {
            # PutMetricData has no resource-level permissions; restricted by namespace.
            Sid       = "PutPipelineMetrics"
            Effect    = "Allow"
            Action    = ["cloudwatch:PutMetricData"]
            Resource  = ["*"]
            Condition = { StringEquals = { "cloudwatch:namespace" = "Customer360/Pipeline" } }
          }
        ],
        [for s in [
          {
            Sid      = "BucketLocation"
            Effect   = "Allow"
            Action   = ["s3:GetBucketLocation"]
            Resource = [var.lakehouse_bucket_arn]
          },
          {
            Sid       = "ListOwnPrefixes"
            Effect    = "Allow"
            Action    = ["s3:ListBucket"]
            Resource  = [var.lakehouse_bucket_arn]
            Condition = { StringLike = { "s3:prefix" = flatten([for p in distinct(concat(cfg.s3_read, cfg.s3_write)) : [p, "${p}*"]]) } }
          }
        ] : s if(length(concat(cfg.s3_read, cfg.s3_write)) > 0)],
        [for s in [
          {
            Sid      = "ReadOwnPrefixes"
            Effect   = "Allow"
            Action   = ["s3:GetObject", "s3:GetObjectVersion"]
            Resource = [for p in cfg.s3_read : "${var.lakehouse_bucket_arn}/${p}*"]
          }
        ] : s if(length(cfg.s3_read) > 0)],
        [for s in [
          {
            Sid      = "WriteOwnPrefixes"
            Effect   = "Allow"
            Action   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
            Resource = [for p in cfg.s3_write : "${var.lakehouse_bucket_arn}/${p}*"]
          }
        ] : s if(length(cfg.s3_write) > 0)],
        [for s in [
          {
            Sid      = "DeleteExpiredIcebergFiles"
            Effect   = "Allow"
            Action   = ["s3:DeleteObject"]
            Resource = [for p in cfg.s3_delete : "${var.lakehouse_bucket_arn}/${p}*"]
          }
        ] : s if(length(cfg.s3_delete) > 0)],
        [for s in [
          {
            Sid    = "GlueCatalogForEnvDatabase"
            Effect = "Allow"
            Action = [
              "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
              "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition"
            ]
            Resource = [
              "arn:aws:glue:${local.region}:${local.account_id}:catalog",
              "arn:aws:glue:${local.region}:${local.account_id}:database/${var.glue_database_name}",
              "arn:aws:glue:${local.region}:${local.account_id}:table/${var.glue_database_name}/*"
            ]
          },
          {
            Sid      = "AthenaInEnvWorkgroup"
            Effect   = "Allow"
            Action   = ["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"]
            Resource = ["arn:aws:athena:${local.region}:${local.account_id}:workgroup/${var.athena_workgroup_name}"]
          }
        ] : s if(cfg.athena)],
        [for s in [
          {
            Sid    = "ReadSourceSecrets"
            Effect = "Allow"
            Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
            Resource = concat(
              ["arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:customer360/${var.environment}/*"],
              [for s in ["arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:${var.legacy_secret_name}*"] : s if(var.legacy_secret_name != "")]
            )
          }
        ] : s if(cfg.secrets)],
        [for s in [
          {
            Sid      = "PublishPipelineAlerts"
            Effect   = "Allow"
            Action   = ["sns:Publish"]
            Resource = [var.sns_topic_arn]
          }
        ] : s if(cfg.sns && var.sns_topic_arn != "")],
        [for s in [
          {
            # Concurrency guard: refuse to start if another run is RUNNING.
            Sid      = "ListOwnExecutions"
            Effect   = "Allow"
            Action   = ["states:ListExecutions"]
            Resource = ["arn:aws:states:${local.region}:${local.account_id}:stateMachine:${local.name_prefix}-batch-orchestrator"]
          }
        ] : s if(fn == "initialize")],
        [for s in [
          {
            Sid      = "UseLakehouseKey"
            Effect   = "Allow"
            Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
            Resource = [var.kms_key_arn]
          }
        ] : s if(var.kms_key_arn != "" && length(concat(cfg.s3_read, cfg.s3_write)) > 0)]
      )
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# IAM: one role per function
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "function" {
  for_each             = local.functions
  name                 = "${local.name_prefix}-lambda-${replace(each.key, "_", "-")}"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-lambda-${each.key}" })
}

resource "aws_iam_role_policy" "function" {
  for_each = local.functions
  name     = "${local.name_prefix}-lambda-${replace(each.key, "_", "-")}"
  role     = aws_iam_role.function[each.key].id
  policy   = jsonencode(local.function_policies[each.key])
}

# ─────────────────────────────────────────────────────────────────────────────
# Lambda functions & log groups
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "functions" {
  for_each          = local.functions
  name              = "/aws/lambda/${local.name_prefix}-${each.key}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_lambda_function" "functions" {
  for_each      = local.functions
  function_name = "${local.name_prefix}-${each.key}"
  description   = each.value.description
  role          = aws_iam_role.function[each.key].arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  architectures = ["x86_64"]
  memory_size   = each.value.memory_size
  timeout       = each.value.timeout

  # Versioned key: a new git SHA produces a new key, so code changes always show in the plan.
  s3_bucket = var.s3_artifacts_bucket
  s3_key    = "artifacts/${var.artifact_version}/lambdas/${each.key}.zip"

  environment {
    variables = {
      ENVIRONMENT                    = var.environment
      S3_BUCKET                      = var.lakehouse_bucket_name
      ICEBERG_DATABASE               = var.glue_database_name
      ICEBERG_S3_PREFIX              = var.iceberg_s3_prefix
      ATHENA_WORKGROUP               = var.athena_workgroup_name
      C360_SNS_ALERT_TOPIC_ARN       = var.sns_topic_arn
      SECRETS_MANAGER_SECRET_NAME    = var.legacy_secret_name
      C360_SOURCE_MODES              = jsonencode(var.source_modes)
      C360_REQUIRED_SOURCE_MODE      = var.required_source_mode
      C360_SOURCE_SECRET_PREFIX      = "customer360/${var.environment}/"
      CHECKPOINT_BACKEND             = "s3"
      DQ_REJECT_HARD_PCT             = "10.0"
      DQ_REJECT_WARN_PCT             = "5.0"
      C360_MANDATORY_SOURCES         = join(",", var.mandatory_sources)
      C360_ENABLED_SOURCES           = join(",", var.enabled_sources)
      C360_SYNTHETIC_INCLUDE_INVALID = var.synthetic_include_invalid ? "true" : "false"
      C360_ARTIFACT_VERSION          = var.artifact_version
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.functions,
    aws_iam_role_policy.function,
  ]

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-${each.key}" })
}
