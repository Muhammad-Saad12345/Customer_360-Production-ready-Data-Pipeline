# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Glue Jobs Module
# Manages: Serverless dbt Runner (Glue Python Shell, 1/16 DPU, ~$0.04/hr)
# ─────────────────────────────────────────────────────────────────────────────

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# IAM Role & Policies for Glue Job
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "glue_exec" {
  name = "${local.name_prefix}-glue-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-glue-execution" })
}

resource "aws_iam_policy" "glue_s3" {
  name        = "${local.name_prefix}-glue-s3"
  description = "Allows Glue job to access Lakehouse and script artifacts"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*",
          var.artifacts_bucket_arn,
          "${var.artifacts_bucket_arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "glue_athena_catalog" {
  name        = "${local.name_prefix}-glue-athena-catalog"
  description = "Allows Glue job to query Athena and modify Glue Catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListQueryExecutions"
        ]
        Resource = [
          "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/${var.athena_workgroup_name}"
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogging"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/python-jobs/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = aws_iam_policy.glue_s3.arn
}

resource "aws_iam_role_policy_attachment" "glue_athena_catalog" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = aws_iam_policy.glue_athena_catalog.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# AWS Glue Job — Python Shell (dbt runner)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_job" "dbt_runner" {
  name         = "${local.name_prefix}-dbt-runner"
  role_arn     = aws_iam_role.glue_exec.arn
  description  = "Executes dbt core models and tests against Iceberg tables via Athena"
  max_capacity = 0.0625 # 1/16 DPU — ultra cost-effective serverless tier
  max_retries  = 0
  timeout      = 60

  command {
    name            = "pythonshell"
    script_location = "s3://${var.artifacts_bucket_name}/glue_jobs/dbt_runner.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--additional-python-modules"        = "dbt-core==1.8.7,dbt-athena-community==1.8.3"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--TempDir"                          = "s3://${var.lakehouse_bucket_name}/glue-temp/"
    "--s3-bucket"                        = var.lakehouse_bucket_name
    "--environment"                      = var.environment
    "--iceberg-database"                 = var.glue_database_name
    "--athena-workgroup"                 = var.athena_workgroup_name
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dbt-runner" })
}
