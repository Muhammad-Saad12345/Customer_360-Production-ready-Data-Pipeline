# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Glue Jobs Module
# Manages: Serverless dbt runner (Glue Python Shell, 1/16 DPU) and its least-privilege role
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
  name                 = "${local.name_prefix}-glue-execution"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id } }
    }]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-glue-execution" })
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  # dbt reads silver Iceberg tables and writes its own models + Athena results.
  lakehouse_rw_prefixes = distinct([
    "${var.iceberg_s3_prefix}/",
    "${var.dbt_data_prefix}/",
    "athena-results/",
    "${var.dbt_athena_results_prefix}/",
    "glue-temp/",
  ])

  glue_policy = {
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid      = "ReadArtifacts"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.artifacts_bucket_arn}/artifacts/*"]
      },
      {
        Sid      = "BucketMetadata"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = distinct([var.lakehouse_bucket_arn, var.artifacts_bucket_arn])
      },
      {
        Sid      = "LakehouseReadWrite"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = [for p in local.lakehouse_rw_prefixes : "${var.lakehouse_bucket_arn}/${p}*"]
      },
      {
        Sid      = "AthenaInEnvWorkgroup"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup", "athena:ListQueryExecutions"]
        Resource = ["arn:aws:athena:${local.region}:${local.account_id}:workgroup/${var.athena_workgroup_name}"]
      },
      {
        Sid    = "GlueCatalogForEnvDatabase"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
          "glue:DeleteTable", "glue:GetTableVersions", "glue:DeleteTableVersion", "glue:BatchDeleteTableVersion",
          "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "glue:CreatePartition",
          "glue:BatchCreatePartition", "glue:UpdatePartition", "glue:DeletePartition", "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${local.region}:${local.account_id}:table/${var.glue_database_name}/*"
        ]
      },
      {
        Sid      = "GlueJobLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"]
      }
      ],
      # Prod buckets are SSE-KMS: without this the job can neither read its script nor write data (audit S11).
      [for s in [{
        Sid      = "UseLakehouseKey"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = [var.kms_key_arn]
      }] : s if(var.kms_key_arn != "")]
    )
  }
}

resource "aws_iam_role_policy" "glue_exec" {
  name   = "${local.name_prefix}-glue-execution"
  role   = aws_iam_role.glue_exec.id
  policy = jsonencode(local.glue_policy)
}

# ─────────────────────────────────────────────────────────────────────────────
# AWS Glue Job — Python Shell (dbt runner)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_job" "dbt_runner" {
  name         = "${local.name_prefix}-dbt-runner"
  role_arn     = aws_iam_role.glue_exec.arn
  description  = "Executes dbt models and tests against Iceberg tables via Athena"
  max_capacity = 0.0625 # 1/16 DPU
  max_retries  = 0
  timeout      = 60

  command {
    name            = "pythonshell"
    script_location = "s3://${var.artifacts_bucket_name}/artifacts/${var.artifact_version}/glue/dbt_runner.py"
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

  depends_on = [aws_iam_role_policy.glue_exec]

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dbt-runner" })
}
