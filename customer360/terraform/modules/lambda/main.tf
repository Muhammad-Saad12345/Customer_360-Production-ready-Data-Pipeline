# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Lambda Module
# Manages: 8 specialized Lambda functions, IAM role, and CloudWatch log groups
# ─────────────────────────────────────────────────────────────────────────────

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })

  functions = {
    initialize = {
      description = "Generates pipeline run_id and execution context"
      memory_size = 256
      timeout     = 60
    }
    ingest = {
      description = "Parameterized connector ingesting data from 6 upstream sources"
      memory_size = 512
      timeout     = 900
    }
    silver_gate = {
      description = "Validates raw records against schema and writes Silver Parquet"
      memory_size = 1024
      timeout     = 900
    }
    quality_check = {
      description = "Enforces rejection rate thresholds and emits CloudWatch DQ metrics"
      memory_size = 512
      timeout     = 300
    }
    iceberg_load = {
      description = "Promotes validated Silver records to Iceberg via Athena MERGE INTO"
      memory_size = 1024
      timeout     = 900
    }
    reconcile = {
      description = "Verifies row counts and data integrity across pipeline branches"
      memory_size = 256
      timeout     = 300
    }
    notify = {
      description = "Publishes execution alerts and failure notifications via SNS"
      memory_size = 256
      timeout     = 60
    }
    iceberg_maintenance = {
      description = "Runs Athena OPTIMIZE and VACUUM on active Iceberg tables"
      memory_size = 512
      timeout     = 900
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# IAM Role & Policies for Pipeline Lambdas (Least-Privilege)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_exec" {
  name = "${local.name_prefix}-lambda-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-lambda-execution" })
}

# 1. CloudWatch Logs policy
resource "aws_iam_policy" "lambda_logging" {
  name        = "${local.name_prefix}-lambda-logging"
  description = "Allows Lambda functions to emit logs to CloudWatch"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CreateAndWriteLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.name_prefix}-*:*"
        ]
      }
    ]
  })
}

# 2. S3 Lakehouse policy
resource "aws_iam_policy" "lambda_s3" {
  name        = "${local.name_prefix}-lambda-s3"
  description = "Allows Lambda functions to read/write Lakehouse S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LakehouseBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetObjectVersion"
        ]
        Resource = [
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*"
        ]
      }
    ]
  })
}

# 3. Glue Catalog & Athena query policy
resource "aws_iam_policy" "lambda_glue_athena" {
  name        = "${local.name_prefix}-lambda-glue-athena"
  description = "Allows Lambda functions to query Athena and update Glue Iceberg catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
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
        Sid    = "AthenaQueryAccess"
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
      }
    ]
  })
}

# 4. CloudWatch Metrics & SNS Alerting policy
resource "aws_iam_policy" "lambda_observability" {
  name        = "${local.name_prefix}-lambda-observability"
  description = "Allows emitting CloudWatch metrics and sending SNS failure alerts"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "PutCustomMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
        Condition = {
          StringEquals = { "cloudwatch:namespace" = "Customer360/Pipeline" }
        }
      },
      {
        Sid    = "PublishAlertSNS"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = var.sns_topic_arn != "" ? [var.sns_topic_arn] : [
          "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${local.name_prefix}-pipeline-alerts"
        ]
      }
    ]
  })
}

# 5. Secrets Manager & KMS policy
resource "aws_iam_policy" "lambda_secrets_kms" {
  name        = "${local.name_prefix}-lambda-secrets-kms"
  description = "Allows accessing API credentials in Secrets Manager and decrypting via KMS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid    = "SecretsManagerAccess"
          Effect = "Allow"
          Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
          Resource = [
            "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:customer360/${var.environment}/*",
            "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:c_360_secret_manager*"
          ]
        }
      ],
      var.kms_key_arn != "" ? [
        {
          Sid    = "KMSKeyAccess"
          Effect = "Allow"
          Action = ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"]
          Resource = [var.kms_key_arn]
        }
      ] : []
    )
  })
}

# Attach policies to role
resource "aws_iam_role_policy_attachment" "lambda_logging" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_logging.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_s3.arn
}

resource "aws_iam_role_policy_attachment" "lambda_glue_athena" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_glue_athena.arn
}

resource "aws_iam_role_policy_attachment" "lambda_observability" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_observability.arn
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_kms" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_secrets_kms.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Lambda Deployment Packages (fallback placeholder zip if S3 bucket unset)
# ─────────────────────────────────────────────────────────────────────────────

data "archive_file" "placeholder" {
  type        = "zip"
  output_path = "${path.module}/placeholder.zip"

  source {
    content  = "def lambda_handler(event, context):\n    return {'status': 'ok'}\n"
    filename = "handler.py"
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Lambda Functions & CloudWatch Log Groups
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "functions" {
  for_each          = local.functions
  name              = "/aws/lambda/${local.name_prefix}-${each.key}"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_lambda_function" "functions" {
  for_each      = local.functions
  function_name = "${local.name_prefix}-${each.key}"
  description   = each.value.description
  role          = aws_iam_role.lambda_exec.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  memory_size   = each.value.memory_size
  timeout       = each.value.timeout

  # If S3 package provided via variables, use S3; otherwise use local archive
  s3_bucket = var.s3_artifacts_bucket != "" ? var.s3_artifacts_bucket : null
  s3_key    = var.s3_artifacts_bucket != "" ? lookup(var.s3_lambda_keys, each.key, null) : null

  filename         = var.s3_artifacts_bucket == "" ? data.archive_file.placeholder.output_path : null
  source_code_hash = var.s3_artifacts_bucket == "" ? data.archive_file.placeholder.output_base64sha256 : null

  environment {
    variables = {
      ENVIRONMENT              = var.environment
      S3_BUCKET                = var.lakehouse_bucket_name
      ICEBERG_DATABASE         = var.glue_database_name
      ATHENA_WORKGROUP         = var.athena_workgroup_name
      C360_SNS_ALERT_TOPIC_ARN = var.sns_topic_arn
      DQ_REJECT_HARD_PCT       = "10.0"
      DQ_REJECT_WARN_PCT       = "5.0"
    }
  }

  dynamic "vpc_config" {
    for_each = length(var.vpc_subnet_ids) > 0 ? [1] : []
    content {
      subnet_ids         = var.vpc_subnet_ids
      security_group_ids = var.vpc_security_group_ids
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.functions,
    aws_iam_role_policy_attachment.lambda_logging
  ]

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-${each.key}" })
}
