# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Step Functions Module
# Manages: State Machine (Standard), IAM role, and CloudWatch execution logging
# ─────────────────────────────────────────────────────────────────────────────

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })

  asl_template_path = "${path.module}/../../../step_functions/customer360_batch.asl.json"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Log Group for State Machine Execution History
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-batch-orchestrator"
  retention_in_days = 30
  tags              = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM Role for Step Functions Execution (Least-Privilege)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "step_functions_exec" {
  name = "${local.name_prefix}-step-functions-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "states.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-step-functions-execution" })
}

# 1. Lambda Invocation Policy
resource "aws_iam_policy" "sfn_lambda_invoke" {
  name        = "${local.name_prefix}-sfn-lambda-invoke"
  description = "Allows Step Functions to invoke Customer 360 pipeline Lambda functions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokePipelineLambdas"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = values(var.lambda_arns)
      }
    ]
  })
}

# 2. Glue Job Execution Policy (.sync integration)
resource "aws_iam_policy" "sfn_glue_start" {
  name        = "${local.name_prefix}-sfn-glue-start"
  description = "Allows Step Functions to run Glue dbt runner and monitor completion"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [var.glue_dbt_job_arn]
      }
    ]
  })
}

# 3. CloudWatch Logging & X-Ray Delivery Policy
resource "aws_iam_policy" "sfn_logging" {
  name        = "${local.name_prefix}-sfn-logging"
  description = "Allows Step Functions to stream execution logs to CloudWatch and send X-Ray traces"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "WriteLogs"
        Effect = "Allow"
        Action = [
          "logs:PutLogEvents",
          "logs:CreateLogStream",
          "logs:DescribeLogStreams"
        ]
        Resource = ["${aws_cloudwatch_log_group.sfn_logs.arn}:*"]
      },
      {
        Sid    = "XRayTracing"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sfn_lambda_invoke" {
  role       = aws_iam_role.step_functions_exec.name
  policy_arn = aws_iam_policy.sfn_lambda_invoke.arn
}

resource "aws_iam_role_policy_attachment" "sfn_glue_start" {
  role       = aws_iam_role.step_functions_exec.name
  policy_arn = aws_iam_policy.sfn_glue_start.arn
}

resource "aws_iam_role_policy_attachment" "sfn_logging" {
  role       = aws_iam_role.step_functions_exec.name
  policy_arn = aws_iam_policy.sfn_logging.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Step Functions State Machine (STANDARD)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sfn_state_machine" "customer360_batch" {
  name     = "${local.name_prefix}-batch-orchestrator"
  role_arn = aws_iam_role.step_functions_exec.arn
  type     = "STANDARD"

  definition = templatefile(local.asl_template_path, {
    initialize_lambda_arn          = var.lambda_arns["initialize"]
    ingest_lambda_arn              = var.lambda_arns["ingest"]
    silver_gate_lambda_arn          = var.lambda_arns["silver_gate"]
    quality_check_lambda_arn        = var.lambda_arns["quality_check"]
    iceberg_load_lambda_arn         = var.lambda_arns["iceberg_load"]
    reconcile_lambda_arn            = var.lambda_arns["reconcile"]
    notify_lambda_arn               = var.lambda_arns["notify"]
    iceberg_maintenance_lambda_arn  = var.lambda_arns["iceberg_maintenance"]
    glue_dbt_job_name              = var.glue_dbt_job_name
    s3_dbt_path                    = var.s3_dbt_path
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-batch-orchestrator" })
}
