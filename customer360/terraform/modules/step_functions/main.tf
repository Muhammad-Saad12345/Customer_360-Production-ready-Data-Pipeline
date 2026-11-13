# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Step Functions Module
# Manages: state machine (Standard), its IAM role, execution logging,
#          failure alerting (EventBridge -> SNS) and execution alarms
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
# Execution logs
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-batch-orchestrator"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM role (least privilege)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "step_functions_exec" {
  name                 = "${local.name_prefix}-step-functions-execution"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id } }
    }]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-step-functions-execution" })
}

resource "aws_iam_role_policy" "step_functions_exec" {
  name = "${local.name_prefix}-step-functions-execution"
  role = aws_iam_role.step_functions_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "InvokePipelineLambdas"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = concat(values(var.lambda_arns), [for a in values(var.lambda_arns) : "${a}:*"])
      },
      {
        # Required set for the glue:startJobRun.sync integration (AWS docs).
        Sid      = "RunDbtGlueJob"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = [var.glue_dbt_job_arn]
      },
      {
        # Log delivery APIs do not support resource-level permissions.
        Sid    = "CloudWatchLogDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
        ]
        Resource = ["*"]
      },
      {
        Sid      = "XRayTracing"
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
        Resource = ["*"]
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# State machine (STANDARD)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sfn_state_machine" "customer360_batch" {
  name     = "${local.name_prefix}-batch-orchestrator"
  role_arn = aws_iam_role.step_functions_exec.arn
  type     = "STANDARD"

  definition = templatefile(local.asl_template_path, {
    initialize_lambda_arn    = var.lambda_arns["initialize"]
    ingest_lambda_arn        = var.lambda_arns["ingest"]
    silver_gate_lambda_arn   = var.lambda_arns["silver_gate"]
    quality_check_lambda_arn = var.lambda_arns["quality_check"]
    iceberg_load_lambda_arn  = var.lambda_arns["iceberg_load"]
    reconcile_lambda_arn     = var.lambda_arns["reconcile"]
    notify_lambda_arn        = var.lambda_arns["notify"]
    glue_dbt_job_name        = var.glue_dbt_job_name
    s3_dbt_path              = var.s3_dbt_path
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }

  depends_on = [aws_iam_role_policy.step_functions_exec]

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-batch-orchestrator" })
}

# ─────────────────────────────────────────────────────────────────────────────
# Failure alerting: execution status change -> SNS (no role; topic policy in root)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "execution_failed" {
  name        = "${local.name_prefix}-batch-execution-failed"
  description = "Customer 360 batch execution ended in FAILED, TIMED_OUT or ABORTED"

  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.customer360_batch.arn]
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "execution_failed_sns" {
  count     = var.alert_topic_arn != "" ? 1 : 0
  rule      = aws_cloudwatch_event_rule.execution_failed.name
  target_id = "sns-alert"
  arn       = var.alert_topic_arn

  input_transformer {
    input_paths = {
      status    = "$.detail.status"
      execution = "$.detail.executionArn"
      name      = "$.detail.name"
    }
    input_template = "\"[${upper(var.environment)}] Customer 360 batch <status>: execution <name> (<execution>). Open the Step Functions console, read the failed state, fix the cause, then redrive the execution.\""
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Execution alarms (AWS/States metrics are always published, no custom code)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "executions_failed" {
  for_each = toset(["ExecutionsFailed", "ExecutionsTimedOut", "ExecutionsAborted"])

  alarm_name          = "${local.name_prefix}-batch-${lower(each.key)}"
  namespace           = "AWS/States"
  metric_name         = each.key
  dimensions          = { StateMachineArn = aws_sfn_state_machine.customer360_batch.arn }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_description   = "Customer 360 ${var.environment} batch: ${each.key} >= 1 in the last hour"
  alarm_actions       = var.alert_topic_arn != "" ? [var.alert_topic_arn] : []
  tags                = local.common_tags
}
