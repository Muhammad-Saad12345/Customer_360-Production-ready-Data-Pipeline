# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — EventBridge Scheduler Module
# Manages: daily batch trigger (Step Functions) and weekly Iceberg maintenance
#          trigger (Lambda), with one least-privilege scheduler role.
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

resource "aws_iam_role" "scheduler_exec" {
  name                 = "${local.name_prefix}-scheduler-execution"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id } }
    }]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-scheduler-execution" })
}

resource "aws_iam_role_policy" "scheduler_exec" {
  name = "${local.name_prefix}-scheduler-execution"
  role = aws_iam_role.scheduler_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid      = "StartBatchExecution"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [var.state_machine_arn]
      }],
      [for s in [{
        Sid      = "InvokeIcebergMaintenance"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [var.maintenance_lambda_arn, "${var.maintenance_lambda_arn}:*"]
      }] : s if(var.maintenance_lambda_arn != "")]
    )
  })
}

# Daily batch. The input is the full execution input; InitializePipeline applies
# defaults for anything omitted (sources, mandatory_sources), so this cannot
# drift from the state machine again (audit S1).
resource "aws_scheduler_schedule" "customer360_batch" {
  name        = "${local.name_prefix}-daily-batch"
  description = "Triggers the daily Customer 360 batch in Step Functions"
  state       = var.schedule_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.state_machine_arn
    role_arn = aws_iam_role.scheduler_exec.arn

    input = jsonencode({
      environment = var.environment
      dry_run     = false
      trigger     = "schedule"
    })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}

# Weekly Iceberg OPTIMIZE/VACUUM, outside the daily critical path (audit S10).
resource "aws_scheduler_schedule" "iceberg_maintenance" {
  count       = var.maintenance_lambda_arn != "" ? 1 : 0
  name        = "${local.name_prefix}-weekly-iceberg-maintenance"
  description = "Weekly OPTIMIZE and VACUUM of the silver Iceberg tables"
  state       = var.schedule_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = var.maintenance_schedule_expression
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.maintenance_lambda_arn
    role_arn = aws_iam_role.scheduler_exec.arn
    input    = jsonencode({ environment = var.environment })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 1
    }
  }
}
