# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — EventBridge Scheduler Module
# Manages: Daily execution trigger for Step Functions pipeline
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
# IAM Role for EventBridge Scheduler
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "scheduler_exec" {
  name = "${local.name_prefix}-scheduler-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "scheduler.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-scheduler-execution" })
}

resource "aws_iam_policy" "scheduler_start_execution" {
  name        = "${local.name_prefix}-scheduler-sfn-start"
  description = "Allows EventBridge Scheduler to trigger Customer 360 Step Functions execution"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "StartStepFunctionsExecution"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [var.state_machine_arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "scheduler_start_execution" {
  role       = aws_iam_role.scheduler_exec.name
  policy_arn = aws_iam_policy.scheduler_start_execution.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# EventBridge Scheduler Schedule
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_scheduler_schedule" "customer360_batch" {
  name        = "${local.name_prefix}-daily-batch"
  description = "Triggers daily Customer 360 lakehouse batch pipeline in Step Functions"
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
    })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}
