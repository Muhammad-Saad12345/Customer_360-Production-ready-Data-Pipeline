# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Observability Module
# Manages: SNS alerts topic, CloudWatch alarms, CloudWatch dashboard
# ─────────────────────────────────────────────────────────────────────────────

variable "environment" { type = string }
variable "project_name" { type = string; default = "c360" }
variable "tags" { type = map(string); default = {} }
variable "alert_email_addresses" {
  description = "Email addresses to subscribe to the pipeline alerts SNS topic"
  type        = list(string)
  default     = []
}
variable "dq_rejection_rate_alarm_threshold" {
  description = "Rejection rate % that triggers a CRITICAL CloudWatch alarm"
  type        = number
  default     = 10
}
variable "task_failure_alarm_threshold" {
  description = "Number of task failures within the evaluation period to trigger alarm"
  type        = number
  default     = 1
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# SNS — Pipeline Alerts Topic
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name              = "${local.name_prefix}-pipeline-alerts"
  kms_master_key_id = "alias/aws/sns"

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-pipeline-alerts" })
}

resource "aws_sns_topic_subscription" "email" {
  for_each = toset(var.alert_email_addresses)

  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Alarms — Data Quality
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "dq_gate_failed" {
  alarm_name          = "${local.name_prefix}-dq-gate-failed"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "DQGateFailed"
  namespace           = "Customer360/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "DQ gate blocked an Iceberg promotion — rejection rate exceeded threshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment = var.environment
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "high_rejection_rate" {
  alarm_name          = "${local.name_prefix}-high-rejection-rate"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "RejectionRate"
  namespace           = "Customer360/Pipeline"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.dq_rejection_rate_alarm_threshold
  alarm_description   = "DQ rejection rate exceeded ${var.dq_rejection_rate_alarm_threshold}%"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment = var.environment
    Tier        = "quality"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Alarms — Pipeline Task Failures
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "task_failures" {
  alarm_name          = "${local.name_prefix}-pipeline-task-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "TaskFailure"
  namespace           = "Customer360/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = var.task_failure_alarm_threshold
  alarm_description   = "One or more Customer 360 pipeline tasks failed"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment = var.environment
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "pipeline_health_error" {
  alarm_name          = "${local.name_prefix}-pipeline-health-error"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "PipelineHealthStatus"
  namespace           = "Customer360/Pipeline"
  period              = 300
  statistic           = "Maximum"
  threshold           = 2   # 2 = ERROR (0=OK, 1=WARNING, 2=ERROR)
  alarm_description   = "Customer 360 overall pipeline health is ERROR"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Environment = var.environment
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Dashboard
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "customer360" {
  dashboard_name = "Customer360-${title(var.environment)}"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0; y = 0; width = 12; height = 6
        properties = {
          title   = "DQ Gate Verdicts (5-min)"
          metrics = [
            ["Customer360/Pipeline", "DQGatePassed", "Environment", var.environment, { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "DQGateFailed", "Environment", var.environment, { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.name
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 12; y = 0; width = 12; height = 6
        properties = {
          title   = "Rejection Rate % by Source"
          metrics = [
            for src in ["hubspot", "oms", "pim", "loyalty", "marketing", "support"] :
            ["Customer360/Pipeline", "RejectionRate", "Environment", var.environment, "Source", src, "Tier", "quality", { stat = "Maximum", period = 300 }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.name
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0; y = 6; width = 12; height = 6
        properties = {
          title   = "Task Outcomes (5-min)"
          metrics = [
            ["Customer360/Pipeline", "TaskSuccess", "Environment", var.environment, { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "TaskFailure", "Environment", var.environment, { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.name
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 12; y = 6; width = 12; height = 6
        properties = {
          title   = "Pipeline Health Status (0=OK 1=WARN 2=ERROR)"
          metrics = [
            ["Customer360/Pipeline", "PipelineHealthStatus", "Environment", var.environment, { stat = "Maximum", period = 300 }]
          ]
          view    = "timeSeries"
          yAxis   = { left = { min = 0, max = 2 } }
          region  = data.aws_region.current.name
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0; y = 12; width = 24; height = 6
        properties = {
          title   = "Record Counts (Input / Valid / Invalid)"
          metrics = [
            ["Customer360/Pipeline", "InputRecordCount",   "Environment", var.environment, { stat = "Sum", period = 300 }],
            ["Customer360/Pipeline", "ValidRecordCount",   "Environment", var.environment, { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "InvalidRecordCount", "Environment", var.environment, { stat = "Sum", period = 300, color = "#ff7f0e" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.name
          period  = 300
        }
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "sns_alert_topic_arn" {
  description = "ARN of the pipeline alerts SNS topic"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "dashboard_name" {
  description = "CloudWatch dashboard name"
  value       = aws_cloudwatch_dashboard.customer360.dashboard_name
}
