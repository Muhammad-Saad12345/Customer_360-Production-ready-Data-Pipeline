# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Staging Environment Root Configuration
# Architecture: Step Functions + Lambda + Glue (dbt) + Athena + EventBridge
#
# Deployed only by the c360-github-deploy-staging role (GitHub environment
# "staging"). Requires terraform/bootstrap to have been applied once.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket       = "c360-staging-tfstate"
    key          = "customer360/staging/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true # S3-native locking (Terraform >= 1.11); DynamoDB locking is deprecated
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.common_tags
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "environment" {
  type    = string
  default = "staging"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "alert_email_addresses" {
  description = "Emails subscribed to pipeline alerts (each must confirm the subscription by email)"
  type        = list(string)
  default     = []
}

variable "artifact_version" {
  description = "Git SHA of the artifacts uploaded by CI (artifacts/<sha>/...)"
  type        = string
}

variable "enabled_sources" {
  description = "Sources processed by scheduled runs. In prod list only sources whose real API secret exists."
  type        = list(string)
  default     = ["hubspot", "oms", "pim", "loyalty", "marketing", "support"]
}

variable "mandatory_sources" {
  description = "A failure of one of these stops the run before dbt"
  type        = list(string)
  default     = ["hubspot", "oms"]
}

variable "schedule_enabled" {
  description = "Keep false until three manual end-to-end runs pass the data checks"
  type        = bool
  default     = false
}

variable "source_modes" {
  description = <<-EOT
    Where each source's data comes from. Staging starts in explicit synthetic mode
    because no source credentials are verified yet; switch a source to "api" only
    after its secret exists under customer360/staging/<source> and M18 passes.
  EOT
  type        = map(string)
  default = {
    hubspot   = "synthetic"
    oms       = "synthetic"
    pim       = "synthetic"
    loyalty   = "synthetic"
    marketing = "synthetic"
    support   = "synthetic"
  }
}

locals {
  account_id   = data.aws_caller_identity.current.account_id
  name_prefix  = "c360-${var.environment}"
  boundary_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/c360-workload-boundary"
  common_tags = {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# Existing resources
# ─────────────────────────────────────────────────────────────────────────────

# Staging lakehouse + artifacts bucket (created by hand, SSE-S3, versioned).
data "aws_s3_bucket" "lakehouse" {
  bucket = "c360-bucket786"
}

# These two already exist in the account (verified 13 Nov 2026). Import, do not recreate.
import {
  to = aws_glue_catalog_database.customer360_staging
  id = "${data.aws_caller_identity.current.account_id}:customer360_iceberg_stage"
}

import {
  to = aws_athena_workgroup.customer360_staging
  id = "customer360-staging"
}

# ─────────────────────────────────────────────────────────────────────────────
# Glue Data Catalog database
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "customer360_staging" {
  name        = "customer360_iceberg_stage"
  description = "Customer 360 Lakehouse — Iceberg tables (staging)"

  # Matches the account's Lake Formation default (IAM_ALLOWED_PRINCIPALS: ALL),
  # so IAM policies stay the single access-control layer. SELECT-only here would
  # silently block the pipeline's own MERGE writes on new tables.
  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Athena workgroup
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "customer360_staging" {
  name  = "customer360-staging"
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 10737418240 # 10 GiB guardrail

    result_configuration {
      output_location = "s3://${data.aws_s3_bucket.lakehouse.bucket}/athena-results/"
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# SNS — pipeline alerts
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-pipeline-alerts" })
}

# Only the failure rule, CloudWatch alarms and this account's principals may publish.
resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn = aws_sns_topic.pipeline_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AccountPrincipals"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action    = ["sns:Publish", "sns:GetTopicAttributes", "sns:Subscribe", "sns:ListSubscriptionsByTopic"]
        Resource  = aws_sns_topic.pipeline_alerts.arn
      },
      {
        Sid       = "ExecutionFailedRule"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.pipeline_alerts.arn
        Condition = { ArnEquals = { "aws:SourceArn" = module.step_functions.execution_failed_rule_arn } }
      },
      {
        Sid       = "CloudWatchAlarms"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.pipeline_alerts.arn
        Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_email_addresses)
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# ─────────────────────────────────────────────────────────────────────────────
# Lambda functions (one least-privilege role each)
# ─────────────────────────────────────────────────────────────────────────────

module "lambda" {
  source                   = "../../modules/lambda"
  environment              = var.environment
  project_name             = "c360"
  lakehouse_bucket_name    = data.aws_s3_bucket.lakehouse.bucket
  lakehouse_bucket_arn     = data.aws_s3_bucket.lakehouse.arn
  glue_database_name       = aws_glue_catalog_database.customer360_staging.name
  athena_workgroup_name    = aws_athena_workgroup.customer360_staging.name
  sns_topic_arn            = aws_sns_topic.pipeline_alerts.arn
  legacy_secret_name       = "c_360_secret_manager"
  permissions_boundary_arn = local.boundary_arn
  s3_artifacts_bucket      = data.aws_s3_bucket.lakehouse.bucket
  artifact_version         = var.artifact_version
  source_modes             = var.source_modes
  enabled_sources          = var.enabled_sources
  mandatory_sources        = var.mandatory_sources
  required_source_mode     = ""
  tags                     = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Glue job (dbt runner)
# ─────────────────────────────────────────────────────────────────────────────

module "glue_jobs" {
  source                    = "../../modules/glue_jobs"
  environment               = var.environment
  project_name              = "c360"
  lakehouse_bucket_name     = data.aws_s3_bucket.lakehouse.bucket
  lakehouse_bucket_arn      = data.aws_s3_bucket.lakehouse.arn
  artifacts_bucket_name     = data.aws_s3_bucket.lakehouse.bucket
  artifacts_bucket_arn      = data.aws_s3_bucket.lakehouse.arn
  glue_database_name        = aws_glue_catalog_database.customer360_staging.name
  athena_workgroup_name     = aws_athena_workgroup.customer360_staging.name
  dbt_data_prefix           = "staging-iceberg"
  dbt_athena_results_prefix = "staging-athena-results"
  artifact_version          = var.artifact_version
  permissions_boundary_arn  = local.boundary_arn
  tags                      = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Step Functions (batch orchestration + failure alerting)
# ─────────────────────────────────────────────────────────────────────────────

module "step_functions" {
  source                   = "../../modules/step_functions"
  environment              = var.environment
  project_name             = "c360"
  lambda_arns              = module.lambda.function_arns
  glue_dbt_job_name        = module.glue_jobs.job_name
  glue_dbt_job_arn         = module.glue_jobs.job_arn
  s3_dbt_path              = "s3://${data.aws_s3_bucket.lakehouse.bucket}/artifacts/${var.artifact_version}/dbt/dbt_project.tar.gz"
  alert_topic_arn          = aws_sns_topic.pipeline_alerts.arn
  permissions_boundary_arn = local.boundary_arn
  tags                     = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# EventBridge Scheduler (disabled until validated)
# ─────────────────────────────────────────────────────────────────────────────

module "eventbridge" {
  source                   = "../../modules/eventbridge"
  environment              = var.environment
  project_name             = "c360"
  state_machine_arn        = module.step_functions.state_machine_arn
  maintenance_lambda_arn   = module.lambda.function_arns["iceberg_maintenance"]
  schedule_enabled         = var.schedule_enabled
  permissions_boundary_arn = local.boundary_arn
  tags                     = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Data-quality alarms. src/monitoring/cloudwatch.py publishes every datum twice:
# with its full dimensions and with {Environment} only, so these match.
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
  alarm_description   = "A DQ gate blocked Iceberg promotion (staging)"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = var.environment }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  tags                = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "high_rejection_rate" {
  alarm_name          = "${local.name_prefix}-high-rejection-rate"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "RejectionRate"
  namespace           = "Customer360/Pipeline"
  period              = 300
  statistic           = "Maximum"
  threshold           = 5
  alarm_description   = "DQ rejection rate >= 5% warning threshold (staging)"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = var.environment }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  tags                = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "zero_records" {
  alarm_name          = "${local.name_prefix}-no-valid-records-24h"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ValidRecordCount"
  namespace           = "Customer360/Pipeline"
  period              = 86400
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "No valid records reached silver in 24 hours (staging). Only meaningful once the schedule is enabled."
  treat_missing_data  = var.schedule_enabled ? "breaching" : "notBreaching"
  dimensions          = { Environment = var.environment }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  tags                = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Dashboard
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "customer360_staging" {
  dashboard_name = "Customer360-Staging"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title = "DQ Gate Verdicts"
          metrics = [
            ["Customer360/Pipeline", "DQGatePassed", "Environment", "staging", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "DQGateFailed", "Environment", "staging", { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view   = "timeSeries"
          region = "us-east-1"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title = "Task Outcomes"
          metrics = [
            ["Customer360/Pipeline", "TaskSuccess", "Environment", "staging", { stat = "Sum", period = 300, color = "#1f77b4" }],
            ["Customer360/Pipeline", "TaskFailure", "Environment", "staging", { stat = "Sum", period = 300, color = "#ff7f0e" }]
          ]
          view   = "timeSeries"
          region = "us-east-1"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title = "DQ Rejection Rate by Source (%)"
          metrics = [
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "hubspot", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "oms", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "pim", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "loyalty", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "marketing", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "support", { stat = "Maximum", period = 300 }]
          ]
          view   = "timeSeries"
          region = "us-east-1"
          yAxis  = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title = "Record Throughput (Valid vs Invalid)"
          metrics = [
            ["Customer360/Pipeline", "ValidRecordCount", "Environment", "staging", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "InvalidRecordCount", "Environment", "staging", { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view   = "timeSeries"
          region = "us-east-1"
        }
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "lakehouse_bucket" {
  value = data.aws_s3_bucket.lakehouse.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.customer360_staging.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.customer360_staging.name
}

output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "state_machine_arn" {
  value = module.step_functions.state_machine_arn
}

output "glue_dbt_job_name" {
  value = module.glue_jobs.job_name
}

output "lambda_function_names" {
  value = module.lambda.function_names
}
