# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Staging Environment Root Configuration
# Architecture: Step Functions + Lambda + Glue + Athena + EventBridge
# (MWAA has been retired to optimize cost from ~$350/mo to ~$20/mo)
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket  = "c360-staging-tfstate"
    key     = "customer360/staging/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags { tags = local.common_tags }
}

# ─────────────────────────────────────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "environment"           { type = string; default = "staging" }
variable "aws_region"            { type = string; default = "us-east-1" }
variable "vpc_id"                { type = string }
variable "private_subnet_ids"    { type = list(string) }
variable "github_org"            { type = string }
variable "github_repo"           { type = string }
variable "alert_email_addresses" { type = list(string); default = [] }

locals {
  account_id  = data.aws_caller_identity.current.account_id
  name_prefix = "c360-${var.environment}"
  common_tags = {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# Data Sources — Verified Live Resources
# ─────────────────────────────────────────────────────────────────────────────

# Verified live S3 lakehouse bucket
data "aws_s3_bucket" "lakehouse" {
  bucket = "c360-bucket786"
}

# Existing GitHub deploy role (for CI/CD OIDC)
data "aws_iam_role" "github_deploy" {
  name = "github-mwaa-deploy-role"
}

# ─────────────────────────────────────────────────────────────────────────────
# Glue Data Catalog Database
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "customer360_staging" {
  name        = "customer360_iceberg_stage"
  description = "Customer 360 Lakehouse — Iceberg tables (staging)"

  create_table_default_permission {
    permissions = ["SELECT"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Athena Workgroup
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "customer360_staging" {
  name  = "customer360-staging"
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

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
# SNS — Pipeline Alerts Topic
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-pipeline-alerts" })
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_email_addresses)
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# ─────────────────────────────────────────────────────────────────────────────
# Modules: Lambda Functions (8 Specialized Pipeline Handlers)
# ─────────────────────────────────────────────────────────────────────────────

module "lambda" {
  source                = "../../modules/lambda"
  environment           = var.environment
  project_name          = "c360"
  lakehouse_bucket_name = data.aws_s3_bucket.lakehouse.bucket
  lakehouse_bucket_arn  = data.aws_s3_bucket.lakehouse.arn
  glue_database_name    = aws_glue_catalog_database.customer360_staging.name
  athena_workgroup_name = aws_athena_workgroup.customer360_staging.name
  sns_topic_arn         = aws_sns_topic.pipeline_alerts.arn
  tags                  = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Modules: Glue Job (dbt Runner on Python Shell)
# ─────────────────────────────────────────────────────────────────────────────

module "glue_jobs" {
  source                = "../../modules/glue_jobs"
  environment           = var.environment
  project_name          = "c360"
  lakehouse_bucket_name = data.aws_s3_bucket.lakehouse.bucket
  lakehouse_bucket_arn  = data.aws_s3_bucket.lakehouse.arn
  artifacts_bucket_name = data.aws_s3_bucket.lakehouse.bucket
  artifacts_bucket_arn  = data.aws_s3_bucket.lakehouse.arn
  glue_database_name    = aws_glue_catalog_database.customer360_staging.name
  athena_workgroup_name = aws_athena_workgroup.customer360_staging.name
  tags                  = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Modules: Step Functions (End-to-End Batch Orchestration)
# ─────────────────────────────────────────────────────────────────────────────

module "step_functions" {
  source            = "../../modules/step_functions"
  environment       = var.environment
  project_name      = "c360"
  lambda_arns       = module.lambda.function_arns
  glue_dbt_job_name = module.glue_jobs.job_name
  glue_dbt_job_arn  = module.glue_jobs.job_arn
  s3_dbt_path       = "s3://${data.aws_s3_bucket.lakehouse.bucket}/artifacts/dbt/dbt_project.tar.gz"
  tags              = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Modules: EventBridge Scheduler (Daily Trigger)
# ─────────────────────────────────────────────────────────────────────────────

module "eventbridge" {
  source              = "../../modules/eventbridge"
  environment         = var.environment
  project_name        = "c360"
  state_machine_arn   = module.step_functions.state_machine_arn
  schedule_expression = "cron(0 2 * * ? *)" # Daily at 02:00 UTC
  schedule_enabled    = true
  tags                = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Alarms — Pipeline Health & Data Quality
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
  alarm_description   = "DQ gate blocked an Iceberg promotion (staging)"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = "staging" }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  tags                = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "task_failures" {
  alarm_name          = "${local.name_prefix}-task-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "TaskFailure"
  namespace           = "Customer360/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "One or more Customer 360 pipeline tasks failed (staging)"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = "staging" }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions          = [aws_sns_topic.pipeline_alerts.arn]
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
  threshold           = 10
  alarm_description   = "DQ rejection rate >= 10% (staging)"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = "staging", Tier = "quality" }
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
        x = 0; y = 0; width = 12; height = 6
        properties = {
          title   = "DQ Gate Verdicts"
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
        x = 12; y = 0; width = 12; height = 6
        properties = {
          title   = "Task Outcomes"
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
        x = 0; y = 6; width = 12; height = 6
        properties = {
          title   = "DQ Rejection Rate by Source (%)"
          metrics = [
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "hubspot", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "oms", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "pim", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "loyalty", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "marketing", { stat = "Maximum", period = 300 }],
            ["Customer360/Pipeline", "RejectionRate", "Environment", "staging", "Source", "support", { stat = "Maximum", period = 300 }]
          ]
          view    = "timeSeries"
          region  = "us-east-1"
          yAxis   = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x = 12; y = 6; width = 12; height = 6
        properties = {
          title   = "Record Throughput (Valid vs Invalid)"
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

output "lakehouse_bucket"                   { value = data.aws_s3_bucket.lakehouse.bucket }
output "glue_database"                      { value = aws_glue_catalog_database.customer360_staging.name }
output "athena_workgroup"                   { value = aws_athena_workgroup.customer360_staging.name }
output "sns_topic_arn"                      { value = aws_sns_topic.pipeline_alerts.arn }
output "step_functions_state_machine_arn"   { value = module.step_functions.state_machine_arn }
output "step_functions_state_machine_name"  { value = module.step_functions.state_machine_name }
output "glue_dbt_job_name"                  { value = module.glue_jobs.job_name }
output "eventbridge_schedule_arn"           { value = module.eventbridge.schedule_arn }
output "lambda_function_arns"               { value = module.lambda.function_arns }
output "github_deploy_role_arn"             { value = data.aws_iam_role.github_deploy.arn }
