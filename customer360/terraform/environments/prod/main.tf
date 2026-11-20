# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Production Environment Root Configuration
#
# Deployed only by c360-github-deploy-prod (GitHub environment "production",
# required reviewer). Requires terraform/bootstrap to have been applied once.
#
# First deploy: CI applies `-target=module.core` first so the artifacts bucket
# exists before artifacts are uploaded, then runs the full plan/apply.
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
    bucket       = "c360-prod-tfstate"
    key          = "customer360/prod/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.common_tags
  }
}

variable "environment" {
  type    = string
  default = "prod"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "alert_email_addresses" {
  description = "Emails subscribed to prod alerts (each must confirm by email)"
  type        = list(string)
  default     = []
}

variable "artifact_version" {
  description = "Git SHA of the artifacts uploaded by CI"
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
  description = "Enable only after three manual prod runs pass the data checks"
  type        = bool
  default     = false
}

variable "source_modes" {
  description = "Production accepts real APIs only; C360_REQUIRED_SOURCE_MODE=api enforces it at runtime."
  type        = map(string)
  default = {
    hubspot   = "api"
    oms       = "api"
    pim       = "api"
    loyalty   = "api"
    marketing = "api"
    support   = "api"
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

# ─────────────────────────────────────────────────────────────────────────────
# Core: KMS key, lakehouse + artifacts buckets, Glue DB, Athena workgroup
# ─────────────────────────────────────────────────────────────────────────────

module "core" {
  source       = "../../modules/core"
  environment  = var.environment
  project_name = "c360"
  aws_region   = var.aws_region
  tags         = local.common_tags

  s3_raw_retention_days        = 90
  s3_quarantine_retention_days = 30
}

# ─────────────────────────────────────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-pipeline-alerts" })
}

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
# Compute and orchestration
# ─────────────────────────────────────────────────────────────────────────────

module "lambda" {
  source                   = "../../modules/lambda"
  environment              = var.environment
  project_name             = "c360"
  lakehouse_bucket_name    = module.core.lakehouse_bucket_name
  lakehouse_bucket_arn     = module.core.lakehouse_bucket_arn
  glue_database_name       = module.core.glue_database_name
  athena_workgroup_name    = module.core.athena_workgroup_name
  sns_topic_arn            = aws_sns_topic.pipeline_alerts.arn
  kms_key_arn              = module.core.kms_key_arn
  legacy_secret_name       = ""
  permissions_boundary_arn = local.boundary_arn
  s3_artifacts_bucket      = module.core.artifacts_bucket_name
  artifact_version         = var.artifact_version
  source_modes             = var.source_modes
  enabled_sources          = var.enabled_sources
  mandatory_sources        = var.mandatory_sources
  required_source_mode     = "api"
  log_retention_days       = 90
  tags                     = local.common_tags
}

module "glue_jobs" {
  source                    = "../../modules/glue_jobs"
  environment               = var.environment
  project_name              = "c360"
  lakehouse_bucket_name     = module.core.lakehouse_bucket_name
  lakehouse_bucket_arn      = module.core.lakehouse_bucket_arn
  artifacts_bucket_name     = module.core.artifacts_bucket_name
  artifacts_bucket_arn      = module.core.artifacts_bucket_arn
  glue_database_name        = module.core.glue_database_name
  athena_workgroup_name     = module.core.athena_workgroup_name
  kms_key_arn               = module.core.kms_key_arn
  dbt_data_prefix           = "dbt"
  dbt_athena_results_prefix = "athena-results"
  artifact_version          = var.artifact_version
  permissions_boundary_arn  = local.boundary_arn
  tags                      = local.common_tags
}

module "step_functions" {
  source                   = "../../modules/step_functions"
  environment              = var.environment
  project_name             = "c360"
  lambda_arns              = module.lambda.function_arns
  glue_dbt_job_name        = module.glue_jobs.job_name
  glue_dbt_job_arn         = module.glue_jobs.job_arn
  s3_dbt_path              = "s3://${module.core.artifacts_bucket_name}/artifacts/${var.artifact_version}/dbt/dbt_project.tar.gz"
  alert_topic_arn          = aws_sns_topic.pipeline_alerts.arn
  permissions_boundary_arn = local.boundary_arn
  log_retention_days       = 90
  tags                     = local.common_tags
}

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
# Data-quality alarms (see staging for the dimension convention)
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
  alarm_description   = "A DQ gate blocked Iceberg promotion (prod)"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = var.environment }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
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
  alarm_description   = "No valid records reached silver in 24 hours (prod)"
  treat_missing_data  = var.schedule_enabled ? "breaching" : "notBreaching"
  dimensions          = { Environment = var.environment }
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
}

output "lakehouse_bucket" {
  value = module.core.lakehouse_bucket_name
}

output "artifacts_bucket" {
  value = module.core.artifacts_bucket_name
}

output "glue_database" {
  value = module.core.glue_database_name
}

output "athena_workgroup" {
  value = module.core.athena_workgroup_name
}

output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "state_machine_arn" {
  value = module.step_functions.state_machine_arn
}

output "lambda_function_names" {
  value = module.lambda.function_names
}
