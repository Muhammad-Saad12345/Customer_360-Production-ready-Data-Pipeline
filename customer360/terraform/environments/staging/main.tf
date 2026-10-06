# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Staging Environment Root Configuration
#
# IMPORTANT: Several AWS resources already exist (MWAA, S3 buckets, IAM roles,
# security group). This configuration IMPORTS those resources and only creates
# what is genuinely missing (Glue DB, Athena workgroup, SNS topic, CloudWatch
# alarms, log groups).
#
# DO NOT run terraform apply without first running: ./scripts/tf_import_staging.sh
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
    bucket         = "c360-staging-tfstate"
    key            = "customer360/staging/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "c360-staging-tflock"
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
# Data sources — reference pre-existing AWS resources (do NOT recreate)
# ─────────────────────────────────────────────────────────────────────────────

# Existing S3 lakehouse bucket
data "aws_s3_bucket" "lakehouse" {
  bucket = "c360-bucket786"
}

# Existing S3 MWAA DAGs bucket
data "aws_s3_bucket" "mwaa_dags" {
  bucket = "c360-mwaa-prod-786"
}

# Existing MWAA execution role
data "aws_iam_role" "mwaa_execution" {
  name = "C360-MWAA-Execution-Role"
}

# Existing GitHub deploy role
data "aws_iam_role" "github_deploy" {
  name = "github-mwaa-deploy-role"
}

# Existing MWAA security group
data "aws_security_group" "mwaa" {
  id = "sg-0a65f661ef6fd310b"
}

# ─────────────────────────────────────────────────────────────────────────────
# Glue Data Catalog Database (create if not already present)
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
# Athena Workgroup (create if not already present)
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
# CloudWatch Log Groups — MWAA (5 log types, 30-day retention)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "mwaa_dag_processing" {
  name              = "/customer360/staging/mwaa/dag-processing"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_scheduler" {
  name              = "/customer360/staging/mwaa/scheduler"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_task" {
  name              = "/customer360/staging/mwaa/task"
  retention_in_days = 30
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_webserver" {
  name              = "/customer360/staging/mwaa/webserver"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_worker" {
  name              = "/customer360/staging/mwaa/worker"
  retention_in_days = 30
  tags              = local.common_tags
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
            ["Customer360/Pipeline", "TaskSuccess", "Environment", "staging", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "TaskFailure", "Environment", "staging", { stat = "Sum", period = 300, color = "#d62728" }]
          ]
          view   = "timeSeries"
          region = "us-east-1"
        }
      },
      {
        type   = "metric"
        x = 0; y = 6; width = 24; height = 6
        properties = {
          title   = "Record Counts"
          metrics = [
            ["Customer360/Pipeline", "InputRecordCount",   "Environment", "staging", { stat = "Sum", period = 300 }],
            ["Customer360/Pipeline", "ValidRecordCount",   "Environment", "staging", { stat = "Sum", period = 300, color = "#2ca02c" }],
            ["Customer360/Pipeline", "InvalidRecordCount", "Environment", "staging", { stat = "Sum", period = 300, color = "#ff7f0e" }]
          ]
          view   = "timeSeries"
          region = "us-east-1"
        }
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — inline policy additions to EXISTING MWAA execution role
# Adds: secretsmanager, sns, cloudwatch — if not already present
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role_policy" "mwaa_secrets" {
  name = "c360-staging-secrets-access"
  role = data.aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SecretsManagerRead"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [
          "arn:aws:secretsmanager:us-east-1:${local.account_id}:secret:c_360_secret_manager*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_cloudwatch" {
  name = "c360-staging-cloudwatch-metrics"
  role = data.aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
        Condition = {
          StringEquals = { "cloudwatch:namespace" = "Customer360/Pipeline" }
        }
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream",
          "logs:PutLogEvents", "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:us-east-1:${local.account_id}:log-group:/customer360/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_sns" {
  name = "c360-staging-sns-publish"
  role = data.aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.pipeline_alerts.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_glue" {
  name = "c360-staging-glue-athena"
  role = data.aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:CreatePartition", "glue:BatchCreatePartition",
          "glue:UpdatePartition", "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${local.account_id}:catalog",
          "arn:aws:glue:us-east-1:${local.account_id}:database/customer360_iceberg_stage",
          "arn:aws:glue:us-east-1:${local.account_id}:table/customer360_iceberg_stage/*"
        ]
      },
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:StopQueryExecution",
          "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:GetWorkGroup", "athena:ListQueryExecutions"
        ]
        Resource = [
          "arn:aws:athena:us-east-1:${local.account_id}:workgroup/customer360-staging"
        ]
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "lakehouse_bucket"        { value = data.aws_s3_bucket.lakehouse.bucket }
output "mwaa_dags_bucket"        { value = data.aws_s3_bucket.mwaa_dags.bucket }
output "glue_database"           { value = aws_glue_catalog_database.customer360_staging.name }
output "athena_workgroup"        { value = aws_athena_workgroup.customer360_staging.name }
output "sns_topic_arn"           { value = aws_sns_topic.pipeline_alerts.arn }
output "mwaa_execution_role_arn" { value = data.aws_iam_role.mwaa_execution.arn }
output "github_deploy_role_arn"  { value = data.aws_iam_role.github_deploy.arn }
