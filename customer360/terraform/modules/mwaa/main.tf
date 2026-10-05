# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — AWS MWAA Module
# Manages: MWAA environment, security group, CloudWatch log groups
# ─────────────────────────────────────────────────────────────────────────────

variable "environment" { type = string }
variable "project_name" { type = string; default = "c360" }
variable "aws_region" { type = string; default = "us-east-1" }
variable "mwaa_dags_bucket_name" { type = string }
variable "pipeline_execution_role_arn" { type = string }
variable "kms_key_arn" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "airflow_version" { type = string; default = "2.9.2" }
variable "environment_class" {
  description = "MWAA environment class (mw1.small | mw1.medium | mw1.large)"
  type        = string
  default     = "mw1.medium"
}
variable "min_workers" { type = number; default = 1 }
variable "max_workers" { type = number; default = 4 }
variable "tags" { type = map(string); default = {} }

# Custom Airflow config overrides — all non-sensitive; secrets go through
# Secrets Manager and are referenced via airflow_configuration_options
variable "airflow_config_overrides" {
  description = "Extra Airflow configuration key/value pairs (non-sensitive)"
  type        = map(string)
  default     = {}
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Log Groups (MWAA generates 5 log types)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "mwaa_dag_processing" {
  name              = "/customer360/${var.environment}/mwaa/dag-processing"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_scheduler" {
  name              = "/customer360/${var.environment}/mwaa/scheduler"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_task" {
  name              = "/customer360/${var.environment}/mwaa/task"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_webserver" {
  name              = "/customer360/${var.environment}/mwaa/webserver"
  retention_in_days = 14
  kms_key_id        = var.kms_key_arn
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa_worker" {
  name              = "/customer360/${var.environment}/mwaa/worker"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
  tags              = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Security Group — MWAA workers and scheduler
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "mwaa" {
  name        = "${local.name_prefix}-mwaa"
  description = "Security group for Customer 360 MWAA environment"
  vpc_id      = var.vpc_id

  # Allow all outbound (MWAA must reach S3, Glue, Athena, Secrets Manager)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  # Allow intra-SG communication (workers <-> scheduler <-> webserver)
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
    description = "Allow intra-security-group traffic"
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-mwaa" })
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA Environment
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_mwaa_environment" "customer360" {
  name               = "customer360-${var.environment}"
  airflow_version    = var.airflow_version
  environment_class  = var.environment_class
  min_workers        = var.min_workers
  max_workers        = var.max_workers
  execution_role_arn = var.pipeline_execution_role_arn
  kms_key            = var.kms_key_arn

  source_bucket_arn   = "arn:aws:s3:::${var.mwaa_dags_bucket_name}"
  dag_s3_path         = "dags/"
  requirements_s3_path = "dags/requirements.txt"
  plugins_s3_path      = "dags/plugins.zip"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = var.private_subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "WARNING"
      cloud_watch_log_group_arn = "${aws_cloudwatch_log_group.mwaa_dag_processing.arn}:*"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
      cloud_watch_log_group_arn = "${aws_cloudwatch_log_group.mwaa_scheduler.arn}:*"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
      cloud_watch_log_group_arn = "${aws_cloudwatch_log_group.mwaa_task.arn}:*"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
      cloud_watch_log_group_arn = "${aws_cloudwatch_log_group.mwaa_webserver.arn}:*"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
      cloud_watch_log_group_arn = "${aws_cloudwatch_log_group.mwaa_worker.arn}:*"
    }
  }

  airflow_configuration_options = merge(
    {
      # Secrets backend — Airflow reads variables/connections from Secrets Manager
      "secrets.backend"                                   = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
      "secrets.backend_kwargs"                            = "{\"connections_prefix\": \"customer360/${var.environment}/airflow/connections\", \"variables_prefix\": \"customer360/${var.environment}/airflow/variables\"}"
      # Parallelism / scheduling
      "core.parallelism"                                  = "16"
      "core.dag_concurrency"                              = "8"
      "core.max_active_runs_per_dag"                      = "1"
      "scheduler.dag_dir_list_interval"                   = "60"
      # Disable example DAGs
      "core.load_examples"                                = "False"
      # Increase XCom size limit (needed for silver summary payloads)
      "core.xcom_backend"                                 = "airflow.models.xcom.BaseXCom"
    },
    var.airflow_config_overrides
  )

  tags = merge(local.common_tags, { Name = "customer360-${var.environment}" })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "mwaa_webserver_url" {
  description = "MWAA Airflow UI URL"
  value       = aws_mwaa_environment.customer360.webserver_url
}

output "mwaa_environment_arn" {
  description = "MWAA environment ARN"
  value       = aws_mwaa_environment.customer360.arn
}

output "mwaa_security_group_id" {
  description = "Security group ID for the MWAA environment"
  value       = aws_security_group.mwaa.id
}
