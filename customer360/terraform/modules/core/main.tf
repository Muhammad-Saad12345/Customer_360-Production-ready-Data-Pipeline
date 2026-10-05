# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — Core Infrastructure Module
# Manages: S3 buckets, Glue database, Athena workgroup, KMS key
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

variable "project_name" {
  description = "Project name prefix for all resource names"
  type        = string
  default     = "c360"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}

variable "athena_query_results_retention_days" {
  description = "Days to retain Athena query result objects"
  type        = number
  default     = 30
}

variable "s3_raw_retention_days" {
  description = "Days to retain raw landing zone objects before transition to Glacier"
  type        = number
  default     = 90
}

variable "s3_quarantine_retention_days" {
  description = "Days to retain quarantine records (PII compliance)"
  type        = number
  default     = 30
}

# ─────────────────────────────────────────────────────────────────────────────
# Locals
# ─────────────────────────────────────────────────────────────────────────────

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# KMS Key (encryption at rest for S3, Glue)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_kms_key" "lakehouse" {
  description             = "Customer 360 Lakehouse encryption key (${var.environment})"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-lakehouse-key" })
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${local.name_prefix}-lakehouse"
  target_key_id = aws_kms_key.lakehouse.key_id
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 — Main Lakehouse Bucket (raw / silver / iceberg / quarantine)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${local.name_prefix}-lakehouse"
  force_destroy = var.environment != "prod"

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-lakehouse" })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "raw-tiering"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = var.s3_raw_retention_days
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "quarantine-expiry"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration {
      days = var.s3_quarantine_retention_days
    }
  }

  rule {
    id     = "athena-results-expiry"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration {
      days = var.athena_query_results_retention_days
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 — MWAA DAG Bucket (separate from lakehouse data)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "mwaa_dags" {
  bucket        = "${local.name_prefix}-mwaa-dags"
  force_destroy = var.environment != "prod"

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-mwaa-dags" })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa_dags" {
  bucket = aws_s3_bucket.mwaa_dags.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "mwaa_dags" {
  bucket = aws_s3_bucket.mwaa_dags.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "mwaa_dags" {
  bucket                  = aws_s3_bucket.mwaa_dags.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─────────────────────────────────────────────────────────────────────────────
# Glue Data Catalog Database
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "customer360" {
  name        = "customer360_${var.environment}"
  description = "Customer 360 Lakehouse — Iceberg tables (${var.environment})"

  create_table_default_permission {
    permissions = ["SELECT"]
    principal { data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS" }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Athena Workgroup
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "customer360" {
  name  = "customer360-${var.environment}"
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.lakehouse.arn
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = merge(local.common_tags, { Name = "customer360-${var.environment}" })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "lakehouse_bucket_name" {
  description = "Name of the main lakehouse S3 bucket"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_bucket_arn" {
  description = "ARN of the main lakehouse S3 bucket"
  value       = aws_s3_bucket.lakehouse.arn
}

output "mwaa_dags_bucket_name" {
  description = "Name of the MWAA DAGs S3 bucket"
  value       = aws_s3_bucket.mwaa_dags.bucket
}

output "mwaa_dags_bucket_arn" {
  description = "ARN of the MWAA DAGs S3 bucket"
  value       = aws_s3_bucket.mwaa_dags.arn
}

output "kms_key_arn" {
  description = "ARN of the KMS encryption key"
  value       = aws_kms_key.lakehouse.arn
}

output "kms_key_id" {
  description = "ID of the KMS encryption key"
  value       = aws_kms_key.lakehouse.key_id
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.customer360.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.customer360.name
}
