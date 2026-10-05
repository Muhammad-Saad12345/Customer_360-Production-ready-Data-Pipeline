# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Production Environment Root Configuration
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
    bucket         = "c360-prod-tfstate"
    key            = "customer360/prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "c360-prod-tflock"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags { tags = local.common_tags }
}

variable "environment" { type = string; default = "prod" }
variable "aws_region" { type = string; default = "us-east-1" }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "github_org" { type = string }
variable "github_repo" { type = string }
variable "alert_email_addresses" { type = list(string) }

locals {
  common_tags = {
    Project     = "Customer360"
    Environment = "prod"
    ManagedBy   = "Terraform"
  }
}

module "core" {
  source       = "../../modules/core"
  environment  = var.environment
  project_name = "c360"
  aws_region   = var.aws_region
  tags         = local.common_tags

  s3_raw_retention_days        = 90
  s3_quarantine_retention_days = 30
}

module "iam" {
  source       = "../../modules/iam"
  environment  = var.environment
  project_name = "c360"
  tags         = local.common_tags

  lakehouse_bucket_arn  = module.core.lakehouse_bucket_arn
  mwaa_dags_bucket_arn  = module.core.mwaa_dags_bucket_arn
  kms_key_arn           = module.core.kms_key_arn
  glue_database_name    = module.core.glue_database_name
  athena_workgroup_name = module.core.athena_workgroup_name
  github_org            = var.github_org
  github_repo           = var.github_repo
}

module "observability" {
  source       = "../../modules/observability"
  environment  = var.environment
  project_name = "c360"
  tags         = local.common_tags

  alert_email_addresses             = var.alert_email_addresses
  dq_rejection_rate_alarm_threshold = 10
  task_failure_alarm_threshold      = 1
}

module "mwaa" {
  source       = "../../modules/mwaa"
  environment  = var.environment
  project_name = "c360"
  aws_region   = var.aws_region
  tags         = local.common_tags

  mwaa_dags_bucket_name       = module.core.mwaa_dags_bucket_name
  pipeline_execution_role_arn = module.iam.pipeline_execution_role_arn
  kms_key_arn                 = module.core.kms_key_arn
  vpc_id                      = var.vpc_id
  private_subnet_ids          = var.private_subnet_ids

  airflow_version   = "2.9.2"
  environment_class = "mw1.large"   # production-grade sizing
  min_workers       = 2
  max_workers       = 8
}

output "lakehouse_bucket" { value = module.core.lakehouse_bucket_name }
output "mwaa_url"         { value = module.mwaa.mwaa_webserver_url }
output "sns_topic_arn"    { value = module.observability.sns_alert_topic_arn }
output "deploy_role_arn"  { value = module.iam.github_deploy_role_arn }
