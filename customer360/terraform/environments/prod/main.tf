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

module "lambda" {
  source                = "../../modules/lambda"
  environment           = var.environment
  project_name          = "c360"
  lakehouse_bucket_name = module.core.lakehouse_bucket_name
  lakehouse_bucket_arn  = module.core.lakehouse_bucket_arn
  glue_database_name    = module.core.glue_database_name
  athena_workgroup_name = module.core.athena_workgroup_name
  sns_topic_arn         = module.observability.sns_alert_topic_arn
  kms_key_arn           = module.core.kms_key_arn
  tags                  = local.common_tags
}

module "glue_jobs" {
  source                = "../../modules/glue_jobs"
  environment           = var.environment
  project_name          = "c360"
  lakehouse_bucket_name = module.core.lakehouse_bucket_name
  lakehouse_bucket_arn  = module.core.lakehouse_bucket_arn
  artifacts_bucket_name = module.core.artifacts_bucket_name
  artifacts_bucket_arn  = module.core.artifacts_bucket_arn
  glue_database_name    = module.core.glue_database_name
  athena_workgroup_name = module.core.athena_workgroup_name
  kms_key_arn           = module.core.kms_key_arn
  tags                  = local.common_tags
}

module "step_functions" {
  source            = "../../modules/step_functions"
  environment       = var.environment
  project_name      = "c360"
  lambda_arns       = module.lambda.function_arns
  glue_dbt_job_name = module.glue_jobs.job_name
  glue_dbt_job_arn  = module.glue_jobs.job_arn
  s3_dbt_path       = "s3://${module.core.artifacts_bucket_name}/artifacts/dbt/dbt_project.tar.gz"
  tags              = local.common_tags
}

module "eventbridge" {
  source              = "../../modules/eventbridge"
  environment         = var.environment
  project_name        = "c360"
  state_machine_arn   = module.step_functions.state_machine_arn
  schedule_expression = "cron(0 2 * * ? *)" # Daily 02:00 UTC
  schedule_enabled    = true
  tags                = local.common_tags
}

output "lakehouse_bucket"                  { value = module.core.lakehouse_bucket_name }
output "artifacts_bucket"                  { value = module.core.artifacts_bucket_name }
output "sns_topic_arn"                     { value = module.observability.sns_alert_topic_arn }
output "deploy_role_arn"                   { value = module.iam.github_deploy_role_arn }
output "step_functions_state_machine_arn"  { value = module.step_functions.state_machine_arn }
output "step_functions_state_machine_name" { value = module.step_functions.state_machine_name }
output "glue_dbt_job_name"                 { value = module.glue_jobs.job_name }
output "eventbridge_schedule_arn"          { value = module.eventbridge.schedule_arn }

