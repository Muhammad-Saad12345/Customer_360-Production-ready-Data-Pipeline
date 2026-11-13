variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "c360"
}

variable "lakehouse_bucket_name" {
  description = "Lakehouse S3 bucket name"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "Lakehouse S3 bucket ARN"
  type        = string
}

variable "artifacts_bucket_name" {
  description = "S3 bucket where Glue scripts and dbt packages are uploaded"
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "S3 bucket ARN for scripts and packages"
  type        = string
}

variable "glue_database_name" {
  description = "Glue Catalog database name"
  type        = string
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name"
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for S3 encryption (optional)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags applied to all Glue resources"
  type        = map(string)
  default     = {}
}

variable "iceberg_s3_prefix" {
  description = "Prefix of the silver Iceberg tables (read by dbt)"
  type        = string
  default     = "iceberg"
}

variable "dbt_data_prefix" {
  description = "Prefix dbt-athena writes its models to (profiles.yml s3_data_dir)"
  type        = string
}

variable "dbt_athena_results_prefix" {
  description = "Prefix of dbt's Athena staging dir (profiles.yml s3_staging_dir)"
  type        = string
  default     = "athena-results"
}

variable "artifact_version" {
  description = "Git SHA of the artifacts (dbt_runner.py and dbt bundle) to run"
  type        = string
}

variable "permissions_boundary_arn" {
  description = "ARN of the c360-workload-boundary policy"
  type        = string
  default     = ""
}
