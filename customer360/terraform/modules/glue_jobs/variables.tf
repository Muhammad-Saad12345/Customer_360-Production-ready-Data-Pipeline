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
