variable "environment" {
  description = "Deployment environment (staging | prod)"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "c360"
}

variable "lakehouse_bucket_name" {
  description = "Name of the lakehouse S3 bucket"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse S3 bucket"
  type        = string
}

variable "iceberg_s3_prefix" {
  description = "Prefix (no slashes) under which silver Iceberg tables live"
  type        = string
  default     = "iceberg"
}

variable "glue_database_name" {
  description = "Name of the Glue Catalog database"
  type        = string
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name"
  type        = string
}

variable "sns_topic_arn" {
  description = "SNS topic for pipeline notifications"
  type        = string
  default     = ""
}

variable "kms_key_arn" {
  description = "KMS key protecting the lakehouse bucket (empty when the bucket uses SSE-S3)"
  type        = string
  default     = ""
}

variable "legacy_secret_name" {
  description = "Existing shared secret (HubSpot/Zendesk/marketing keys). Empty to disable."
  type        = string
  default     = ""
}

variable "permissions_boundary_arn" {
  description = "ARN of the c360-workload-boundary policy (from terraform/bootstrap)"
  type        = string
  default     = ""
}

variable "s3_artifacts_bucket" {
  description = "Bucket holding artifacts/<artifact_version>/lambdas/<fn>.zip"
  type        = string
}

variable "artifact_version" {
  description = "Git SHA (or other immutable id) of the artifacts to deploy"
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]{7,64}$", var.artifact_version))
    error_message = "artifact_version must be an immutable build id such as a git SHA."
  }
}

variable "source_modes" {
  description = "Map of source name -> api | mock_api | synthetic | dataset"
  type        = map(string)

  validation {
    condition     = alltrue([for m in values(var.source_modes) : contains(["api", "mock_api", "synthetic", "dataset"], m)])
    error_message = "source_modes values must be one of api, mock_api, synthetic, dataset."
  }
}

variable "required_source_mode" {
  description = "If set (e.g. \"api\" in prod), any source running in another mode fails the run"
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch log retention"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional tags"
  type        = map(string)
  default     = {}
}

variable "mandatory_sources" {
  description = "Sources whose failure stops the run before dbt; other failures become warnings"
  type        = list(string)
  default     = ["hubspot", "oms"]
}

variable "synthetic_include_invalid" {
  description = "Include the deliberately broken records in synthetic batches (set true only to test the DQ gate)"
  type        = bool
  default     = false
}

variable "enabled_sources" {
  description = "Sources a scheduled run processes when the execution input does not list them"
  type        = list(string)
  default     = ["hubspot", "oms", "pim", "loyalty", "marketing", "support"]
}
