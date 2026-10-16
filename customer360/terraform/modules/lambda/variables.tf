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
  description = "Name of the lakehouse S3 bucket"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse S3 bucket"
  type        = string
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
  description = "SNS alerts topic ARN"
  type        = string
  default     = ""
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption"
  type        = string
  default     = ""
}

variable "secrets_manager_arn" {
  description = "Secrets Manager ARN for API credentials"
  type        = string
  default     = ""
}

variable "s3_artifacts_bucket" {
  description = "S3 bucket containing Lambda deployment packages (optional)"
  type        = string
  default     = ""
}

variable "s3_lambda_keys" {
  description = "Map of function name to S3 object key for pre-built zip packages"
  type        = map(string)
  default     = {}
}

variable "vpc_subnet_ids" {
  description = "Subnet IDs for Lambda VPC configuration (optional)"
  type        = list(string)
  default     = []
}

variable "vpc_security_group_ids" {
  description = "Security group IDs for Lambda VPC configuration (optional)"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags applied to all Lambda resources"
  type        = map(string)
  default     = {}
}
