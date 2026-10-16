variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "c360"
}

variable "lambda_arns" {
  description = "Map of all Lambda function ARNs"
  type        = map(string)
}

variable "glue_dbt_job_name" {
  description = "Name of the Glue dbt runner job"
  type        = string
}

variable "glue_dbt_job_arn" {
  description = "ARN of the Glue dbt runner job"
  type        = string
}

variable "s3_dbt_path" {
  description = "S3 URI for the dbt project bundle archive (e.g. s3://bucket/artifacts/dbt/dbt_project.tar.gz)"
  type        = string
}

variable "tags" {
  description = "Tags applied to all Step Functions resources"
  type        = map(string)
  default     = {}
}
