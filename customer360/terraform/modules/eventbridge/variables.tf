variable "environment" {
  description = "Deployment environment (staging | prod)"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "c360"
}

variable "state_machine_arn" {
  description = "ARN of the batch state machine"
  type        = string
}

variable "schedule_expression" {
  description = "Schedule for the daily batch"
  type        = string
  default     = "cron(0 2 * * ? *)" # 02:00 UTC = 07:00 PKT
}

variable "schedule_enabled" {
  description = "Enable schedules. Keep false until three manual end-to-end runs pass the data checks."
  type        = bool
  default     = false
}

variable "maintenance_lambda_arn" {
  description = "ARN of the iceberg_maintenance Lambda (empty disables the weekly schedule)"
  type        = string
  default     = ""
}

variable "maintenance_schedule_expression" {
  description = "Schedule for Iceberg maintenance"
  type        = string
  default     = "cron(0 4 ? * SUN *)" # Sundays 04:00 UTC
}

variable "permissions_boundary_arn" {
  description = "ARN of the c360-workload-boundary policy"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags"
  type        = map(string)
  default     = {}
}
