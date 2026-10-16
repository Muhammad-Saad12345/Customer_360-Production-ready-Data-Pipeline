variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "c360"
}

variable "state_machine_arn" {
  description = "ARN of the Step Functions state machine to trigger"
  type        = string
}

variable "schedule_expression" {
  description = "Cron or rate expression for the pipeline schedule (e.g., cron(0 2 * * ? *))"
  type        = string
  default     = "cron(0 2 * * ? *)" # Daily at 02:00 UTC
}

variable "schedule_enabled" {
  description = "Whether the EventBridge schedule is active"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to all EventBridge resources"
  type        = map(string)
  default     = {}
}
