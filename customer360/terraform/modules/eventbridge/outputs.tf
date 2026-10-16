output "schedule_arn" {
  description = "ARN of the EventBridge Scheduler schedule"
  value       = aws_scheduler_schedule.customer360_batch.arn
}

output "schedule_name" {
  description = "Name of the EventBridge Scheduler schedule"
  value       = aws_scheduler_schedule.customer360_batch.name
}

output "scheduler_role_arn" {
  description = "ARN of the IAM role assumed by EventBridge Scheduler"
  value       = aws_iam_role.scheduler_exec.arn
}
