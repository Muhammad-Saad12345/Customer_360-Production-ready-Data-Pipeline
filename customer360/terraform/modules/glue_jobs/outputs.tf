output "job_name" {
  description = "Name of the Glue dbt runner job"
  value       = aws_glue_job.dbt_runner.name
}

output "job_arn" {
  description = "ARN of the Glue dbt runner job"
  value       = aws_glue_job.dbt_runner.arn
}

output "glue_execution_role_arn" {
  description = "ARN of the IAM role assumed by Glue jobs"
  value       = aws_iam_role.glue_exec.arn
}
