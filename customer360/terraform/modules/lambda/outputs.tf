output "function_arns" {
  description = "Map of all Lambda function names to ARNs"
  value       = { for k, v in aws_lambda_function.functions : k => v.arn }
}

output "function_names" {
  description = "Map of all Lambda function keys to actual AWS function names"
  value       = { for k, v in aws_lambda_function.functions : k => v.function_name }
}

output "lambda_execution_role_arn" {
  description = "ARN of the IAM role assumed by pipeline Lambdas"
  value       = aws_iam_role.lambda_exec.arn
}

output "initialize_arn" {
  description = "ARN of the initialize Lambda function"
  value       = aws_lambda_function.functions["initialize"].arn
}

output "ingest_arn" {
  description = "ARN of the ingest Lambda function"
  value       = aws_lambda_function.functions["ingest"].arn
}

output "silver_gate_arn" {
  description = "ARN of the silver_gate Lambda function"
  value       = aws_lambda_function.functions["silver_gate"].arn
}

output "quality_check_arn" {
  description = "ARN of the quality_check Lambda function"
  value       = aws_lambda_function.functions["quality_check"].arn
}

output "iceberg_load_arn" {
  description = "ARN of the iceberg_load Lambda function"
  value       = aws_lambda_function.functions["iceberg_load"].arn
}

output "reconcile_arn" {
  description = "ARN of the reconcile Lambda function"
  value       = aws_lambda_function.functions["reconcile"].arn
}

output "notify_arn" {
  description = "ARN of the notify Lambda function"
  value       = aws_lambda_function.functions["notify"].arn
}

output "iceberg_maintenance_arn" {
  description = "ARN of the iceberg_maintenance Lambda function"
  value       = aws_lambda_function.functions["iceberg_maintenance"].arn
}
