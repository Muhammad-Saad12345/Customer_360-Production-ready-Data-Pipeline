output "function_arns" {
  description = "Map of function key -> Lambda ARN"
  value       = { for k, v in aws_lambda_function.functions : k => v.arn }
}

output "function_names" {
  description = "Map of function key -> Lambda function name"
  value       = { for k, v in aws_lambda_function.functions : k => v.function_name }
}

output "role_arns" {
  description = "Map of function key -> execution role ARN"
  value       = { for k, v in aws_iam_role.function : k => v.arn }
}
