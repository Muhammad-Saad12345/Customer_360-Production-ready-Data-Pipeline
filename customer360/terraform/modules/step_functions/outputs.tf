output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.customer360_batch.arn
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.customer360_batch.name
}

output "step_functions_role_arn" {
  description = "ARN of the IAM role assumed by Step Functions"
  value       = aws_iam_role.step_functions_exec.arn
}

output "execution_failed_rule_arn" {
  description = "EventBridge rule that forwards failed executions to SNS"
  value       = aws_cloudwatch_event_rule.execution_failed.arn
}
