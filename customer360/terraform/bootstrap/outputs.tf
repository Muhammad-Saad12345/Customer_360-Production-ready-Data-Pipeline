output "plan_role_arn" {
  description = "GitHub variable AWS_PLAN_ROLE_ARN (repository level)"
  value       = aws_iam_role.github_plan.arn
}

output "staging_deploy_role_arn" {
  description = "GitHub variable AWS_DEPLOY_ROLE_ARN in environment 'staging'"
  value       = module.deploy_staging.role_arn
}

output "prod_deploy_role_arn" {
  description = "GitHub variable AWS_DEPLOY_ROLE_ARN in environment 'production'"
  value       = module.deploy_prod.role_arn
}

output "operator_role_arn" {
  value = aws_iam_role.operator.arn
}

output "boundary_policy_arn" {
  value = aws_iam_policy.workload_boundary.arn
}

output "state_buckets" {
  value = { for k, b in aws_s3_bucket.state : k => b.bucket }
}
