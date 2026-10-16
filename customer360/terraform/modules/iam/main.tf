# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 Lakehouse — IAM Module
# Manages: pipeline execution role, MWAA role, OIDC GitHub Actions role
# ─────────────────────────────────────────────────────────────────────────────

variable "environment" {
  type = string
}
variable "project_name" {
  type    = string
  default = "c360"
}
variable "lakehouse_bucket_arn" {
  type = string
}
variable "mwaa_dags_bucket_arn" {
  type    = string
  default = ""
}
variable "kms_key_arn" {
  type = string
}
variable "glue_database_name" {
  type = string
}
variable "athena_workgroup_name" {
  type = string
}
variable "tags" {
  type    = map(string)
  default = {}
}
variable "github_org" {
  description = "GitHub organisation name for OIDC trust (e.g. myorg)"
  type        = string
}
variable "github_repo" {
  description = "GitHub repository name for OIDC trust (e.g. customer360)"
  type        = string
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Customer360"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Execution Role (assumed by Lambda, Step Functions, Glue)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "pipeline_execution" {
  name = "${local.name_prefix}-pipeline-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "states.amazonaws.com",
            "glue.amazonaws.com"
          ]
        }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-pipeline-execution" })
}

resource "aws_iam_policy" "pipeline_s3" {
  name        = "${local.name_prefix}-pipeline-s3"
  description = "S3 access for Customer 360 pipeline execution"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid    = "LakehouseBucketAccess"
          Effect = "Allow"
          Action = [
            "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
            "s3:ListBucket", "s3:GetBucketLocation",
            "s3:GetObjectVersion"
          ]
          Resource = [
            var.lakehouse_bucket_arn,
            "${var.lakehouse_bucket_arn}/*"
          ]
        }
      ],
      var.mwaa_dags_bucket_arn != "" ? [
        {
          Sid    = "MWAADagsBucketReadAccess"
          Effect = "Allow"
          Action = ["s3:GetObject", "s3:ListBucket"]
          Resource = [
            var.mwaa_dags_bucket_arn,
            "${var.mwaa_dags_bucket_arn}/*"
          ]
        }
      ] : []
    )
  })
}

resource "aws_iam_policy" "pipeline_glue_athena" {
  name        = "${local.name_prefix}-pipeline-glue-athena"
  description = "Glue + Athena access for Customer 360 pipeline execution"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:CreatePartition", "glue:BatchCreatePartition",
          "glue:UpdatePartition", "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
        ]
      },
      {
        Sid    = "AthenaQueryAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:StopQueryExecution",
          "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:GetWorkGroup", "athena:ListQueryExecutions"
        ]
        Resource = [
          "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/${var.athena_workgroup_name}"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "pipeline_kms" {
  name        = "${local.name_prefix}-pipeline-kms"
  description = "KMS access for Customer 360 pipeline execution"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KMSEncryptDecrypt"
        Effect = "Allow"
        Action = [
          "kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"
        ]
        Resource = [var.kms_key_arn]
      }
    ]
  })
}

resource "aws_iam_policy" "pipeline_secrets" {
  name        = "${local.name_prefix}-pipeline-secrets"
  description = "Secrets Manager access for Customer 360 API credentials"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SecretsManagerRead"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [
          "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:customer360/${var.environment}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "pipeline_cloudwatch" {
  name        = "${local.name_prefix}-pipeline-cloudwatch"
  description = "CloudWatch metrics + logs for Customer 360 observability"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
        Condition = {
          StringEquals = { "cloudwatch:namespace" = "Customer360/Pipeline" }
        }
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream",
          "logs:PutLogEvents", "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/customer360/${var.environment}/*"
        ]
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = [
          "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:customer360-${var.environment}-alerts"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "pipeline_s3" {
  role       = aws_iam_role.pipeline_execution.name
  policy_arn = aws_iam_policy.pipeline_s3.arn
}
resource "aws_iam_role_policy_attachment" "pipeline_glue_athena" {
  role       = aws_iam_role.pipeline_execution.name
  policy_arn = aws_iam_policy.pipeline_glue_athena.arn
}
resource "aws_iam_role_policy_attachment" "pipeline_kms" {
  role       = aws_iam_role.pipeline_execution.name
  policy_arn = aws_iam_policy.pipeline_kms.arn
}
resource "aws_iam_role_policy_attachment" "pipeline_secrets" {
  role       = aws_iam_role.pipeline_execution.name
  policy_arn = aws_iam_policy.pipeline_secrets.arn
}
resource "aws_iam_role_policy_attachment" "pipeline_cloudwatch" {
  role       = aws_iam_role.pipeline_execution.name
  policy_arn = aws_iam_policy.pipeline_cloudwatch.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# GitHub Actions OIDC Role (keyless deploy)
# ─────────────────────────────────────────────────────────────────────────────

data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_role" "github_deploy" {
  name = "${local.name_prefix}-github-deploy"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
        Action    = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_org}/${var.github_repo}:*"
          }
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-github-deploy" })
}

# Allow the deploy role to sync the DAG bucket and invoke Terraform
resource "aws_iam_role_policy_attachment" "github_deploy_s3" {
  role       = aws_iam_role.github_deploy.name
  policy_arn = aws_iam_policy.pipeline_s3.arn
}

resource "aws_iam_policy" "github_deploy_tf" {
  name        = "${local.name_prefix}-github-deploy-tf"
  description = "Terraform state and infrastructure management for CI/CD"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "TerraformStateAccess"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = ["arn:aws:s3:::${local.name_prefix}-tfstate/*", "arn:aws:s3:::${local.name_prefix}-tfstate"]
      },
      {
        Sid      = "TerraformLockTable"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
        Resource = ["arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.name_prefix}-tflock"]
      },
      {
        Sid    = "InfrastructureManagement"
        Effect = "Allow"
        Action = [
          "s3:*", "glue:*", "athena:*",
          "lambda:*", "states:*", "scheduler:*", "events:*",
          "kms:CreateKey", "kms:CreateAlias", "kms:DescribeKey",
          "kms:EnableKeyRotation", "kms:GetKeyPolicy", "kms:GetKeyRotationStatus",
          "kms:ListAliases", "kms:ListKeys", "kms:PutKeyPolicy",
          "kms:ScheduleKeyDeletion", "kms:TagResource",
          "iam:CreateRole", "iam:DeleteRole", "iam:AttachRolePolicy",
          "iam:DetachRolePolicy", "iam:GetRole", "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies", "iam:PutRolePolicy",
          "iam:CreatePolicy", "iam:DeletePolicy", "iam:GetPolicy",
          "iam:GetPolicyVersion", "iam:TagRole", "iam:TagPolicy",
          "secretsmanager:CreateSecret", "secretsmanager:DescribeSecret",
          "secretsmanager:GetSecretValue", "secretsmanager:TagResource",
          "cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms",
          "cloudwatch:DescribeAlarms", "cloudwatch:PutDashboard",
          "sns:CreateTopic", "sns:DeleteTopic", "sns:GetTopicAttributes",
          "sns:SetTopicAttributes", "sns:TagResource"
        ]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_deploy_tf" {
  role       = aws_iam_role.github_deploy.name
  policy_arn = aws_iam_policy.github_deploy_tf.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "pipeline_execution_role_arn" {
  value = aws_iam_role.pipeline_execution.arn
}

output "github_deploy_role_arn" {
  value = aws_iam_role.github_deploy.arn
}
