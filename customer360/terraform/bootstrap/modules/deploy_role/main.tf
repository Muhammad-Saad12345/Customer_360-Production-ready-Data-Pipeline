# ─────────────────────────────────────────────────────────────────────────────
# GitHub OIDC deploy role for ONE environment (staging or prod).
# Can only touch c360-<env>-* resources, its own state prefix and its own
# artifacts; can only create roles that carry the workload boundary.
# ─────────────────────────────────────────────────────────────────────────────

variable "environment" { type = string }
variable "account_id" { type = string }
variable "region" { type = string }
variable "oidc_provider_arn" { type = string }
variable "github_repository" { type = string }
variable "github_environment" { type = string }
variable "state_bucket" { type = string }
variable "lakehouse_bucket" { type = string }
variable "artifacts_bucket" { type = string }
variable "glue_database" { type = string }
variable "athena_workgroup" { type = string }
variable "dashboard_name" { type = string }
variable "boundary_arn" { type = string }
variable "manages_buckets_and_kms" {
  description = "Prod creates its own lakehouse/artifacts buckets and KMS key"
  type        = bool
  default     = false
}

locals {
  p   = "c360-${var.environment}"
  a   = var.account_id
  r   = var.region
  arn = "arn:aws:iam::${var.account_id}"

  bucket_read_actions = [
    "s3:GetBucketLocation", "s3:GetBucketPolicy", "s3:GetBucketPolicyStatus", "s3:GetBucketAcl", "s3:GetBucketCORS",
    "s3:GetBucketWebsite", "s3:GetBucketVersioning", "s3:GetBucketLogging", "s3:GetBucketTagging",
    "s3:GetBucketRequestPayment", "s3:GetBucketObjectLockConfiguration", "s3:GetBucketPublicAccessBlock",
    "s3:GetBucketOwnershipControls", "s3:GetEncryptionConfiguration", "s3:GetLifecycleConfiguration",
    "s3:GetAccelerateConfiguration", "s3:GetReplicationConfiguration", "s3:ListBucket"
  ]

  statements = concat(
    [
      {
        Sid       = "StateList"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = "arn:aws:s3:::${var.state_bucket}"
        Condition = { StringLike = { "s3:prefix" = ["customer360/${var.environment}/*"] } }
      },
      {
        Sid      = "StateReadWriteLock"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${var.state_bucket}/customer360/${var.environment}/*"
      },
      {
        Sid      = "LakehouseBucketRead"
        Effect   = "Allow"
        Action   = local.bucket_read_actions
        Resource = distinct(["arn:aws:s3:::${var.lakehouse_bucket}", "arn:aws:s3:::${var.artifacts_bucket}"])
      },
      {
        Sid    = "UploadArtifactsAndReadQueryResults"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject"]
        Resource = [
          "arn:aws:s3:::${var.artifacts_bucket}/artifacts/*",
          "arn:aws:s3:::${var.lakehouse_bucket}/athena-results/*"
        ]
      },
      {
        Sid      = "LambdaFunctions"
        Effect   = "Allow"
        Action   = ["lambda:*"]
        Resource = ["arn:aws:lambda:${local.r}:${local.a}:function:${local.p}-*"]
      },
      {
        Sid    = "StateMachines"
        Effect = "Allow"
        Action = ["states:*"]
        Resource = [
          "arn:aws:states:${local.r}:${local.a}:stateMachine:${local.p}-*",
          "arn:aws:states:${local.r}:${local.a}:execution:${local.p}-*:*"
        ]
      },
      {
        Sid      = "StatesNoResourceLevel"
        Effect   = "Allow"
        Action   = ["states:ValidateStateMachineDefinition", "states:ListStateMachines"]
        Resource = "*"
      },
      {
        Sid    = "GlueJobAndDatabase"
        Effect = "Allow"
        Action = [
          "glue:CreateJob", "glue:UpdateJob", "glue:DeleteJob", "glue:GetJob", "glue:GetJobRun", "glue:GetJobRuns",
          "glue:GetTags", "glue:TagResource", "glue:UntagResource",
          "glue:GetDatabase", "glue:CreateDatabase", "glue:UpdateDatabase", "glue:DeleteDatabase",
          "glue:GetTable", "glue:GetTables", "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:${local.r}:${local.a}:job/${local.p}-*",
          "arn:aws:glue:${local.r}:${local.a}:catalog",
          "arn:aws:glue:${local.r}:${local.a}:database/${var.glue_database}",
          "arn:aws:glue:${local.r}:${local.a}:table/${var.glue_database}/*"
        ]
      },
      {
        Sid    = "AthenaWorkgroupAndChecks"
        Effect = "Allow"
        Action = [
          "athena:CreateWorkGroup", "athena:UpdateWorkGroup", "athena:GetWorkGroup", "athena:DeleteWorkGroup",
          "athena:TagResource", "athena:UntagResource", "athena:ListTagsForResource",
          "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"
        ]
        Resource = "arn:aws:athena:${local.r}:${local.a}:workgroup/${var.athena_workgroup}"
      },
      {
        Sid      = "Schedules"
        Effect   = "Allow"
        Action   = ["scheduler:*"]
        Resource = "arn:aws:scheduler:${local.r}:${local.a}:schedule/default/${local.p}-*"
      },
      {
        Sid    = "EventRules"
        Effect = "Allow"
        Action = [
          "events:PutRule", "events:DeleteRule", "events:DescribeRule", "events:EnableRule", "events:DisableRule",
          "events:PutTargets", "events:RemoveTargets", "events:ListTargetsByRule",
          "events:TagResource", "events:UntagResource", "events:ListTagsForResource"
        ]
        Resource = "arn:aws:events:${local.r}:${local.a}:rule/${local.p}-*"
      },
      {
        Sid      = "Topics"
        Effect   = "Allow"
        Action   = ["sns:*"]
        Resource = "arn:aws:sns:${local.r}:${local.a}:${local.p}-*"
      },
      {
        Sid    = "AlarmsAndDashboard"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:DescribeAlarms",
          "cloudwatch:TagResource", "cloudwatch:UntagResource", "cloudwatch:ListTagsForResource",
          "cloudwatch:PutDashboard", "cloudwatch:GetDashboard", "cloudwatch:DeleteDashboards"
        ]
        Resource = [
          "arn:aws:cloudwatch:${local.r}:${local.a}:alarm:${local.p}-*",
          "arn:aws:cloudwatch::${local.a}:dashboard/${var.dashboard_name}"
        ]
      },
      {
        Sid    = "LogGroups"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:DeleteLogGroup", "logs:PutRetentionPolicy", "logs:DeleteRetentionPolicy",
          "logs:TagResource", "logs:UntagResource", "logs:ListTagsForResource", "logs:TagLogGroup", "logs:ListTagsLogGroup",
          "logs:FilterLogEvents", "logs:GetLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${local.r}:${local.a}:log-group:/aws/lambda/${local.p}-*",
          "arn:aws:logs:${local.r}:${local.a}:log-group:/aws/vendedlogs/states/${local.p}-*"
        ]
      },
      {
        Sid      = "LogsNoResourceLevel"
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        # Roles may only be created or given policies if they carry the boundary.
        Sid    = "CreateRolesOnlyWithBoundary"
        Effect = "Allow"
        Action = [
          "iam:CreateRole", "iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:AttachRolePolicy",
          "iam:DetachRolePolicy", "iam:PutRolePermissionsBoundary"
        ]
        Resource  = "${local.arn}:role/${local.p}-*"
        Condition = { StringEquals = { "iam:PermissionsBoundary" = var.boundary_arn } }
      },
      {
        Sid    = "ManageOwnRoles"
        Effect = "Allow"
        Action = [
          "iam:GetRole", "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole", "iam:ListRoleTags", "iam:TagRole", "iam:UntagRole",
          "iam:UpdateRole", "iam:UpdateRoleDescription", "iam:UpdateAssumeRolePolicy", "iam:DeleteRole"
        ]
        Resource = "${local.arn}:role/${local.p}-*"
      },
      {
        Sid       = "PassOwnRolesToPipelineServices"
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = "${local.arn}:role/${local.p}-*"
        Condition = { StringEquals = { "iam:PassedToService" = ["lambda.amazonaws.com", "states.amazonaws.com", "glue.amazonaws.com", "scheduler.amazonaws.com"] } }
      },
      {
        Sid    = "ProtectBoundary"
        Effect = "Deny"
        Action = [
          "iam:DeleteRolePermissionsBoundary", "iam:CreatePolicyVersion", "iam:DeletePolicy",
          "iam:DeletePolicyVersion", "iam:SetDefaultPolicyVersion"
        ]
        Resource = ["${local.arn}:role/*", var.boundary_arn]
      }
    ],
    [for s in [
      {
        Sid    = "ProdBuckets"
        Effect = "Allow"
        Action = concat(local.bucket_read_actions, [
          "s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketPolicy", "s3:DeleteBucketPolicy", "s3:PutBucketVersioning",
          "s3:PutBucketTagging", "s3:PutBucketPublicAccessBlock", "s3:PutBucketOwnershipControls",
          "s3:PutEncryptionConfiguration", "s3:PutLifecycleConfiguration"
        ])
        Resource = ["arn:aws:s3:::${local.p}-*"]
      },
      {
        Sid       = "CreateTaggedKmsKey"
        Effect    = "Allow"
        Action    = ["kms:CreateKey", "kms:TagResource"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestTag/Project" = "Customer360", "aws:RequestTag/Environment" = var.environment } }
      },
      {
        Sid    = "ManageProjectKmsKey"
        Effect = "Allow"
        Action = [
          "kms:DescribeKey", "kms:GetKeyPolicy", "kms:GetKeyRotationStatus", "kms:ListResourceTags",
          "kms:EnableKeyRotation", "kms:PutKeyPolicy", "kms:TagResource", "kms:UntagResource",
          "kms:UpdateKeyDescription", "kms:ScheduleKeyDeletion", "kms:CreateAlias", "kms:DeleteAlias", "kms:UpdateAlias",
          "kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"
        ]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "Customer360", "aws:ResourceTag/Environment" = var.environment } }
      },
      {
        Sid      = "KmsAliases"
        Effect   = "Allow"
        Action   = ["kms:CreateAlias", "kms:DeleteAlias", "kms:UpdateAlias"]
        Resource = "arn:aws:kms:${local.r}:${local.a}:alias/${local.p}-*"
      },
      {
        Sid      = "KmsList"
        Effect   = "Allow"
        Action   = ["kms:ListAliases"]
        Resource = "*"
      }
    ] : s if(var.manages_buckets_and_kms)]
  )
}

resource "aws_iam_role" "this" {
  name                 = "c360-github-deploy-${var.environment}"
  description          = "GitHub Actions deploy role for Customer 360 ${var.environment} (environment-pinned OIDC)"
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:environment:${var.github_environment}"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "this" {
  name   = "c360-github-deploy-${var.environment}"
  role   = aws_iam_role.this.id
  policy = jsonencode({ Version = "2012-10-17", Statement = local.statements })
}

output "role_arn" {
  value = aws_iam_role.this.arn
}
