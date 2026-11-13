# ─────────────────────────────────────────────────────────────────────────────
# Identities
#   c360-github-plan          PR plans (read-only, no data, no secrets)
#   c360-github-deploy-staging  apply staging (GitHub environment "staging")
#   c360-github-deploy-prod     apply prod (GitHub environment "production",
#                               which only allows the main branch + reviewer)
#   c360-operator             humans: run / redrive / inspect, MFA required
# ─────────────────────────────────────────────────────────────────────────────

module "deploy_staging" {
  source             = "./modules/deploy_role"
  environment        = "staging"
  account_id         = local.account_id
  region             = local.region
  oidc_provider_arn  = data.aws_iam_openid_connect_provider.github.arn
  github_repository  = var.github_repository
  github_environment = "staging"
  state_bucket       = var.staging_state_bucket
  lakehouse_bucket   = var.staging_lakehouse_bucket
  artifacts_bucket   = var.staging_lakehouse_bucket
  glue_database      = "customer360_iceberg_stage"
  athena_workgroup   = "customer360-staging"
  dashboard_name     = "Customer360-Staging"
  boundary_arn       = aws_iam_policy.workload_boundary.arn
}

module "deploy_prod" {
  source                  = "./modules/deploy_role"
  environment             = "prod"
  account_id              = local.account_id
  region                  = local.region
  oidc_provider_arn       = data.aws_iam_openid_connect_provider.github.arn
  github_repository       = var.github_repository
  github_environment      = "production"
  state_bucket            = var.prod_state_bucket
  lakehouse_bucket        = "c360-prod-lakehouse"
  artifacts_bucket        = "c360-prod-artifacts"
  glue_database           = "customer360_prod"
  athena_workgroup        = "customer360-prod"
  dashboard_name          = "Customer360-Prod"
  boundary_arn            = aws_iam_policy.workload_boundary.arn
  manages_buckets_and_kms = true
}

# ── Plan role (pull requests) ────────────────────────────────────────────────

resource "aws_iam_role" "github_plan" {
  name                 = "c360-github-plan"
  description          = "terraform plan on pull requests; read-only"
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "${local.repo_sub_base}:pull_request"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_plan" {
  name = "c360-github-plan"
  role = aws_iam_role.github_plan.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListStatePrefixes"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.staging_state_bucket}",
          "arn:aws:s3:::${var.prod_state_bucket}"
        ]
        Condition = { StringLike = { "s3:prefix" = ["customer360/*"] } }
      },
      {
        Sid    = "ReadState"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "arn:aws:s3:::${var.staging_state_bucket}/customer360/*",
          "arn:aws:s3:::${var.prod_state_bucket}/customer360/*"
        ]
      },
      {
        # plan takes the S3 lock; it may only write/delete the .tflock object
        Sid    = "LockFileOnly"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::${var.staging_state_bucket}/customer360/*.tflock",
          "arn:aws:s3:::${var.prod_state_bucket}/customer360/*.tflock"
        ]
      },
      {
        # HeadBucket for the lakehouse data sources (object keys only, never object bodies)
        Sid    = "HeadLakehouseBuckets"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.staging_lakehouse_bucket}",
          "arn:aws:s3:::c360-prod-*"
        ]
      },
      {
        # Describe-only. No s3:GetObject on data, no GetSecretValue, no queries.
        Sid    = "DescribeInfrastructure"
        Effect = "Allow"
        Action = [
          "s3:GetBucket*", "s3:GetEncryptionConfiguration", "s3:GetLifecycleConfiguration",
          "s3:GetAccelerateConfiguration", "s3:GetReplicationConfiguration", "s3:ListAllMyBuckets",
          "lambda:GetFunction", "lambda:GetFunctionConfiguration", "lambda:GetFunctionCodeSigningConfig",
          "lambda:ListVersionsByFunction", "lambda:GetPolicy", "lambda:ListTags",
          "states:DescribeStateMachine", "states:ListTagsForResource", "states:ValidateStateMachineDefinition",
          "glue:GetJob", "glue:GetDatabase", "glue:GetTags",
          "athena:GetWorkGroup", "athena:ListTagsForResource",
          "scheduler:GetSchedule", "scheduler:ListTagsForResource",
          "events:DescribeRule", "events:ListTargetsByRule", "events:ListTagsForResource",
          "sns:GetTopicAttributes", "sns:GetSubscriptionAttributes", "sns:ListSubscriptionsByTopic", "sns:ListTagsForResource",
          "cloudwatch:DescribeAlarms", "cloudwatch:ListTagsForResource", "cloudwatch:GetDashboard",
          "logs:DescribeLogGroups", "logs:ListTagsForResource", "logs:ListTagsLogGroup",
          "iam:GetRole", "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
          "iam:GetPolicy", "iam:GetPolicyVersion", "iam:GetOpenIDConnectProvider",
          "kms:DescribeKey", "kms:GetKeyPolicy", "kms:GetKeyRotationStatus", "kms:ListResourceTags", "kms:ListAliases",
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      }
    ]
  })
}

# ── Operator role (humans, MFA) ──────────────────────────────────────────────

resource "aws_iam_role" "operator" {
  name                 = "c360-operator"
  description          = "Day-to-day pipeline operations for Customer 360 (MFA required)"
  max_session_duration = 14400

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:user/${var.operator_user_name}" }
      Action    = "sts:AssumeRole"
      Condition = { Bool = { "aws:MultiFactorAuthPresent" = "true" } }
    }]
  })
}

resource "aws_iam_role_policy" "operator" {
  name = "c360-operator"
  role = aws_iam_role.operator.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunAndRecoverPipelines"
        Effect = "Allow"
        Action = [
          "states:StartExecution", "states:StopExecution", "states:RedriveExecution",
          "states:DescribeExecution", "states:DescribeStateMachine", "states:DescribeStateMachineForExecution",
          "states:GetExecutionHistory", "states:ListExecutions", "states:DescribeMapRun", "states:ListMapRuns"
        ]
        Resource = [
          "arn:aws:states:${local.region}:${local.account_id}:stateMachine:c360-*",
          "arn:aws:states:${local.region}:${local.account_id}:execution:c360-*:*",
          "arn:aws:states:${local.region}:${local.account_id}:mapRun:c360-*/*:*"
        ]
      },
      {
        Sid      = "ListStateMachines"
        Effect   = "Allow"
        Action   = ["states:ListStateMachines"]
        Resource = "*"
      },
      {
        Sid      = "ReadSchedules"
        Effect   = "Allow"
        Action   = ["scheduler:GetSchedule", "scheduler:ListSchedules"]
        Resource = "*"
      },
      {
        Sid    = "ReadLakehouse"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"]
        Resource = [
          "arn:aws:s3:::${var.staging_lakehouse_bucket}", "arn:aws:s3:::${var.staging_lakehouse_bucket}/*",
          "arn:aws:s3:::c360-prod-*", "arn:aws:s3:::c360-prod-*/*"
        ]
      },
      {
        Sid      = "WriteQueryResults"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.staging_lakehouse_bucket}/*athena-results/*", "arn:aws:s3:::c360-prod-lakehouse/athena-results/*"]
      },
      {
        Sid    = "QueryLakehouse"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:StopQueryExecution", "athena:GetWorkGroup", "athena:ListQueryExecutions"
        ]
        Resource = "arn:aws:athena:${local.region}:${local.account_id}:workgroup/customer360-*"
      },
      {
        Sid      = "ReadCatalog"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions", "glue:GetJobRun", "glue:GetJobRuns"]
        Resource = "*"
      },
      {
        Sid    = "ReadLogsAndMetrics"
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups", "logs:DescribeLogStreams", "logs:GetLogEvents", "logs:FilterLogEvents",
          "logs:StartQuery", "logs:GetQueryResults",
          "cloudwatch:GetMetricData", "cloudwatch:ListMetrics", "cloudwatch:DescribeAlarms", "cloudwatch:GetDashboard"
        ]
        Resource = "*"
      },
      {
        Sid      = "TestAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish", "sns:ListSubscriptionsByTopic", "sns:GetTopicAttributes"]
        Resource = "arn:aws:sns:${local.region}:${local.account_id}:c360-*"
      },
      {
        Sid       = "KmsDecryptProdData"
        Effect    = "Allow"
        Action    = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "Customer360" } }
      },
      {
        Sid      = "NoSecretsNoIam"
        Effect   = "Deny"
        Action   = ["secretsmanager:GetSecretValue", "iam:*"]
        Resource = "*"
      }
    ]
  })
}

# The only thing user C360 needs once its old broad policies are detached.
resource "aws_iam_policy" "operator_assume" {
  name        = "c360-operator-assume"
  description = "Lets the operator user assume c360-operator (MFA enforced by the role trust)"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.operator.arn
      },
      {
        Sid      = "ManageOwnMfa"
        Effect   = "Allow"
        Action   = ["iam:ListMFADevices", "iam:CreateVirtualMFADevice", "iam:EnableMFADevice", "iam:ResyncMFADevice", "iam:GetUser"]
        Resource = ["arn:aws:iam::${local.account_id}:user/$${aws:username}", "arn:aws:iam::${local.account_id}:mfa/*"]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "operator_assume" {
  count      = var.attach_operator_assume_policy ? 1 : 0
  user       = var.operator_user_name
  policy_arn = aws_iam_policy.operator_assume.arn
}
