# ─────────────────────────────────────────────────────────────────────────────
# c360-workload-boundary
#
# Every role created by the deploy roles MUST carry this boundary (enforced by
# the iam:PermissionsBoundary condition in the deploy-role policy). A role's
# effective permissions are the intersection of its own policy and this
# boundary, so a mistaken or malicious role policy cannot exceed it.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_policy" "workload_boundary" {
  name        = local.boundary_name
  description = "Maximum permissions for Customer 360 runtime roles (Lambda, Step Functions, Glue, Scheduler)"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LakehouseData"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
          "s3:GetBucketLocation", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "arn:aws:s3:::${var.staging_lakehouse_bucket}", "arn:aws:s3:::${var.staging_lakehouse_bucket}/*",
          "arn:aws:s3:::c360-prod-*", "arn:aws:s3:::c360-prod-*/*"
        ]
      },
      {
        Sid    = "Catalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
          "glue:UpdateTable", "glue:DeleteTable", "glue:GetTableVersions", "glue:DeleteTableVersion",
          "glue:BatchDeleteTableVersion", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
          "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/customer360_*",
          "arn:aws:glue:${local.region}:${local.account_id}:table/customer360_*/*"
        ]
      },
      {
        Sid    = "Athena"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults", "athena:GetWorkGroup", "athena:ListQueryExecutions"
        ]
        Resource = "arn:aws:athena:${local.region}:${local.account_id}:workgroup/customer360-*"
      },
      {
        Sid      = "AthenaDefaultCatalog"
        Effect   = "Allow"
        Action   = ["athena:GetDataCatalog"]
        Resource = "arn:aws:athena:${local.region}:${local.account_id}:datacatalog/AwsDataCatalog"
      },
      {
        Sid    = "Orchestration"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction", "states:StartExecution", "states:ListExecutions",
          "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:lambda:${local.region}:${local.account_id}:function:c360-*",
          "arn:aws:states:${local.region}:${local.account_id}:stateMachine:c360-*",
          "arn:aws:glue:${local.region}:${local.account_id}:job/c360-*"
        ]
      },
      {
        Sid    = "SourceSecrets"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [
          "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:customer360/*",
          "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:c_360_secret_manager*"
        ]
      },
      {
        Sid      = "Alerts"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = "arn:aws:sns:${local.region}:${local.account_id}:c360-*"
      },
      {
        Sid       = "ProjectKmsKeysOnly"
        Effect    = "Allow"
        Action    = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "Customer360" } }
      },
      {
        Sid    = "OwnLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
        Resource = [
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/c360-*",
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/vendedlogs/states/c360-*",
          "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"
        ]
      },
      {
        # These APIs have no resource-level permissions. Each role narrows them
        # further (e.g. cloudwatch:namespace = Customer360/Pipeline).
        Sid    = "NoResourceLevelSupport"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
          "xray:PutTraceSegments", "xray:PutTelemetryRecords"
        ]
        Resource = "*"
      },
      {
        Sid      = "NeverIdentityOrAccount"
        Effect   = "Deny"
        Action   = ["iam:*", "sts:*", "organizations:*", "account:*"]
        Resource = "*"
      }
    ]
  })
}
