# ─────────────────────────────────────────────────────────────────────────────
# Terraform state buckets
# ─────────────────────────────────────────────────────────────────────────────

# Existing bucket: adopt it so its safety settings are enforced as code.
import {
  to = aws_s3_bucket.state["staging"]
  id = var.staging_state_bucket
}

locals {
  state_buckets = {
    staging = var.staging_state_bucket
    prod    = var.prod_state_bucket
  }
}

resource "aws_s3_bucket" "state" {
  for_each = local.state_buckets
  bucket   = each.value

  lifecycle {
    prevent_destroy = true
  }

  tags = { Name = each.value, Environment = each.key }
}

resource "aws_s3_bucket_versioning" "state" {
  for_each = local.state_buckets
  bucket   = aws_s3_bucket.state[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  for_each = local.state_buckets
  bucket   = aws_s3_bucket.state[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  for_each                = local.state_buckets
  bucket                  = aws_s3_bucket.state[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "state" {
  for_each = local.state_buckets
  bucket   = aws_s3_bucket.state[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Keep 90 days of old state versions for recovery, then expire them.
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  for_each = local.state_buckets
  bucket   = aws_s3_bucket.state[each.key].id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_policy" "state" {
  for_each = local.state_buckets
  bucket   = aws_s3_bucket.state[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.state[each.key].arn, "${aws_s3_bucket.state[each.key].arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}
