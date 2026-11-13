# ─────────────────────────────────────────────────────────────────────────────
# Customer 360 — Bootstrap (apply ONCE, by a human admin: c360-admin)
#
# Creates what CI cannot create for itself:
#   * Terraform state buckets (staging bucket adopted, prod bucket created)
#   * c360-workload-boundary: the ceiling for every role CI creates
#   * GitHub OIDC roles: plan (PRs), deploy-staging, deploy-prod
#   * c360-operator: daily human role (MFA required) + assume-only policy
#
# State lives in s3://c360-staging-tfstate/bootstrap/terraform.tfstate. The
# staging deploy role can only touch customer360/staging/* in that bucket, so
# CI can never read or change this state.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket       = "c360-staging-tfstate"
    key          = "bootstrap/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project   = "Customer360"
      ManagedBy = "Terraform-bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id    = data.aws_caller_identity.current.account_id
  region        = var.aws_region
  repo_sub_base = "repo:${var.github_repository}"
  boundary_name = "c360-workload-boundary"
  boundary_arn  = "arn:aws:iam::${local.account_id}:policy/${local.boundary_name}"
}

# Reuse the existing GitHub OIDC provider (created 6 Oct 2026, verified).
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}
