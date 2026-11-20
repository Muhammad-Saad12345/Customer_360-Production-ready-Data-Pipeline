variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "github_repository" {
  description = "owner/repo exactly as GitHub reports it (case-sensitive in the OIDC sub claim)"
  type        = string
  default     = "Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline"
}

# Repositories created after 15 Jul 2026 (this one: 5 Oct 2026) get GitHub's
# immutable OIDC subject: repo:<owner>@<owner_id>/<repo>@<repo_id>:...
# IDs: gh api repos/<owner>/<repo> -q '.owner.id, .id'
variable "github_owner_id" {
  description = "Numeric GitHub owner ID (immutable)"
  type        = string
  default     = "179042869"
}

variable "github_repository_id" {
  description = "Numeric GitHub repository ID (immutable)"
  type        = string
  default     = "1405365304"
}

variable "use_immutable_oidc_subject" {
  description = "true when the repository issues immutable subject claims (default for repos created after 15 Jul 2026)"
  type        = bool
  default     = true
}

variable "staging_state_bucket" {
  type    = string
  default = "c360-staging-tfstate"
}

variable "prod_state_bucket" {
  type    = string
  default = "c360-prod-tfstate"
}

variable "staging_lakehouse_bucket" {
  description = "Existing staging lakehouse/artifacts bucket"
  type        = string
  default     = "c360-bucket786"
}

variable "operator_user_name" {
  description = "IAM user allowed to assume c360-operator (with MFA)"
  type        = string
  default     = "C360"
}

variable "attach_operator_assume_policy" {
  description = "Attach c360-operator-assume to the operator user. Leave false until the MFA profile (M9) works; detaching the old broad policies is manual (M10)."
  type        = bool
  default     = false
}
