# IAM permissions matrix

Superseded. The `terraform/modules/iam` module this file described was removed
in `fix/production-readiness`. Current identities, their policies and how to
verify them: [PRODUCTION_PERMISSIONS_GUIDE.md](PRODUCTION_PERMISSIONS_GUIDE.md).

| Role | Created by | Trust |
|---|---|---|
| `c360-github-plan` | `terraform/bootstrap` | GitHub OIDC, pull requests |
| `c360-github-deploy-staging` / `-prod` | `terraform/bootstrap/modules/deploy_role` | GitHub OIDC, environment `staging` / `production` |
| `c360-operator` | `terraform/bootstrap` | user `C360` with MFA |
| `c360-<env>-lambda-<function>` | `terraform/modules/lambda` | lambda.amazonaws.com, boundary |
| `c360-<env>-step-functions-execution` | `terraform/modules/step_functions` | states.amazonaws.com, boundary |
| `c360-<env>-glue-execution` | `terraform/modules/glue_jobs` | glue.amazonaws.com, boundary |
| `c360-<env>-scheduler-execution` | `terraform/modules/eventbridge` | scheduler.amazonaws.com, boundary |
