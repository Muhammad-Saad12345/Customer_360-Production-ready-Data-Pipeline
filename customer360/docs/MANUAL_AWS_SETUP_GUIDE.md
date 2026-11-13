# Manual AWS setup guide

Everything in this file is done by a person in the AWS account. Nothing here
was run from this repository or by CI. Each step lists the Console path, the CLI
commands, how to check the step worked, and the errors you are likely to hit.

Run the CLI steps in **AWS CloudShell** (Console, top bar, the `>_` icon) in
`us-east-1`. CloudShell uses the identity you signed in with, so no access keys
are created or typed anywhere.

Order: **M1 → M2 → M3 → M4 → M5 → M6 → M7 → M8**, then the runbook
(`PRODUCTION_DEPLOYMENT_RUNBOOK.md`). M9–M11 retire the old credentials once
the new path works.

| Step | What | Who signs in | Destructive? |
|---|---|---|---|
| M1 | Secure the root user, create `c360-admin` | root (last time) | No |
| M2 | Pre-flight checks | c360-admin | No |
| M3 | Apply `terraform/bootstrap` | c360-admin | No (imports the staging state bucket) |
| M4 | Configure GitHub environments and variables | GitHub owner | No |
| M5 | Review the first staging plan | c360-admin | **Review destroys** |
| M6 | First staging deploy (CI) | GitHub | Only what M5 approved |
| M7 | Confirm the alert email subscription and test it | mailbox + c360-admin | No |
| M8 | Source secrets (only for sources that go to `api` mode) | c360-admin | No |
| M9 | MFA + profile for the operator user `C360` | C360 | No |
| M10 | Detach the operator user's old broad policies, deactivate its keys | c360-admin | **Yes** (reversible for 7 days) |
| M11 | Retire `GitHubActionsStagingRole` | c360-admin | **Yes** |

```bash
# set once per CloudShell session
export AWS_REGION=us-east-1
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export REPO=Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline
```

---

## M1. Secure root, create the admin identity

**Why:** the account is currently operated as root. Root cannot be limited by
IAM policy and its loss is unrecoverable, so it should only be used for the
few tasks that need it. Identity Center would be the first choice, but it
needs an AWS Organizations management account; a single IAM user with MFA and
no access keys is the simplest safe option for a one-person account.

Console (signed in as root):

1. Account menu → **Security credentials**.
   * **Multi-factor authentication** → *Assign MFA device* if none is listed.
   * **Access keys** → if any root key exists, *Deactivate* it now and *Delete*
     it after M6 works. (Check first: `aws iam get-account-summary --query 'SummaryMap.AccountAccessKeysPresent'` → `0` means none.)
2. IAM → Users → **Create user** `c360-admin`
   * *Provide user access to the AWS Management Console* → custom password,
     *must create a new password at next sign-in*.
   * Permissions → *Attach policies directly* → `AdministratorAccess`.
3. Sign out. Sign in as `c360-admin` (account ID + user name), set the new
   password, then IAM → Users → `c360-admin` → *Security credentials* →
   *Assign MFA device*. Sign out and back in so the session has MFA.
4. Do **not** create access keys for `c360-admin`; use CloudShell.

Verify (as c360-admin in CloudShell):

```bash
aws sts get-caller-identity --query Arn --output text      # ...:user/c360-admin
aws iam list-mfa-devices --user-name c360-admin --query 'MFADevices[].SerialNumber'
aws iam list-access-keys --user-name c360-admin --query 'AccessKeyMetadata'   # []
```

From here on root is not used.

---

## M2. Pre-flight checks

```bash
# 1. GitHub OIDC provider (bootstrap reuses it, it does not create one)
aws iam list-open-id-connect-providers
#    expect: arn:aws:iam::<acct>:oidc-provider/token.actions.githubusercontent.com

# 2. Staging state bucket: exists, versioned, current bucket policy (bootstrap replaces it)
aws s3api get-bucket-versioning --bucket c360-staging-tfstate
aws s3api get-bucket-policy --bucket c360-staging-tfstate --query Policy --output text 2>&1 | head -5
aws s3 ls s3://c360-staging-tfstate/ --recursive | head

# 3. Prod bucket names are free (S3 names are global)
for b in c360-prod-tfstate c360-prod-lakehouse c360-prod-artifacts; do
  aws s3api head-bucket --bucket "$b" 2>&1 | grep -q 'Not Found' && echo "$b free" || echo "$b TAKEN or yours: pick another name"
done

# 4. Nothing else is called c360-workload-boundary or c360-github-*
aws iam list-policies --scope Local --query "Policies[?starts_with(PolicyName,'c360-')].PolicyName"
aws iam list-roles --query "Roles[?starts_with(RoleName,'c360-')].RoleName"
```

| Result | Action |
|---|---|
| no OIDC provider | IAM → Identity providers → Add → OpenID Connect, URL `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com` |
| bucket policy already has statements you need | copy them into `terraform/bootstrap/state_buckets.tf` before M3 |
| a prod name is taken | change `prod_state_bucket` (bootstrap) and `project_name`/bucket names in `terraform/modules/core` before creating prod |

---

## M3. Apply the bootstrap stack

**What it creates:** prod state bucket (and adopts the staging one),
`c360-workload-boundary`, `c360-github-plan`, `c360-github-deploy-staging`,
`c360-github-deploy-prod`, `c360-operator`, `c360-operator-assume`.
Nothing in the data pipeline changes.

```bash
# Terraform in CloudShell (x86_64)
mkdir -p ~/bin && cd /tmp
curl -fsSLo tf.zip https://releases.hashicorp.com/terraform/1.13.4/terraform_1.13.4_linux_amd64.zip
unzip -o tf.zip -d ~/bin && export PATH=$HOME/bin:$PATH && terraform version

cd ~ && git clone https://github.com/$REPO.git c360 && cd c360
git checkout fix/production-readiness          # or main once the PR is merged
cd customer360/terraform/bootstrap

terraform init
terraform plan -out=bootstrap.plan
```

Read the plan before applying. Expected: **1 import** (`aws_s3_bucket.state["staging"]`),
about 25 to add, **0 to destroy**. If anything is marked for destroy or replace, stop.

```bash
terraform apply bootstrap.plan
terraform output
```

Verify:

```bash
aws iam get-role --role-name c360-github-deploy-staging \
  --query 'Role.AssumeRolePolicyDocument.Statement[0].Condition'
#  sub must be repo:<owner>/<repo>:environment:staging
aws iam get-policy --policy-arn arn:aws:iam::$ACCOUNT_ID:policy/c360-workload-boundary --query Policy.DefaultVersionId
aws s3api get-bucket-versioning --bucket c360-prod-tfstate      # Enabled
```

Errors:

| Error | Cause / fix |
|---|---|
| `BucketAlreadyExists` for c360-prod-tfstate | name taken globally: `terraform apply -var prod_state_bucket=<new-name>` and use the same name in `environments/prod/main.tf` backend |
| `EntityAlreadyExists` for a role | an older role has the same name: rename or delete it (check `RoleLastUsed` first) |
| `no matching IAM OpenID Connect provider` | do the M2 fix |
| `Error acquiring the state lock` | someone else is running bootstrap; wait, never `force-unlock` a lock you did not create |

---

## M4. GitHub configuration

Repository → **Settings → Environments**:

* `staging` (no protection rules needed).
* `production` → *Required reviewers*: you; *Deployment branches and tags*:
  **Selected branches** → `main`. (Required reviewers on a **private** repository
  need a paid GitHub plan; on a free private repo the branch rule still applies
  but approval does not.)

Repository → **Settings → Secrets and variables → Actions → Variables**:

| Scope | Name | Value |
|---|---|---|
| Repository | `AWS_REGION` | `us-east-1` |
| Repository | `AWS_PLAN_ROLE_ARN` | `terraform output -raw plan_role_arn` |
| Repository | `ALERT_EMAIL` | `datawithsaad97531786@gmail.com` |
| Environment `staging` | `AWS_DEPLOY_ROLE_ARN` | `terraform output -raw staging_deploy_role_arn` |
| Environment `production` | `AWS_DEPLOY_ROLE_ARN` | `terraform output -raw prod_deploy_role_arn` |
| Environment (optional) | `SCHEDULE_ENABLED` | leave unset until the runbook says `true` |
| Environment (optional) | `ENABLED_SOURCES` | e.g. `["hubspot","oms","pim"]` |

No AWS secret or key goes into GitHub. Same with the CLI:

```bash
gh variable set AWS_REGION --body us-east-1 -R $REPO
gh variable set AWS_PLAN_ROLE_ARN --body "$(terraform output -raw plan_role_arn)" -R $REPO
gh variable set ALERT_EMAIL --body datawithsaad97531786@gmail.com -R $REPO
gh api -X PUT repos/$REPO/environments/staging
gh variable set AWS_DEPLOY_ROLE_ARN --env staging --body "$(terraform output -raw staging_deploy_role_arn)" -R $REPO
gh api -X PUT repos/$REPO/environments/production \
  -F "reviewers[][type]=User" -F "reviewers[][id]=$(gh api users/${REPO%%/*} -q .id)" \
  -F "deployment_branch_policy[protected_branches]=false" -F "deployment_branch_policy[custom_branch_policies]=true"
gh api -X POST repos/$REPO/environments/production/deployment-branch-policies -f name=main -f type=branch
gh variable set AWS_DEPLOY_ROLE_ARN --env production --body "$(terraform output -raw prod_deploy_role_arn)" -R $REPO
```

Recommended: Settings → Branches → protect `main` (require a pull request and
the `Lint, tests, ASL, dbt parse, Terraform validate` check).

---

## M5. Review the first staging plan (destructive-change gate)

The new code removes the `iam`, `mwaa` and `observability` modules and gives
every Lambda its own role. If the current staging state contains resources
from those modules, the first plan will **destroy** them. Look before CI does it.

```bash
cd ~/c360/customer360/terraform/environments/staging
terraform init
terraform state list | sort > /tmp/state-before.txt
grep -E 'module\.(iam|mwaa|observability)' /tmp/state-before.txt || echo "none of the removed modules are in state"
terraform plan -var "artifact_version=planonly0001" -var 'alert_email_addresses=["datawithsaad97531786@gmail.com"]' -no-color > /tmp/staging.plan.txt
grep -E '(will be destroyed|must be replaced)' /tmp/staging.plan.txt
```

Decide for every destroy line:

* old per-environment deploy role, old shared Lambda role, old observability
  alarms → expected, let it go;
* an MWAA environment, an S3 bucket or anything holding data → **stop**. Either
  remove it from state so Terraform forgets it without deleting it
  (`terraform state rm '<address>'`) or keep the module. Ask before deleting.

`planonly0001` does not exist in S3; that is fine for a plan.

---

## M6. First staging deploy

Merge the PR (or run the workflow manually: Actions → *Customer 360 CI/CD* →
*Run workflow* → `staging`). The `deploy-staging` job uploads the artifacts,
applies Terraform with the reviewed plan shape and starts a **dry-run**
execution. It fails the job if the dry run does not succeed.

Verify:

```bash
aws stepfunctions list-state-machines --query "stateMachines[?starts_with(name,'c360-staging')].name"
aws lambda list-functions --query "Functions[?starts_with(FunctionName,'c360-staging')].[FunctionName,Role]" --output table
aws iam get-role --role-name c360-staging-lambda-ingest --query Role.PermissionsBoundary
aws scheduler get-schedule --name c360-staging-daily-batch --query State      # DISABLED
```

---

## M7. Alert email

After M6, AWS sends *AWS Notification - Subscription Confirmation* to
`datawithsaad97531786@gmail.com`. Click **Confirm subscription** (check spam).

```bash
TOPIC=$(aws sns list-topics --query "Topics[?contains(TopicArn,'c360-staging-pipeline-alerts')].TopicArn" --output text)
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC" --query 'Subscriptions[].[Endpoint,SubscriptionArn]' --output table
#  SubscriptionArn must be an ARN, not PendingConfirmation
aws sns publish --topic-arn "$TOPIC" --subject "Customer360 staging alert test" --message "Test from M7"
```

Notifications are working only when that test message arrives in the inbox.
Then prove the real path: start an execution that fails on purpose and check the
EventBridge rule emails you:

```bash
SM=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='c360-staging-batch-orchestrator'].stateMachineArn" --output text)
aws stepfunctions start-execution --state-machine-arn "$SM" --input '{"sources":["not-a-source"]}'
# InitializePipeline fails -> execution FAILED -> email "... FAILED" within ~1 minute
```

---

## M8. Source secrets (only when a source moves to `api`)

Staging starts with every source in explicit `synthetic` mode, so no secret is
needed for the first runs. When you switch a source to `api`, create its secret
first. One JSON secret per source and environment:

| Secret | Keys |
|---|---|
| `customer360/<env>/hubspot` | `access_token` (HubSpot private app, scope `crm.objects.contacts.read`) |
| `customer360/<env>/oms` | `shop_domain`, `client_id`, `client_secret` (or `access_token`), `api_version` — see `OMS_PIM_API_INTEGRATION_GUIDE.md` |
| `customer360/<env>/pim` | same as oms (same Shopify app) |
| `customer360/<env>/marketing` | `api_key` (`<key>-<dc>`), `audience_id` |
| `customer360/<env>/support` | `subdomain`, `email`, `api_token` |
| `customer360/<env>/loyalty` | `base_url`, `api_token` |

```bash
# write the JSON with an editor so the values never appear in shell history
nano /tmp/oms.json
aws secretsmanager create-secret --name customer360/staging/oms --secret-string file:///tmp/oms.json
shred -u /tmp/oms.json
aws secretsmanager describe-secret --secret-id customer360/staging/oms --query '{name:Name,changed:LastChangedDate}'
```

Then change `source_modes` for that source to `"api"` in
`terraform/environments/staging/main.tf` through a pull request. Only the
`ingest` Lambda role can read `customer360/<env>/*`.

---

## M9. Operator user `C360`: MFA and the assume-role profile

**Why:** `C360` currently has broad attached policies and long-lived access
keys. After this step it can only assume `c360-operator`, and only with MFA.

1. Console as `C360` (or as c360-admin on its behalf): IAM → Users → `C360` →
   *Security credentials* → *Assign MFA device*. Note the device ARN.
2. As c360-admin, attach the assume policy through Terraform:
   ```bash
   cd ~/c360/customer360/terraform/bootstrap
   terraform apply -var attach_operator_assume_policy=true
   ```
3. On the laptop where `C360`'s keys are configured, add to `~/.aws/config`:
   ```ini
   [profile c360-operator]
   role_arn = arn:aws:iam::<ACCOUNT_ID>:role/c360-operator
   source_profile = default
   mfa_serial = <MFA device ARN from step 1>
   region = us-east-1
   ```
4. Test: `aws sts get-caller-identity --profile c360-operator` (asks for the MFA
   code) and `aws stepfunctions list-executions --state-machine-arn "$SM" --max-results 5 --profile c360-operator`.

---

## M10. Remove the operator user's old broad access (after M9 works)

```bash
aws iam list-attached-user-policies --user-name C360
aws iam list-user-policies --user-name C360
aws iam list-groups-for-user --user-name C360
for k in $(aws iam list-access-keys --user-name C360 --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
  aws iam get-access-key-last-used --access-key-id "$k" --query '[AccessKeyLastUsed.LastUsedDate,AccessKeyLastUsed.ServiceName]' --output text
done
```

* Detach every policy except `c360-operator-assume`
  (`aws iam detach-user-policy --user-name C360 --policy-arn <arn>`); remove
  inline policies and groups the same way.
* Keep one access key for the profile above. Any other key: `update-access-key
  --status Inactive`, wait 7 days for complaints, then `delete-access-key`.
* Rollback: re-attach the detached ARNs (keep the list from the first command).

## M11. Retire `GitHubActionsStagingRole`

Used by `.github/workflows/test-aws-oidc.yml` only. After a green `deploy-staging` run:

```bash
aws iam get-role --role-name GitHubActionsStagingRole --query 'Role.RoleLastUsed'
aws iam list-attached-role-policies --role-name GitHubActionsStagingRole
aws iam list-role-policies --role-name GitHubActionsStagingRole
```

If it was not used since the new pipeline took over: detach/delete its
policies, `aws iam delete-role --role-name GitHubActionsStagingRole`, and delete
`test-aws-oidc.yml` (or point it at `vars.AWS_PLAN_ROLE_ARN` from a pull request).

---

## Lake Formation

Leave it as it is. The account is in IAM-only mode (`IAM_ALLOWED_PRINCIPALS`
has `ALL` on new databases and tables), so the IAM policies above are what
grant access. The Glue databases are created with that default. Do not
remove data lake administrators or switch to Lake Formation permissions
without first granting every role in this repo explicitly; doing it in the
other order breaks every Athena query.
