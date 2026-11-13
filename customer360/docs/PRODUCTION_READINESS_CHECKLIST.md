# Production readiness checklist

Branch `fix/production-readiness`. Status per fix:
**implemented** (code in this branch) · **tested** (automated test in CI passes
locally) · **deployed** (applied to AWS) · **verified** (checked in AWS).
Nothing in this branch is deployed or verified yet; those columns are filled
in by the runbook.

Local results on the branch: `pytest` 265 passed (moto, no AWS); ruff clean;
bandit (medium+) clean; ASL validates (asl-validator 3.15.1); `dbt parse` passes
with dbt-core 1.8.7 (the Glue version) for staging and prod; Terraform
`validate` passes for bootstrap, staging and prod (OpenTofu 1.8 locally, with
the 1.11-only `use_lockfile` stripped for the check); artifacts build and the
packaged handlers import (pyarrow 20.0.0 from the zip).

## Pipeline correctness

**F1 Silent synthetic fallback** · implemented, tested
Root cause: `settings.py` set `ALLOW_SYNTHETIC_FALLBACK=true` for staging/dev and every connector returned hard-coded rows when its API failed, while the run reported success.
Change: `src/ingestion/source_mode.py` (explicit `api | mock_api | synthetic | dataset` per source, no fallback, `C360_REQUIRED_SOURCE_MODE=api` in prod); all six connectors rewritten on `BaseIngest.fetch_records`; every raw row tagged `source_mode`.
Test: `test_source_connectors.py::TestSourceMode`, `TestIngestFlow::test_api_failure_never_falls_back_to_synthetic`, `test_lambda_handlers.py::TestIngest::test_required_mode_blocks_synthetic`.
Limitation: staging starts in explicit `synthetic` mode until source secrets exist.

**F2 Raw write failures reported as success** · implemented, tested
Root cause: `BaseIngest.persist_raw` returned `""` on AccessDenied/NoSuchBucket; the checkpoint could still move.
Change: raises `RawWriteError`; order is raw write → metadata → checkpoint.
Test: `TestIngestFlow::test_raw_write_failure_keeps_checkpoint`, `test_checkpoint.py::test_failed_s3_write_*`.

**F3 run_id did not flow; silver read nothing** · implemented, tested
Root cause: each connector generated its own `uuid4` run_id, so raw files were written under a different run_id than Step Functions used; the silver gate searched for the Step Functions run_id and found no raw data; the ingest handler read a `_last_metadata` attribute that never existed, so `s3_raw_key` was never returned.
Change: run_id passed into every connector; `run()` returns `{status, records_ingested, s3_raw_key, source_mode}`; the state machine passes the exact `s3_raw_key` to the silver gate and `silver_path` to the loader.
Test: `TestIngest::test_synthetic_run_writes_raw_and_moves_checkpoint`, `test_synthetic_run_through_every_handler`.

**F4 Retried ingest could overwrite its own raw file with fewer rows** · implemented, tested
Change: the checkpoint stores `last_raw_key`/`last_record_count`; a retry of a completed run_id returns the stored result.
Test: `TestIngestFlow::test_retry_of_completed_run_is_idempotent`.

**F5 Iceberg MERGE errors swallowed** · implemented, tested
Root cause: `iceberg_loader.py` caught MERGE exceptions and returned `status: success`.
Change: raises (`TransientError` on Athena throttling); handler refuses "success without MERGE"; returns `loaded_row_count`.
Test: `TestIcebergLoad::*`, `TestReconcile::test_success_without_merge_counts_as_failure`.

**F6 Uncatchable state machine errors** · implemented, tested
Root cause: dry-run/zero-row paths returned dicts without fields the ResultSelectors select → `States.Runtime`, which no Catch handles.
Change: every handler returns every selected field in every mode; contract tests read the ASL and compare.
Test: `test_lambda_handlers.py::*_has_every_selected_field`, `*_matches_state_machine`.

**F7 Reconcile could not import** · implemented, tested
Root cause: imported `_emit_metric`, which does not exist.
Change: rewritten: mandatory sources (default hubspot, oms) fail the run with `ReconciliationFailed`; others become warnings → `NotifyWarnings`; numbers that do not add up count as failures.
Test: `TestReconcile::*`.

**F8 Maintenance Lambda broken** · implemented, tested
Root cause: called a non-existent `run_full_maintenance` with wrong table names (`contacts` instead of `hubspot_contacts`).
Change: uses `run_maintenance_all` over `TABLE_REGISTRY`; scheduled weekly by EventBridge Scheduler, not in the batch path.
Test: `TestMaintenance::*`.

**F9 Overlapping runs** · implemented, tested
Change: `initialize` refuses to start if another execution is RUNNING (`states:ListExecutions`, own state machine only).
Test: `TestInitialize::test_refuses_to_overlap_running_execution`.

**F10 Connector bugs** · implemented, tested
Zendesk: wrong auth (`email` instead of `email/token`), ISO `start_time` instead of epoch, one page only. Marketing/support checkpoints used the last record, not the newest. HubSpot retried 401s with tenacity for minutes; synthetic phone was a literal `{idx}`.
Test: `test_zendesk_mapping`, `TestHttpClient::*`.
Limitation: HubSpot still pages through all contacts and filters client-side.

**F11 OMS/PIM had no real source** · implemented, unit-tested, **not run live**
Change: Shopify Admin GraphQL adapter (`src/ingestion/shopify.py`), `dataset` mode for files such as Olist. See `OMS_PIM_API_INTEGRATION_GUIDE.md`.

## Packaging and deploy

**F12 Lambdas could not run their code** · implemented, tested (build)
Root cause: zips had no third-party packages; `silver_gate`/`iceberg_loader` ran `pip install pyarrow` at runtime; Terraform could deploy placeholder code.
Change: `scripts/package_artifacts.py` builds `artifacts/<sha>/...` with Linux wheels (pyarrow 20.0.0: newest with manylinux2014 wheels, needed by the Amazon Linux 2 python3.11 runtime); runtime installs removed; Lambda `s3_key` always points at a versioned artifact.
Test: local build + import of packaged handlers. Limitation: not yet executed on Lambda.

**F13 CI never ran** · implemented (workflow), not run on GitHub
Root cause: the workflow lived in `customer360/.github/workflows/`, which GitHub ignores; it also used `|| true` and stored no OIDC environment pinning.
Change: root `.github/workflows/ci.yml`: checks (ruff, bandit, pytest, ASL, dbt 1.8.7 parse, terraform validate, gitleaks) → PR plan (read-only role) → build once → staging deploy with dry-run smoke test → prod only via manual dispatch on `main` in the `production` environment.
Test: actionlint clean. Limitation: first real run happens on this PR.

**F14 Tests depended on whoever ran them** · implemented, tested
Root cause: 17 tests wrote to real S3 and failed without credentials.
Change: `tests/conftest.py` runs every test inside moto with a fresh bucket; checkpoints go to moto, not `./.checkpoints`.

## Infrastructure and security

**F15 Terraform did not initialise** · implemented, validated — `;` one-line blocks, stale modules; staging/prod roots rewritten; `iam`, `mwaa`, `observability` modules removed. **M5 must review any destroys.**

**F16 Deprecated DynamoDB locking** · implemented — S3-native `use_lockfile` (Terraform ≥ 1.11).

**F17 Over-broad roles** · implemented, validated
One Lambda role per function scoped to S3 prefixes; Glue, Step Functions, Scheduler roles scoped; every workload role carries `c360-workload-boundary`; deploy roles can only create roles with that boundary; OIDC subjects pinned to GitHub environments; operator role with MFA. See `PRODUCTION_PERMISSIONS_GUIDE.md`.
Limitation: not yet checked with Access Analyzer or `simulate-principal-policy`.

**F18 Alerting** · implemented, validated
EventBridge rule on execution FAILED/TIMED_OUT/ABORTED → SNS (topic policy restricted by `aws:SourceArn`); `ExecutionsFailed/TimedOut/Aborted` alarms; DQ alarms now match the metrics (metrics also published with the Environment-only dimension).
Limitation: email delivery is verified only by M7.

**F19 Glue job KMS + Lake Formation** · implemented — Glue role gets the prod KMS key; Glue DB default permission `ALL` for `IAM_ALLOWED_PRINCIPALS` (IAM-only mode kept).

**F20 dbt runner extraction** · implemented, tested — archive members validated (no `../`, links); temp dir via `mkdtemp`. Test: `test_dbt_runner_extract.py`.

## dbt models

**F21 dbt could never run on Glue** · implemented, tested
Root cause: `schema.yml` used the dbt ≥ 1.10 `arguments:` test syntax; the Glue job pins dbt-core 1.8.7, which fails to compile the project, so both `DbtRun` and `DbtTest` would fail.
Change: 1.8-compatible syntax (`data_tests:`), CI parses with 1.8.7.

**F22 Identity resolution** · implemented, tested (DuckDB)
Root cause: OMS rows had no email so orders never matched a CRM contact; ambiguous email/phone matches could fan one source key into several master ids (double-counted revenue).
Change: one row per source key (lowest contact id wins), OMS `customer_email` carried through silver/Iceberg/staging, new rule `shared_customer_id_email_match` (0.85); tests `assert_bridge_unique_source_key`, `assert_fact_order_unique`, `test_dbt_identity_bridge.py`.

**F23 KPI definitions** · implemented, parsed (not run on Athena)
AOV = revenue / orders (was a mean of per-customer means); CSAT unrated = NULL (was 0); RFM uses recency; `estimated_clv` renamed `clv_proxy` and no longer invents 50 for non-buyers; `dim_date` spine fixed (ended Feb 2025, now 2016–2030); new `fct_kpi_daily`; `assert_revenue_reconciles` (fact_order = customer_360 = executive KPIs).

**F24 Wrong defaults** · implemented — prod settings/profile defaults pointed at the staging bucket, `primary` workgroup and a non-existent database; prod dbt data now under `dbt/`.

## Not done / out of scope

* No live run of any source API, Glue job, Athena query or deploy from this branch.
* Shopify/Olist seeding and Olist→contract converter scripts.
* Airflow DAGs and docs (`dags/`, `docs/AIRFLOW_*`) are left as the local-dev path; Step Functions is the production orchestrator.
