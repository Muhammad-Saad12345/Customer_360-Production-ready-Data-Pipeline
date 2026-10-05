# Customer 360 Deployment Guide

## Local Development

Local development is intentionally simple and cost-aware.

```text
Windows host
    |
    +--> Docker Desktop
    +--> Docker Compose
    +--> Apache Airflow
    +--> PostgreSQL metadata database
    +--> Customer 360 Python modules
    +--> AWS S3
```

This is the current project priority: orchestrate proven processing modules with Airflow locally without introducing costly AWS services or additional infrastructure.

## Production

A future production deployment would likely use a managed Airflow environment, such as Amazon MWAA, while keeping the existing Python extraction and DQ logic intact.

The production path would require:

- managed Airflow runtime
- managed metadata database
- IAM role/permissions for S3
- secret management
- logging and monitoring
- CI/CD pipeline
- environment separation
- alerting and incident response
- backup and recovery plans

## What would need to change before production

- Replace local `.env` secrets with a managed secrets system
- Use IAM roles instead of long-lived access keys where possible
- Move Airflow metadata to a managed database
- Add monitoring and alerting around DAG failures
- Define a proper operational ownership model
- Add environment-specific deployments for dev/test/prod
- Use version-controlled deployments with CI/CD

## Current project posture

This repository is not yet production-ready because the local Airflow setup is still a development environment and the Zendesk integration remains externally blocked. The project is intentionally focused on local orchestration and validation before production hardening.

## Lakehouse Layer Implementation (Complete)

The Apache Iceberg Lakehouse layer is implemented on AWS S3, AWS Glue Data Catalog, and Amazon Athena Engine v3.

```text
Sources
    -> Airflow
    -> Raw S3
    -> DQ / validation
    -> Silver (Parquet)
    -> Iceberg / lakehouse (Complete - see docs/ICEBERG_LAKEHOUSE.md)
    -> [Next] identity resolution
    -> [Next] golden customer model
    -> [Next] Customer 360 marts
    -> [Next] dbt / warehouse layer
    -> [Next] BI and analytics
```

For complete architectural details, schema registries, AWS resource configurations, Airflow DAG integration, and operational runbooks, refer to:
- [ICEBERG_LAKEHOUSE.md](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/docs/ICEBERG_LAKEHOUSE.md)
