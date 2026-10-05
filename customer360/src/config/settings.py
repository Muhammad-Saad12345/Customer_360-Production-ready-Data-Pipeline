from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

from src.config.secrets import get_secret

load_dotenv()
logger = logging.getLogger("customer360.config")


@dataclass
class Settings:
    ENVIRONMENT: str
    AWS_REGION: str
    S3_BUCKET: str

    # Secrets Manager
    SECRETS_MANAGER_SECRET_NAME: str

    # HubSpot
    HUBSPOT_ACCESS_TOKEN: Optional[str]
    HUBSPOT_BASE_URL: str
    HUBSPOT_LOOKBACK_MINUTES: int = 15

    # Marketing
    MARKETING_PROVIDER: Optional[str] = None
    MARKETING_API_KEY: Optional[str] = None
    MAILCHIMP_API_KEY: Optional[str] = None
    MAILCHIMP_SERVER_PREFIX: Optional[str] = None
    MAILCHIMP_AUDIENCE_ID: Optional[str] = None

    # Zendesk
    ZENDESK_SUBDOMAIN: Optional[str] = None
    ZENDESK_API_KEY: Optional[str] = None
    ZENDESK_EMAIL: Optional[str] = None
    ZENDESK_SECRET_KEY: Optional[str] = None
    ZENDESK_KEY_ID: Optional[str] = None

    # Mock Source Endpoints
    OMS_BASE_URL: Optional[str] = None
    PIM_BASE_URL: Optional[str] = None
    LOYALTY_BASE_URL: Optional[str] = None
    LOYALTY_API_BASE_URL: Optional[str] = None
    LOYALTY_API_KEY: Optional[str] = None

    # Lakehouse & Athena Configuration
    ICEBERG_DATABASE: str = "customer360_iceberg"
    ATHENA_WORKGROUP: str = "primary"
    ATHENA_OUTPUT_LOCATION: Optional[str] = None
    ICEBERG_S3_PREFIX: str = "iceberg"
    ICEBERG_MAINTENANCE_RETENTION_DAYS: int = 7
    ATHENA_QUERY_TIMEOUT_SECONDS: float = 900.0

    # Data Quality & Production Gates
    REJECTION_RATE_THRESHOLD: float = 0.10
    ZERO_RECORD_ANOMALY_FAIL: bool = True

    # Observability & Alerts
    LOG_LEVEL: str = "INFO"
    SNS_ALERT_TOPIC_ARN: Optional[str] = None
    CLOUDWATCH_NAMESPACE: str = "Customer360/Pipeline"


def get_settings() -> Settings:
    """Resolve settings based on the active ENVIRONMENT (dev | staging | prod | test)."""
    env = (os.getenv("ENVIRONMENT") or "dev").lower().strip()
    aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    secret_name = os.getenv("SECRETS_MANAGER_SECRET_NAME", "c_360_secret_manager")

    # Environment-specific defaults
    if env in ("prod", "production"):
        default_bucket = "customer360-lakehouse-prod"
        default_db = "customer360_iceberg"
        default_wg = "customer360_prod"
    elif env in ("staging", "stage"):
        default_bucket = "customer360-lakehouse-stage"
        default_db = "customer360_iceberg_stage"
        default_wg = "customer360_staging"
    else:  # dev / test
        default_bucket = "customer360-lakehouse-dev"
        default_db = "customer360_iceberg_dev"
        default_wg = "primary"

    # Allow explicit override from environment if provided (e.g. existing c360-bucket786 in .env)
    s3_bucket = os.getenv("S3_BUCKET") or os.getenv("AWS_S3_BUCKET") or default_bucket
    iceberg_database = os.getenv("ICEBERG_DATABASE") or default_db
    athena_workgroup = os.getenv("ATHENA_WORKGROUP") or default_wg

    # Credentials resolved via secrets abstraction (Secrets Manager in cloud tiers, env fallback in dev)
    hubspot_token = get_secret("HUBSPOT_ACCESS_TOKEN") or get_secret("HUBSPOT_API_KEY")
    hubspot_base = os.getenv("HUBSPOT_BASE_URL") or "https://api.hubapi.com"
    hubspot_lookback = int(os.getenv("HUBSPOT_LOOKBACK_MINUTES", "15"))

    marketing_api_key = (
        get_secret("MARKETING_API_KEY")
        or get_secret("BREVO_API_KEY")
        or get_secret("MAILCHIMP_API_KEY")
        or get_secret("MAILCHIMP_API_TOKEN")
    )
    mailchimp_api_key = get_secret("MAILCHIMP_API_KEY") or get_secret("MAILCHIMP_API_TOKEN")
    mailchimp_server_prefix = os.getenv("MAILCHIMP_SERVER_PREFIX") or os.getenv("MAILCHIMP_DC") or ""
    mailchimp_audience_id = os.getenv("MAILCHIMP_AUDIENCE_ID") or os.getenv("MAILCHIMP_LIST_ID") or ""

    marketing_provider = os.getenv("MARKETING_PROVIDER") or os.getenv("EMAIL_PROVIDER")
    if not marketing_provider:
        if mailchimp_api_key:
            marketing_provider = "mailchimp"
        elif env in ("prod", "production", "staging", "stage"):
            marketing_provider = "mailchimp"
        else:
            marketing_provider = "synthetic"

    zendesk_subdomain = os.getenv("ZENDESK_SUBDOMAIN") or os.getenv("ZENDESK_DOMAIN")
    zendesk_api_key = (
        get_secret("ZENDESK_API_KEY")
        or get_secret("ZENDESK_SECRET_KEY")
        or get_secret("ZENDESK_API_TOKEN")
        or get_secret("ZENDESK_TOKEN")
    )
    zendesk_secret_key = get_secret("ZENDESK_SECRET_KEY") or zendesk_api_key
    zendesk_key_id = os.getenv("ZENDESK_KEY_ID") or os.getenv("ZENDESK_KEY_id") or os.getenv("ZENDESK_CLIENT_ID")
    zendesk_email = os.getenv("ZENDESK_EMAIL") or os.getenv("ZENDESK_USER")

    oms_base_url = os.getenv("OMS_BASE_URL") or "http://localhost:8001"
    pim_base_url = os.getenv("PIM_BASE_URL") or "http://localhost:8002"
    loyalty_base_url = os.getenv("LOYALTY_BASE_URL") or os.getenv("LOYALTY_API_BASE_URL") or "http://localhost:8003"
    loyalty_api_key = get_secret("LOYALTY_API_KEY")

    athena_output = os.getenv("ATHENA_OUTPUT_LOCATION") or f"s3://{s3_bucket}/athena-results/"
    rejection_threshold = float(os.getenv("REJECTION_RATE_THRESHOLD", "0.10"))
    retention_days = int(os.getenv("ICEBERG_MAINTENANCE_RETENTION_DAYS", "7"))
    athena_timeout = float(os.getenv("ATHENA_QUERY_TIMEOUT_SECONDS", "900.0"))
    sns_topic = os.getenv("SNS_ALERT_TOPIC_ARN")

    return Settings(
        ENVIRONMENT=env,
        AWS_REGION=aws_region,
        S3_BUCKET=s3_bucket,
        SECRETS_MANAGER_SECRET_NAME=secret_name,
        HUBSPOT_ACCESS_TOKEN=hubspot_token,
        HUBSPOT_BASE_URL=hubspot_base,
        HUBSPOT_LOOKBACK_MINUTES=hubspot_lookback,
        MARKETING_PROVIDER=marketing_provider,
        MARKETING_API_KEY=marketing_api_key,
        MAILCHIMP_API_KEY=mailchimp_api_key,
        MAILCHIMP_SERVER_PREFIX=mailchimp_server_prefix,
        MAILCHIMP_AUDIENCE_ID=mailchimp_audience_id,
        ZENDESK_SUBDOMAIN=zendesk_subdomain,
        ZENDESK_API_KEY=zendesk_api_key,
        ZENDESK_EMAIL=zendesk_email,
        ZENDESK_SECRET_KEY=zendesk_secret_key,
        ZENDESK_KEY_ID=zendesk_key_id,
        OMS_BASE_URL=oms_base_url,
        PIM_BASE_URL=pim_base_url,
        LOYALTY_BASE_URL=loyalty_base_url,
        LOYALTY_API_BASE_URL=loyalty_base_url,
        LOYALTY_API_KEY=loyalty_api_key,
        ICEBERG_DATABASE=iceberg_database,
        ATHENA_WORKGROUP=athena_workgroup,
        ATHENA_OUTPUT_LOCATION=athena_output,
        ICEBERG_S3_PREFIX=os.getenv("ICEBERG_S3_PREFIX", "iceberg"),
        ICEBERG_MAINTENANCE_RETENTION_DAYS=retention_days,
        ATHENA_QUERY_TIMEOUT_SECONDS=athena_timeout,
        REJECTION_RATE_THRESHOLD=rejection_threshold,
        ZERO_RECORD_ANOMALY_FAIL=(os.getenv("ZERO_RECORD_ANOMALY_FAIL", "true").lower() in ("true", "1")),
        LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO"),
        SNS_ALERT_TOPIC_ARN=sns_topic,
        CLOUDWATCH_NAMESPACE=os.getenv("CLOUDWATCH_NAMESPACE", "Customer360/Pipeline"),
    )
