"""Which data source mode each connector runs in, and where its credentials live.

Modes
-----
api       the real system (HubSpot, Shopify for OMS/PIM, Mailchimp, Zendesk,
          or the loyalty REST API). Credentials come from Secrets Manager.
mock_api  the REST contract served by ``mock_sources/`` (local docker-compose
          or a hosted mock). Proves the HTTP/pagination path, not real data.
synthetic deterministic in-code generator. For dev, tests and first staging runs.
dataset   JSON-lines files you uploaded to s3://<bucket>/datasets/<source>/<entity>/
          (for example an Olist extract). Real-shaped data without an API.

There is no implicit fallback between modes. A connector configured for
``api`` that cannot reach its API fails; it never quietly returns synthetic
rows. ``C360_REQUIRED_SOURCE_MODE`` (set to ``api`` in prod) makes any other
mode a hard error.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

from src.errors import SourceModeError

logger = logging.getLogger("customer360.source_mode")

VALID_MODES = ("api", "mock_api", "synthetic", "dataset")
CLOUD_ENVIRONMENTS = ("staging", "stage", "prod", "production")


def _environment() -> str:
    return os.environ.get("ENVIRONMENT", "dev").lower().strip()


def configured_modes() -> Dict[str, str]:
    raw = os.environ.get("C360_SOURCE_MODES", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SourceModeError(f"C360_SOURCE_MODES is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SourceModeError("C360_SOURCE_MODES must be a JSON object of source -> mode")
    return {str(k): str(v).lower().strip() for k, v in parsed.items()}


def resolve_source_mode(source: str) -> str:
    """Return the mode for *source*, enforcing the required mode if one is set."""
    override = os.environ.get(f"C360_SOURCE_MODE_{source.upper()}", "").lower().strip()
    mode = override or configured_modes().get(source, "")

    if not mode:
        if _environment() in CLOUD_ENVIRONMENTS:
            raise SourceModeError(
                f"No source mode configured for '{source}' in {_environment()}. "
                "Set it in C360_SOURCE_MODES (Terraform variable source_modes)."
            )
        mode = "synthetic"  # local dev and unit tests only

    if mode not in VALID_MODES:
        raise SourceModeError(f"Invalid source mode '{mode}' for '{source}'. Valid: {', '.join(VALID_MODES)}")

    required = os.environ.get("C360_REQUIRED_SOURCE_MODE", "").lower().strip()
    if required and mode != required:
        raise SourceModeError(
            f"Source '{source}' is configured as '{mode}' but this environment requires '{required}'."
        )
    return mode


def load_source_credentials(source: str, client: Any = None) -> Dict[str, Any]:
    """Read ``<C360_SOURCE_SECRET_PREFIX><source>`` (a JSON object) from Secrets Manager.

    Returns ``{}`` when the secret does not exist so callers can fall back to the
    legacy shared secret; any other error is raised. Values are never logged.
    """
    prefix = os.environ.get("C360_SOURCE_SECRET_PREFIX", "").strip()
    if not prefix:
        return {}
    secret_id = f"{prefix}{source}"
    if client is None:
        import boto3

        client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:  # botocore ClientError
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if code == "ResourceNotFoundException":
            logger.info("No per-source secret %s; using legacy settings if present", secret_id)
            return {}
        raise
    try:
        value = json.loads(response.get("SecretString") or "{}")
    except json.JSONDecodeError as exc:
        raise SourceModeError(f"Secret {secret_id} must be a JSON object") from exc
    if not isinstance(value, dict):
        raise SourceModeError(f"Secret {secret_id} must be a JSON object")
    logger.info("Loaded per-source secret %s (%d keys)", secret_id, len(value))
    return value
