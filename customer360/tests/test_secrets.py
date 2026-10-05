from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.config.secrets import (
    DEFAULT_SECRET_NAME,
    SecretResolutionError,
    SecretsManager,
    get_secret,
)


def test_secrets_manager_env_fallback(monkeypatch):
    """Verify that in local dev mode, secrets fall back to environment variables."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("TEST_API_KEY", "env_secret_12345")

    sm = SecretsManager(secret_name="dummy", auto_load=False)
    val = sm.get_secret("TEST_API_KEY")
    assert val == "env_secret_12345"


def test_secrets_manager_aws_success():
    """Verify that AWS Secrets Manager payload is decrypted and cached."""
    mock_client = MagicMock()
    secret_payload = {
        "HUBSPOT_ACCESS_TOKEN": "aws_hubspot_token",
        "ZENDESK_API_KEY": "aws_zendesk_key",
    }
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(secret_payload)
    }

    sm = SecretsManager(secret_name="c_360_secret_manager", client=mock_client, auto_load=True)

    assert sm.get_secret("HUBSPOT_ACCESS_TOKEN") == "aws_hubspot_token"
    assert sm.get_secret("ZENDESK_API_KEY") == "aws_zendesk_key"
    assert mock_client.get_secret_value.call_count == 1

    # Second call should use cache
    assert sm.get_secret("HUBSPOT_ACCESS_TOKEN") == "aws_hubspot_token"
    assert mock_client.get_secret_value.call_count == 1


def test_secrets_manager_required_missing(monkeypatch):
    """Verify that missing required secret raises SecretResolutionError."""
    monkeypatch.setenv("ENVIRONMENT", "dev")
    sm = SecretsManager(auto_load=False)

    with pytest.raises(SecretResolutionError) as exc_info:
        sm.get_secret("NON_EXISTENT_KEY", required=True)

    assert "NON_EXISTENT_KEY" in str(exc_info.value)


def test_secrets_manager_cloud_failure_raises(monkeypatch):
    """In production, failure to contact Secrets Manager must raise an error, not fall back."""
    monkeypatch.setenv("ENVIRONMENT", "prod")
    mock_client = MagicMock()
    mock_client.get_secret_value.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Secret not found"}},
        "GetSecretValue",
    )

    sm = SecretsManager(secret_name="c_360_secret_manager", client=mock_client, auto_load=False)

    with pytest.raises(SecretResolutionError):
        sm.load_secrets_from_aws()


def test_secrets_manager_default_fallback():
    """Verify that a default value is returned when secret is not found."""
    sm = SecretsManager(auto_load=False)
    assert sm.get_secret("OPTIONAL_KEY", default="default_val") == "default_val"
