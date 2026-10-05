"""Centralized Secrets Management Abstraction for Customer 360 Lakehouse.

Resolves secrets dynamically from AWS Secrets Manager in production/staging environments
with local .env / os.environ fallback for development and testing.

Guarantees:
1. Never logs secret values or tokens.
2. In production/staging, verifies credentials against AWS Secrets Manager.
3. Provides explicit, descriptive exceptions on missing required secrets.
4. Caches retrieved secrets in memory for the process lifetime to minimize AWS API calls.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger("customer360.secrets")

DEFAULT_SECRET_NAME = "c_360_secret_manager"
DEFAULT_REGION = "us-east-1"


class SecretResolutionError(RuntimeError):
    """Raised when a required secret cannot be resolved."""
    pass


class SecretsManager:
    """Manages secret retrieval with tiered lookup: In-Memory Cache -> Secrets Manager -> os.environ."""

    def __init__(
        self,
        secret_name: Optional[str] = None,
        region_name: Optional[str] = None,
        client: Optional[Any] = None,
        auto_load: Optional[bool] = None,
    ):
        self.secret_name = secret_name or os.getenv("SECRETS_MANAGER_SECRET_NAME", DEFAULT_SECRET_NAME)
        self.region_name = region_name or os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", DEFAULT_REGION))
        self._client = client
        self._cached_secrets: Dict[str, str] = {}
        self._loaded_from_aws: bool = False

        if auto_load is None:
            auto_load = self.is_cloud_environment()

        if auto_load:
            self.load_secrets_from_aws()

    @staticmethod
    def is_cloud_environment() -> bool:
        """Determines if running in an automated cloud tier (staging / prod)."""
        env = os.getenv("ENVIRONMENT", "dev").lower().strip()
        return env in ("staging", "stage", "prod", "production")

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        import boto3
        self._client = boto3.client("secretsmanager", region_name=self.region_name)
        return self._client

    def load_secrets_from_aws(self, force_refresh: bool = False) -> Dict[str, str]:
        """Fetch and decrypt secret payload from AWS Secrets Manager."""
        if self._loaded_from_aws and not force_refresh:
            return self._cached_secrets

        try:
            logger.info("Resolving secrets from AWS Secrets Manager [secret=%s, region=%s]", self.secret_name, self.region_name)
            client = self._get_client()
            response = client.get_secret_value(SecretId=self.secret_name)
            secret_string = response.get("SecretString")

            if secret_string:
                try:
                    parsed = json.loads(secret_string)
                    if isinstance(parsed, dict):
                        self._cached_secrets.update({str(k): str(v) for k, v in parsed.items() if v is not None})
                    else:
                        self._cached_secrets[self.secret_name] = str(parsed)
                except json.JSONDecodeError:
                    # In case the secret is stored as a raw key-value string
                    self._cached_secrets[self.secret_name] = secret_string

            self._loaded_from_aws = True
            logger.info("Successfully loaded %d secrets from AWS Secrets Manager", len(self._cached_secrets))
            return self._cached_secrets

        except Exception as exc:
            msg = f"Failed to retrieve secret '{self.secret_name}' from AWS Secrets Manager in region '{self.region_name}': {exc}"
            if self.is_cloud_environment():
                logger.error("%s", msg)
                raise SecretResolutionError(msg) from exc
            logger.warning("%s. Falling back to local environment variables.", msg)
            return self._cached_secrets

    def get_secret(
        self,
        key: str,
        default: Optional[str] = None,
        required: bool = False,
    ) -> Optional[str]:
        """
        Retrieve a secret value by key.
        Precedence:
          1. AWS Secrets Manager cached dictionary
          2. Environment variable (os.environ)
          3. Default fallback value
        """
        # 1. Check cached Secrets Manager dictionary
        if key in self._cached_secrets and self._cached_secrets[key]:
            return self._cached_secrets[key]

        # 2. Check local environment variable
        env_val = os.getenv(key)
        if env_val is not None and env_val != "":
            return env_val

        # 3. If required, raise descriptive error
        if required and default is None:
            raise SecretResolutionError(
                f"Required secret '{key}' was not found in AWS Secrets Manager '{self.secret_name}' or environment variables."
            )

        return default


# Global singleton instance
_GLOBAL_SECRETS_MANAGER: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """Get or initialize the global SecretsManager instance."""
    global _GLOBAL_SECRETS_MANAGER
    if _GLOBAL_SECRETS_MANAGER is None:
        _GLOBAL_SECRETS_MANAGER = SecretsManager(auto_load=False)
        if _GLOBAL_SECRETS_MANAGER.is_cloud_environment():
            _GLOBAL_SECRETS_MANAGER.load_secrets_from_aws()
    return _GLOBAL_SECRETS_MANAGER


def get_secret(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """Convenience helper to retrieve a secret via the global SecretsManager."""
    return get_secrets_manager().get_secret(key, default=default, required=required)
