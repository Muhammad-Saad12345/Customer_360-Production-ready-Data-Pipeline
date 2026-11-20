"""Test isolation: every test runs against moto, never a real AWS account.

Before this file existed, tests that write to S3 used whatever credentials the
machine had (and failed with 403 without them), so the suite's result depended
on who ran it.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_REGION", "us-east-1")

TEST_BUCKETS = ("c360-bucket786",)


@pytest.fixture(autouse=True)
def _aws_isolation(monkeypatch):
    from moto import mock_aws

    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(key, "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    # Checkpoints go to the per-test moto bucket instead of ./.checkpoints in the repo.
    monkeypatch.setenv("CHECKPOINT_BACKEND", "s3")
    # No per-source secrets / modes leak between tests.
    for key in ("C360_SOURCE_MODES", "C360_REQUIRED_SOURCE_MODE", "C360_SOURCE_SECRET_PREFIX"):
        monkeypatch.delenv(key, raising=False)

    with mock_aws():
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        for bucket in TEST_BUCKETS:
            s3.create_bucket(Bucket=bucket)
        import src.monitoring.cloudwatch as cw

        cw._cw_client = None
        yield
        cw._cw_client = None
