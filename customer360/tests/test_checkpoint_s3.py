from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pytest

from src.checkpoint import CheckpointStore


def test_s3_checkpoint_get_or_create_missing():
    mock_s3 = MagicMock()
    # Simulate S3 NoSuchKey exception
    mock_s3.get_object.side_effect = Exception("NoSuchKey")

    store = CheckpointStore(
        bucket="my-test-bucket",
        s3_client=mock_s3,
        storage_backend="s3",
    )

    rec = store.get_or_create("oms", "orders", default_watermark="2025-01-01T00:00:00Z")
    assert rec["source"] == "oms"
    assert rec["entity"] == "orders"
    assert rec["last_successful_watermark"] == "2025-01-01T00:00:00Z"
    assert rec["status"] == "not_started"

    # Verify that initial record was written to S3
    mock_s3.put_object.assert_called_once()
    _, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == "my-test-bucket"
    assert kwargs["Key"] == "checkpoints/oms/orders.json"


def test_s3_checkpoint_round_trip():
    mock_s3 = MagicMock()
    existing_payload = {
        "source": "hubspot",
        "entity": "contacts",
        "last_successful_watermark": "2025-01-10T12:00:00Z",
        "last_successful_run_id": "run-001",
        "updated_at": "2025-01-10T12:00:00Z",
        "status": "success",
    }
    body_mock = MagicMock()
    body_mock.read.return_value = json.dumps(existing_payload).encode("utf-8")
    mock_s3.get_object.return_value = {"Body": body_mock}

    store = CheckpointStore(
        bucket="my-test-bucket",
        s3_client=mock_s3,
        storage_backend="s3",
    )

    rec = store.get_or_create("hubspot", "contacts")
    assert rec["last_successful_watermark"] == "2025-01-10T12:00:00Z"

    store.update(
        source="hubspot",
        entity="contacts",
        watermark="2025-01-11T12:00:00Z",
        run_id="run-002",
        status="success",
    )

    mock_s3.put_object.assert_called_once()
    _, kwargs = mock_s3.put_object.call_args
    assert kwargs["Key"] == "checkpoints/hubspot/contacts.json"
    written_data = json.loads(kwargs["Body"].decode("utf-8"))
    assert written_data["last_successful_watermark"] == "2025-01-11T12:00:00Z"
    assert written_data["last_successful_run_id"] == "run-002"
