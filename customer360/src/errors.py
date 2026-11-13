"""Error types shared by the ingestion code and the Lambda handlers.

Step Functions matches retry/catch rules on the exception class name, so the
names here are part of the state machine contract:

* ``TransientError``      – retried by every Lambda task (see the ASL Retry list).
* everything else         – not retried; the per-source Catch records the failure.
"""
from __future__ import annotations


class TransientError(Exception):
    """A temporary failure (throttling, 5xx, network). Safe to retry."""


class SourceModeError(Exception):
    """The source is not configured to run in the requested/required mode."""


class SourceRequestError(Exception):
    """The source API rejected the request permanently (4xx other than 429)."""


class RawWriteError(Exception):
    """Raw records could not be written to S3; the checkpoint must not move."""


class ReconciliationFailed(Exception):
    """A mandatory source failed, so the run must not reach the dbt layer."""
