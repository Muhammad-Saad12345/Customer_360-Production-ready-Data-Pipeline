"""Helpers shared by the synthetic generators."""
from __future__ import annotations

import os


def include_invalid_synthetic() -> bool:
    """Whether synthetic batches include the deliberately broken records.

    Default true for local dev/tests (they exercise the DQ gate). Terraform sets
    C360_SYNTHETIC_INCLUDE_INVALID=false in staging so a synthetic run is not
    blocked by the >=10% rejection gate; set it to true there to prove the
    gate blocks a bad batch (runbook test T6).
    """
    return os.environ.get("C360_SYNTHETIC_INCLUDE_INVALID", "true").lower() in ("1", "true", "yes")
