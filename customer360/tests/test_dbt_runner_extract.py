"""The Glue dbt runner must refuse archive entries that escape the work dir."""
from __future__ import annotations

import importlib.util
import io
import tarfile
from pathlib import Path

import boto3
import pytest

RUNNER = Path(__file__).resolve().parent.parent / "glue_jobs" / "dbt_runner.py"


def _runner():
    spec = importlib.util.spec_from_file_location("dbt_runner", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tar(entries):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def test_extracts_project(tmp_path):
    boto3.client("s3").put_object(Bucket="c360-bucket786", Key="a/dbt.tar.gz", Body=_tar([("dbt_project.yml", b"name: x\n")]))
    _runner()._download_s3_artifact("s3://c360-bucket786/a/dbt.tar.gz", tmp_path / "w")
    assert (tmp_path / "w" / "dbt_project.yml").exists()


def test_rejects_path_traversal(tmp_path):
    boto3.client("s3").put_object(Bucket="c360-bucket786", Key="a/evil.tar.gz", Body=_tar([("../evil.txt", b"x")]))
    with pytest.raises(ValueError):
        _runner()._download_s3_artifact("s3://c360-bucket786/a/evil.tar.gz", tmp_path / "w")
    assert not (tmp_path / "evil.txt").exists()
