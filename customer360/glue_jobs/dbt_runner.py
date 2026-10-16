"""
AWS Glue Python Shell Job: dbt_runner.py
Step Functions states: DbtRun, DbtTest

Executes dbt transformation models and schema/data tests in AWS Glue Python Shell.
Glue Python Shell runs with 1/16 DPU (~$0.04/hour), providing a lightweight,
cost-effective serverless environment for dbt execution without the Lambda 250MB limit.

Usage (Glue Python Shell):
    aws glue start-job-run \
        --job-name c360-dbt-runner-staging \
        --arguments '{
            "--dbt-target": "staging",
            "--dbt-action": "both",
            "--run-id": "c360-...",
            "--s3-dbt-path": "s3://c360-bucket786/artifacts/dbt/dbt_project.tar.gz"
        }'

Glue Job Configuration:
    - Type: pythonshell
    - PythonVersion: 3.9
    - MaxCapacity: 0.0625 (1/16 DPU)
    - DefaultArguments:
        --additional-python-modules: "dbt-core==1.8.7,dbt-athena-community==1.8.3"
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("customer360.glue.dbt_runner")


def _parse_args() -> Dict[str, str]:
    """Parse job arguments from AWS Glue or command-line."""
    args_dict = {
        "dbt-target": "staging",
        "dbt-action": "both",
        "run-id": "manual-run",
        "s3-dbt-path": "",
        "environment": "staging",
        "s3-bucket": "c360-bucket786",
        "athena-workgroup": "customer360-staging",
        "iceberg-database": "customer360_iceberg_stage",
    }

    # Attempt to resolve AWS Glue arguments
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore
        glue_params = [
            "JOB_NAME",
            "dbt-target",
            "dbt-action",
            "run-id",
            "s3-dbt-path",
            "environment",
            "s3-bucket",
            "athena-workgroup",
            "iceberg-database",
        ]
        # Only query args that are present in sys.argv
        present_params = []
        for p in glue_params:
            if f"--{p}" in sys.argv:
                present_params.append(p)
        if present_params:
            resolved = getResolvedOptions(sys.argv, present_params)
            args_dict.update({k.replace("_", "-"): v for k, v in resolved.items()})
            logger.info("Resolved Glue arguments: %s", args_dict)
            return args_dict
    except ImportError:
        logger.info("awsglue module not present; falling back to argparse")

    parser = argparse.ArgumentParser(description="Customer 360 dbt runner for AWS Glue Python Shell")
    parser.add_argument("--JOB_NAME", dest="job_name", default="local-dbt-runner")
    parser.add_argument("--dbt-target", dest="dbt_target", default=args_dict["dbt-target"])
    parser.add_argument("--dbt-action", dest="dbt_action", choices=["run", "test", "both"], default=args_dict["dbt-action"])
    parser.add_argument("--run-id", dest="run_id", default=args_dict["run-id"])
    parser.add_argument("--s3-dbt-path", dest="s3_dbt_path", default=args_dict["s3-dbt-path"])
    parser.add_argument("--environment", dest="environment", default=args_dict["environment"])
    parser.add_argument("--s3-bucket", dest="s3_bucket", default=args_dict["s3-bucket"])
    parser.add_argument("--athena-workgroup", dest="athena_workgroup", default=args_dict["athena-workgroup"])
    parser.add_argument("--iceberg-database", dest="iceberg_database", default=args_dict["iceberg-database"])

    parsed, _ = parser.parse_known_args()
    args_dict.update({
        "dbt-target": parsed.dbt_target,
        "dbt-action": parsed.dbt_action,
        "run-id": parsed.run_id,
        "s3-dbt-path": parsed.s3_dbt_path,
        "environment": parsed.environment,
        "s3-bucket": parsed.s3_bucket,
        "athena-workgroup": parsed.athena_workgroup,
        "iceberg-database": parsed.iceberg_database,
    })
    return args_dict


def _download_s3_artifact(s3_uri: str, target_dir: Path) -> None:
    """Download and extract dbt project package from S3."""
    import boto3

    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    logger.info("Downloading dbt package from s3://%s/%s to %s", bucket, key, target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    archive_file = target_dir / Path(key).name

    s3 = boto3.client("s3")
    s3.download_file(bucket, key, str(archive_file))
    logger.info("Downloaded %d bytes to %s", archive_file.stat().st_size, archive_file)

    if archive_file.name.endswith(".tar.gz") or archive_file.name.endswith(".tgz"):
        with tarfile.open(archive_file, "r:gz") as tar:
            tar.extractall(path=target_dir)
        archive_file.unlink()
    elif archive_file.name.endswith(".zip"):
        with zipfile.ZipFile(archive_file, "r") as z:
            z.extractall(path=target_dir)
        archive_file.unlink()


def _resolve_project_dir(s3_path: str) -> Path:
    """Locate the dbt project root containing dbt_project.yml."""
    work_dir = Path("/tmp/dbt_project")
    if s3_path:
        _download_s3_artifact(s3_path, work_dir)
        if (work_dir / "dbt_project.yml").exists():
            return work_dir
        # Check subdirectories
        for sub in work_dir.iterdir():
            if sub.is_dir() and (sub / "dbt_project.yml").exists():
                return sub

    # Local fallback candidates
    candidates = [
        Path.cwd() / "dbt",
        Path(__file__).resolve().parent.parent / "dbt",
        Path(__file__).resolve().parent / "dbt",
    ]
    for c in candidates:
        if c.exists() and (c / "dbt_project.yml").exists():
            return c

    raise FileNotFoundError(
        f"dbt_project.yml not found in work directory {work_dir} or local candidates"
    )


def _execute_dbt(cmd: List[str], cwd: Path, env: Dict[str, str], timeout_seconds: int = 1800) -> None:
    """Execute dbt command with stdout/stderr logging and error propagation."""
    logger.info("Executing: %s (cwd=%s)", " ".join(cmd), cwd)
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    if proc.stdout:
        logger.info("dbt STDOUT:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("dbt STDERR:\n%s", proc.stderr)

    if proc.returncode != 0:
        logger.error("dbt command failed with exit code %d", proc.returncode)
        raise RuntimeError(f"dbt command failed with exit code {proc.returncode}")


def main() -> None:
    """Main orchestrator for Glue dbt execution."""
    args = _parse_args()
    logger.info("Starting Customer 360 dbt runner: %s", args)

    dbt_target = args["dbt-target"]
    dbt_action = args["dbt-action"]
    run_id = args["run-id"]
    s3_dbt_path = args["s3-dbt-path"]

    # Environment variable overrides
    env = os.environ.copy()
    env["DBT_TARGET"] = dbt_target
    env["RUN_ID"] = run_id
    env["ENVIRONMENT"] = args["environment"]
    env["S3_BUCKET"] = args["s3-bucket"]
    env["ATHENA_WORKGROUP"] = args["athena-workgroup"]
    env["ICEBERG_DATABASE"] = args["iceberg-database"]
    env.setdefault("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))

    # Resolve project directory
    project_dir = _resolve_project_dir(s3_dbt_path)
    logger.info("Using dbt project directory: %s", project_dir)

    # Determine dbt executable
    dbt_bin = shutil.which("dbt")
    if dbt_bin:
        base_cmd = [dbt_bin]
    else:
        base_cmd = [sys.executable, "-m", "dbt.cli.main"]

    common_flags = [
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
        "--target", dbt_target,
        "--no-use-colors",
        "--no-partial-parse",
    ]

    try:
        # Step 1: dbt run
        if dbt_action in ("run", "both"):
            logger.info("--- Starting dbt run (target: %s, run_id: %s) ---", dbt_target, run_id)
            run_cmd = base_cmd + ["run"] + common_flags
            _execute_dbt(run_cmd, project_dir, env, timeout_seconds=1800)
            logger.info("dbt run completed successfully.")

        # Step 2: dbt test
        if dbt_action in ("test", "both"):
            logger.info("--- Starting dbt test (target: %s, run_id: %s) ---", dbt_target, run_id)
            test_cmd = base_cmd + ["test"] + common_flags
            _execute_dbt(test_cmd, project_dir, env, timeout_seconds=900)
            logger.info("dbt test completed successfully.")

        logger.info("Customer 360 dbt execution completed successfully.")
        sys.exit(0)
    except Exception as exc:
        logger.error("dbt execution failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
