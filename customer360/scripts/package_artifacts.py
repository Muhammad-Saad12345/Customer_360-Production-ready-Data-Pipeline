"""
Customer 360 Artifact Packaging Utility
=======================================
Packages Lambda functions, Glue runner scripts, and dbt project bundles
for serverless AWS deployment in CI/CD and local environments.

Outputs:
  - build/lambdas/<function_name>.zip (handler + src/ library)
  - build/artifacts/dbt/dbt_project.tar.gz (dbt models, macros, project config)
  - build/glue_jobs/dbt_runner.py (Glue Python Shell runner script)
"""
from __future__ import annotations

import os
import shutil
import sys
import tarfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = REPO_ROOT / "build"
LAMBDAS_BUILD_DIR = BUILD_DIR / "lambdas"
ARTIFACTS_BUILD_DIR = BUILD_DIR / "artifacts"
GLUE_BUILD_DIR = BUILD_DIR / "glue_jobs"


def _should_exclude(path: Path) -> bool:
    """Return True if path should be excluded from deployment packages."""
    exclude_patterns = [
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        "*.pyc",
        "*.pyo",
        ".git",
        ".venv",
        "target",
        "logs",
        "dbt_packages",
        ".DS_Store",
    ]
    for pattern in exclude_patterns:
        if pattern.startswith("*.") and path.suffix == pattern[1:]:
            return True
        if pattern in path.parts:
            return True
    return False


def package_lambdas() -> None:
    """Package each Lambda handler and include src/ for shared utilities."""
    LAMBDAS_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    handlers_dir = REPO_ROOT / "lambda_handlers"
    src_dir = REPO_ROOT / "src"

    if not handlers_dir.exists():
        print(f"Error: handlers directory not found at {handlers_dir}")
        sys.exit(1)

    for item in sorted(handlers_dir.iterdir()):
        if not item.is_dir():
            continue

        handler_py = item / "handler.py"
        if not handler_py.exists():
            continue

        zip_path = LAMBDAS_BUILD_DIR / f"{item.name}.zip"
        print(f"Packaging Lambda: {item.name} -> {zip_path.name}")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # Add handler.py at root
            zf.write(handler_py, arcname="handler.py")

            # Add any extra files in the handler directory
            for f in item.iterdir():
                if f.is_file() and f.name != "handler.py" and not _should_exclude(f):
                    zf.write(f, arcname=f.name)

            # Add src/ tree for pipeline dependencies
            if src_dir.exists():
                for root, dirs, files in os.walk(src_dir):
                    root_path = Path(root)
                    dirs[:] = [d for d in dirs if not _should_exclude(root_path / d)]
                    for f in files:
                        file_path = root_path / f
                        if not _should_exclude(file_path):
                            rel_path = file_path.relative_to(REPO_ROOT)
                            zf.write(file_path, arcname=str(rel_path).replace("\\", "/"))

        size_kb = zip_path.stat().st_size / 1024
        print(f"  Created {zip_path.name}: {size_kb:.1f} KB")


def package_dbt() -> None:
    """Package dbt project into a tar.gz archive for Glue Python Shell execution."""
    dbt_artifacts_dir = ARTIFACTS_BUILD_DIR / "dbt"
    dbt_artifacts_dir.mkdir(parents=True, exist_ok=True)
    dbt_dir = REPO_ROOT / "dbt"
    tar_path = dbt_artifacts_dir / "dbt_project.tar.gz"

    if not dbt_dir.exists():
        print(f"Warning: dbt directory not found at {dbt_dir}")
        return

    print(f"Packaging dbt project: {dbt_dir} -> {tar_path.name}")
    with tarfile.open(tar_path, "w:gz") as tar:
        for root, dirs, files in os.walk(dbt_dir):
            root_path = Path(root)
            dirs[:] = [d for d in dirs if not _should_exclude(root_path / d)]
            for f in files:
                file_path = root_path / f
                if not _should_exclude(file_path):
                    rel_path = file_path.relative_to(dbt_dir)
                    tar.add(file_path, arcname=str(rel_path).replace("\\", "/"))

    size_kb = tar_path.stat().st_size / 1024
    print(f"  Created {tar_path.name}: {size_kb:.1f} KB")


def package_glue_jobs() -> None:
    """Stage Glue job runner script for deployment."""
    GLUE_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    glue_script = REPO_ROOT / "glue_jobs" / "dbt_runner.py"

    if glue_script.exists():
        dest = GLUE_BUILD_DIR / "dbt_runner.py"
        shutil.copy2(glue_script, dest)
        print(f"Staged Glue runner script: {dest.name}")
    else:
        print(f"Warning: Glue script not found at {glue_script}")


def main() -> None:
    print("=== Customer 360 Artifact Packaging ===")
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    package_lambdas()
    package_dbt()
    package_glue_jobs()
    print("=== Packaging Complete ===")


if __name__ == "__main__":
    main()
