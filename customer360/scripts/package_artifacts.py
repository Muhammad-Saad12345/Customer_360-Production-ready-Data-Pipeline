"""
Customer 360 artifact packaging
===============================
Builds everything Terraform deploys, under one version folder that mirrors the
S3 layout (s3://<artifacts-bucket>/artifacts/<version>/...):

  build/artifacts/<version>/lambdas/<function>.zip   handler + src/ + Linux wheels
  build/artifacts/<version>/glue/dbt_runner.py
  build/artifacts/<version>/dbt/dbt_project.tar.gz

Third-party packages are installed for the Lambda platform (manylinux2014
x86_64, CPython 3.11, binary wheels only), so a build on macOS or Windows still
produces zips that import on Lambda. Nothing is pip-installed at runtime.

Usage:
  python scripts/package_artifacts.py --version "$(git rev-parse --short=12 HEAD)"
  aws s3 sync build/artifacts/ s3://<bucket>/artifacts/
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = REPO_ROOT / "build"
ARTIFACTS_BUILD_DIR = BUILD_DIR / "artifacts"
DEPS_CACHE = BUILD_DIR / ".deps"

LAMBDA_PYTHON = "3.11"
LAMBDA_PLATFORM = "manylinux2014_x86_64"
LAMBDA_UNZIPPED_LIMIT = 250 * 1024 * 1024

COMMON = ["requests==2.34.2", "tenacity==9.2.1"]
# pyarrow 20 is the newest release with manylinux2014 wheels; the python3.11
# Lambda runtime is Amazon Linux 2 (glibc 2.26) and cannot load manylinux_2_28 wheels.
ARROW = ["pyarrow==20.0.0"]
# boto3 is provided by the Lambda runtime and deliberately not bundled.
FUNCTION_DEPENDENCIES = {
    "initialize": COMMON,
    "ingest": COMMON,
    "silver_gate": COMMON + ARROW,
    "quality_check": [],
    "iceberg_load": COMMON + ARROW,
    "reconcile": [],
    "notify": [],
    "iceberg_maintenance": COMMON + ARROW,
}


def _should_exclude(path: Path) -> bool:
    """Return True if path should be excluded from deployment packages."""
    excluded_parts = {"__pycache__", ".pytest_cache", ".ruff_cache", ".git", ".venv", "target", "logs", "dbt_packages", ".DS_Store", ".user.yml"}
    return path.suffix in (".pyc", ".pyo") or bool(excluded_parts.intersection(path.parts))


def _install_dependencies(requirements: list[str]) -> Path | None:
    """Install Linux wheels for *requirements* into a cached folder and return it."""
    if not requirements:
        return None
    target = DEPS_CACHE / ("_".join(sorted(r.split("==")[0] for r in requirements)))
    if target.exists():
        return target
    target.mkdir(parents=True)
    if importlib.util.find_spec("pip") is not None:
        cmd = [sys.executable, "-m", "pip", "install", "--quiet", "--no-compile",
               "--platform", LAMBDA_PLATFORM, "--implementation", "cp",
               "--python-version", LAMBDA_PYTHON, "--only-binary=:all:",
               "--target", str(target), *requirements]
    elif shutil.which("uv"):
        cmd = ["uv", "pip", "install", "--quiet", "--python-platform", "x86_64-manylinux2014",
               "--python-version", LAMBDA_PYTHON, "--only-binary", ":all:",
               "--target", str(target), *requirements]
    else:
        raise SystemExit("Neither pip nor uv is available to install Lambda dependencies")
    print(f"  installing {', '.join(requirements)} for {LAMBDA_PLATFORM}/py{LAMBDA_PYTHON}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        shutil.rmtree(target, ignore_errors=True)
        raise
    return target


def _add_tree(zf: zipfile.ZipFile, root: Path, arc_root: Path) -> int:
    size = 0
    for dirpath, dirs, files in os.walk(root):
        dir_path = Path(dirpath)
        dirs[:] = sorted(d for d in dirs if not _should_exclude(dir_path / d))
        for name in sorted(files):
            file_path = dir_path / name
            if _should_exclude(file_path):
                continue
            zf.write(file_path, arcname=str(arc_root / file_path.relative_to(root)).replace("\\", "/"))
            size += file_path.stat().st_size
    return size


def package_lambdas(out_dir: Path) -> None:
    """One zip per handler: handler.py at the root, src/ package, Linux dependencies."""
    out_dir.mkdir(parents=True, exist_ok=True)
    handlers_dir = REPO_ROOT / "lambda_handlers"
    src_dir = REPO_ROOT / "src"
    found = sorted(p.name for p in handlers_dir.iterdir() if (p / "handler.py").exists())
    unknown = set(found) - set(FUNCTION_DEPENDENCIES)
    missing = set(FUNCTION_DEPENDENCIES) - set(found)
    if unknown or missing:
        raise SystemExit(f"Handler list and FUNCTION_DEPENDENCIES differ: unknown={unknown} missing={missing}")

    for name in found:
        zip_path = out_dir / f"{name}.zip"
        deps = _install_dependencies(FUNCTION_DEPENDENCIES[name])
        unzipped = 0
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            unzipped += _add_tree(zf, handlers_dir / name, Path("."))
            unzipped += _add_tree(zf, src_dir, Path("src"))
            if deps:
                unzipped += _add_tree(zf, deps, Path("."))
        if unzipped > LAMBDA_UNZIPPED_LIMIT:
            raise SystemExit(f"{name}: unzipped size {unzipped / 1e6:.0f} MB exceeds the 250 MB Lambda limit")
        print(f"  {zip_path.name}: {zip_path.stat().st_size / 1e6:.1f} MB zipped, {unzipped / 1e6:.0f} MB unzipped")


def package_dbt(out_dir: Path) -> None:
    """Package dbt project into a tar.gz archive for Glue Python Shell execution."""
    dbt_artifacts_dir = out_dir
    dbt_artifacts_dir.mkdir(parents=True, exist_ok=True)
    dbt_dir = REPO_ROOT / "dbt"
    tar_path = dbt_artifacts_dir / "dbt_project.tar.gz"

    if not dbt_dir.exists():
        raise SystemExit(f"dbt directory not found at {dbt_dir}")

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


def package_glue_jobs(out_dir: Path) -> None:
    """Stage Glue job runner script for deployment."""
    out_dir.mkdir(parents=True, exist_ok=True)
    glue_script = REPO_ROOT / "glue_jobs" / "dbt_runner.py"
    if not glue_script.exists():
        raise SystemExit(f"Glue script not found at {glue_script}")
    shutil.copy2(glue_script, out_dir / "dbt_runner.py")
    print(f"  staged {out_dir / 'dbt_runner.py'}")


def _default_version() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short=12", "HEAD"], cwd=REPO_ROOT, check=True,
                              capture_output=True, text=True).stdout.strip()
    except Exception:  # noqa: BLE001
        raise SystemExit("Pass --version (no git checkout found)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", default=None, help="artifact version (default: short git SHA)")
    args = parser.parse_args()
    version = args.version or _default_version()

    out = ARTIFACTS_BUILD_DIR / version
    if ARTIFACTS_BUILD_DIR.exists():
        shutil.rmtree(ARTIFACTS_BUILD_DIR)
    print(f"=== Customer 360 artifacts, version {version} ===")
    package_lambdas(out / "lambdas")
    package_dbt(out / "dbt")
    package_glue_jobs(out / "glue")
    print(f"=== Done: {out} ===")


if __name__ == "__main__":
    main()
