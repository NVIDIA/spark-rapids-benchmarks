#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Build and verify the standalone YARN job cost source bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_LICENSE_FILE = PROJECT_DIR.parent / "LICENSE"
PACKAGE_SOURCES = {
    PROJECT_DIR / "yarn_resource_cost.py": "yarn_resource_cost.py",
    PROJECT_DIR / "yarn_job_cost_core.py": "yarn_job_cost_core.py",
    PROJECT_DIR / "yarn_job_cost_adapters.py": "yarn_job_cost_adapters.py",
    PROJECT_DIR / "yarn_job_cost_dataproc.py": "yarn_job_cost_dataproc.py",
    PROJECT_DIR / "calculate_yarn_job_cost.py": "calculate_yarn_job_cost.py",
    PROJECT_DIR / "yarn_job_cost_discovery.py": "yarn_job_cost_discovery.py",
    PROJECT_DIR / "yarn_job_cost_eventlog.py": "yarn_job_cost_eventlog.py",
    PROJECT_DIR / "yarn_job_cost_defaults.py": "yarn_job_cost_defaults.py",
    PROJECT_DIR / "test_calculate_yarn_job_cost.py": "test_calculate_yarn_job_cost.py",
    PROJECT_DIR / "test_portable_yarn_resource_cost.py": "test_portable_yarn_resource_cost.py",
    PROJECT_DIR / "test_fair_scheduler_policy.py": "test_fair_scheduler_policy.py",
    PROJECT_DIR / "test_dataproc_log_normalization.py": "test_dataproc_log_normalization.py",
    PROJECT_DIR / "test_dataproc_adapter.py": "test_dataproc_adapter.py",
    PROJECT_DIR / "test_portable_comparison.py": "test_portable_comparison.py",
    PROJECT_DIR / "RESOURCE_COST_MODEL.md": "RESOURCE_COST_MODEL.md",
    PROJECT_DIR / "tests/fixtures/on_prem/eventlog_v2_application_1_0001/events_1_application_1_0001": "tests/fixtures/on_prem/eventlog_v2_application_1_0001/events_1_application_1_0001",
    PROJECT_DIR / "tests/fixtures/on_prem/yarn/hadoop-yarn-resourcemanager-rm.log": "tests/fixtures/on_prem/yarn/hadoop-yarn-resourcemanager-rm.log",
    PROJECT_DIR / "tests/fixtures/on_prem/node-classes.json": "tests/fixtures/on_prem/node-classes.json",
    PROJECT_DIR / "tests/fixtures/on_prem/prices.json": "tests/fixtures/on_prem/prices.json",
    PROJECT_DIR / "README.md": "README.md",
    PROJECT_DIR / "CONTRIBUTING.md": "CONTRIBUTING.md",
}
BANNED_TEXT: tuple[str, ...] = ()


def run(command: list[str], cwd: Path | None = None) -> str:
    completed = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True
    )
    if completed.returncode:
        diagnostic = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: "
            f"{' '.join(command)}\n{diagnostic}"
        )
    return completed.stdout.strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def aggregate_sha256(paths: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def scan_bundle(root: Path) -> None:
    failures = []
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        if path.suffix not in {".py", ".md", ".json"}:
            continue
        text = path.read_text(encoding="utf-8")
        for forbidden in BANNED_TEXT:
            if forbidden.lower() in text.lower():
                failures.append(f"{path.name}: contains {forbidden!r}")
    if failures:
        raise ValueError("Bundle content scan failed:\n" + "\n".join(failures))


def validate_bundle(root: Path) -> None:
    license_path = root / "LICENSE"
    if not license_path.is_file() or license_path.stat().st_size == 0:
        raise ValueError("Bundle LICENSE is missing or empty")
    contributing_path = root / "CONTRIBUTING.md"
    if (
        not contributing_path.is_file()
        or contributing_path.stat().st_size == 0
    ):
        raise ValueError("Bundle CONTRIBUTING.md is missing or empty")
    if (
        "Developer's Certificate of Origin 1.1"
        not in contributing_path.read_text()
    ):
        raise ValueError("Bundle CONTRIBUTING.md does not contain DCO 1.1")
    run([sys.executable, "-m", "unittest", "discover", "-s", ".", "-p", "test*.py"], cwd=root)
    run([sys.executable, "yarn_resource_cost.py", "--help"], cwd=root)


def remove_bytecode(root: Path) -> None:
    for cache in root.rglob("__pycache__"):
        shutil.rmtree(cache)
    for bytecode in root.rglob("*.py[co]"):
        bytecode.unlink()


def create_archive(
    staging: Path, archive: Path, package_name: str, archive_format: str
) -> None:
    if archive_format == "zip":
        with zipfile.ZipFile(
            archive, "w", compression=zipfile.ZIP_DEFLATED
        ) as bundle:
            for path in sorted(staging.rglob("*")):
                if path.is_file():
                    arcname = Path(package_name) / path.relative_to(staging)
                    bundle.write(path, arcname=arcname)
        return
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(staging, arcname=package_name)


def extract_archive(
    archive: Path, destination: Path, archive_format: str
) -> None:
    if archive_format == "zip":
        with zipfile.ZipFile(archive, "r") as bundle:
            bundle.extractall(destination)
        return
    with tarfile.open(archive, "r:gz") as bundle:
        bundle.extractall(destination)


def build(
    output_dir: Path, license_file: Path, archive_format: str
) -> tuple[Path, Path]:
    if archive_format not in {"zip", "tar.gz"}:
        raise ValueError(f"Unsupported archive format: {archive_format}")
    if not license_file.is_file() or license_file.stat().st_size == 0:
        raise ValueError(f"License file is missing or empty: {license_file}")
    license_digest = sha256(license_file)
    source_digest = aggregate_sha256(tuple(PACKAGE_SOURCES))
    package_name = (
        f"yarn-job-cost-{source_digest[:12]}-{license_digest[:12]}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    extension = ".zip" if archive_format == "zip" else ".tar.gz"
    archive = output_dir / f"{package_name}{extension}"

    with tempfile.TemporaryDirectory() as temporary:
        staging = Path(temporary) / package_name
        staging.mkdir()
        for source, destination in PACKAGE_SOURCES.items():
            target = staging / destination
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        shutil.copy2(license_file, staging / "LICENSE")
        scan_bundle(staging)
        validate_bundle(staging)
        remove_bytecode(staging)

        files = {
            path.relative_to(staging).as_posix(): sha256(path)
            for path in sorted(staging.rglob("*"))
            if path.is_file()
        }
        manifest = {
            "package": "yarn-resource-cost",
            "archive_format": archive_format,
            "source_sha256": source_digest,
            "built_at_utc": datetime.now(timezone.utc).isoformat(),
            "python_requires": ">=3.10",
            "license_file": "LICENSE",
            "license_sha256": license_digest,
            "files": files,
        }
        (staging / "PACKAGE-MANIFEST.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        create_archive(staging, archive, package_name, archive_format)

    checksum = archive.with_suffix(archive.suffix + ".sha256")
    checksum.write_text(f"{sha256(archive)}  {archive.name}\n", encoding="utf-8")

    with tempfile.TemporaryDirectory() as temporary:
        extracted = Path(temporary)
        extract_archive(archive, extracted, archive_format)
        validate_bundle(extracted / package_name)
    return archive, checksum


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--license-file",
        type=Path,
        default=DEFAULT_LICENSE_FILE,
        help=(
            "Approved license text to embed verbatim as LICENSE "
            f"(default: {DEFAULT_LICENSE_FILE})"
        ),
    )
    parser.add_argument(
        "--archive-format",
        choices=("zip", "tar.gz"),
        default="zip",
        help="Archive format (default: zip for Slack compatibility)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_DIR / "dist",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        archive, checksum = build(
            args.output_dir, args.license_file, args.archive_format
        )
    except (RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(archive)
    print(checksum)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
