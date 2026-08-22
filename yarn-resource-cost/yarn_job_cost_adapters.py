#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Platform adapters for discovery, node classification, and pricing inputs."""

from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from yarn_job_cost_core import YarnEvidence, relevant_log_files


class AdapterCommandError(RuntimeError):
    """External adapter command failure with its diagnostic text preserved."""


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode:
        diagnostic = completed.stderr.strip() or completed.stdout.strip()
        raise AdapterCommandError(
            f"Command failed with exit code {completed.returncode}:\n"
            f"{diagnostic or '(no diagnostic output)'}\n"
            f"Command: {shlex.join(command)}"
        )
    return completed


def _safe_archive_member(destination: Path, member_name: str) -> Path:
    member = (destination / member_name).resolve()
    if destination.resolve() not in member.parents and member != destination.resolve():
        raise ValueError(f"Archive member escapes destination: {member_name}")
    return member


def extract_archive(path: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                _safe_archive_member(destination, info.filename)
            archive.extractall(destination)
    elif tarfile.is_tarfile(path):
        with tarfile.open(path) as archive:
            members = archive.getmembers()
            for member in members:
                _safe_archive_member(destination, member.name)
                if member.issym() or member.islnk():
                    raise ValueError(f"Archive links are not accepted: {member.name}")
            archive.extractall(destination, members=members)
    else:
        raise ValueError(f"Unsupported log archive: {path}")
    return destination


def materialize_local_or_archive(uri: str, cache_dir: Path) -> Path:
    path = Path(uri).expanduser()
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        return path
    digest = hashlib.sha256(str(path.resolve()).encode()).hexdigest()[:16]
    target = cache_dir / f"archive-{digest}"
    if not target.exists():
        extract_archive(path, target)
    return target


def materialize_hdfs(uri: str, cache_dir: Path, refresh: bool) -> Path:
    digest = hashlib.sha256(uri.encode()).hexdigest()[:16]
    target = cache_dir / f"hdfs-{digest}"
    marker = target / ".download-complete"
    if marker.exists() and not refresh:
        return target
    target.mkdir(parents=True, exist_ok=True)
    run_command(["hdfs", "dfs", "-copyToLocal", "-f", uri, str(target)])
    marker.write_text(uri + "\n", encoding="utf-8")
    return target


def materialize_gcs(uri: str, cache_dir: Path, refresh: bool) -> Path:
    digest = hashlib.sha256(uri.rstrip("/").encode()).hexdigest()[:16]
    target = cache_dir / f"gcs-{digest}"
    marker = target / ".download-complete"
    if marker.exists() and not refresh:
        return target
    target.mkdir(parents=True, exist_ok=True)
    run_command(
        ["gcloud", "storage", "cp", "--recursive", uri.rstrip("/") + "/*", str(target)]
    )
    marker.write_text(uri + "\n", encoding="utf-8")
    return target


def materialize_s3(
    uri: str, cache_dir: Path, refresh: bool, aws_profile: str | None
) -> Path:
    normalized = "s3://" + uri.split("://", 1)[1]
    digest = hashlib.sha256(normalized.rstrip("/").encode()).hexdigest()[:16]
    target = cache_dir / f"s3-{digest}"
    marker = target / ".download-complete"
    if marker.exists() and not refresh:
        return target
    target.mkdir(parents=True, exist_ok=True)
    command = ["aws"]
    if aws_profile:
        command += ["--profile", aws_profile]
    command += [
        "s3", "cp", "--recursive", normalized.rstrip("/") + "/", str(target),
        "--exclude", "*", "--include", "*hadoop-yarn-nodemanager*.log*",
        "--include", "*hadoop-yarn-resourcemanager*.log*",
    ]
    run_command(command)
    marker.write_text(uri + "\n", encoding="utf-8")
    return target


def materialize_yarn_logs(
    adapter: str,
    uri: str,
    cache_dir: Path,
    refresh: bool = False,
    aws_profile: str | None = None,
) -> Path:
    if uri.startswith(("s3://", "s3a://", "s3n://")):
        if adapter != "emr":
            raise ValueError("S3 YARN log discovery is only provided by the EMR adapter")
        result = materialize_s3(uri, cache_dir, refresh, aws_profile)
    elif uri.startswith("gs://"):
        if adapter != "dataproc":
            raise ValueError("gs:// YARN logs require --adapter dataproc")
        result = materialize_gcs(uri, cache_dir, refresh)
    elif uri.startswith("hdfs://"):
        if adapter != "on-prem":
            raise ValueError("HDFS YARN logs require --adapter on-prem")
        result = materialize_hdfs(uri, cache_dir, refresh)
    else:
        result = materialize_local_or_archive(uri, cache_dir)
    if not relevant_log_files(result):
        raise ValueError(f"No ResourceManager or NodeManager logs found in {uri}")
    return result


@dataclass(frozen=True)
class PriceCatalog:
    currency: str
    rates_per_hour: dict[str, float]
    provenance: dict[str, object]


def load_price_catalog(path: Path) -> PriceCatalog:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("Price catalog schema_version must be 1")
    rates: dict[str, float] = {}
    for entry in payload.get("rates") or []:
        node_class = str(entry.get("node_class") or "")
        hourly = float(entry.get("hourly_rate"))
        if not node_class or hourly < 0:
            raise ValueError("Each price entry needs node_class and nonnegative hourly_rate")
        rates[node_class] = hourly
    return PriceCatalog(
        currency=str(payload.get("currency") or "USD"),
        rates_per_hour=rates,
        provenance={
            "mode": "catalog",
            "source": payload.get("source") or str(path),
            "effective_at": payload.get("effective_at") or "",
            "loaded_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def apply_node_class_map(evidence: YarnEvidence, path: Path | None) -> None:
    if path is None:
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    mappings = payload.get("nodes") or {}
    default = str(payload.get("default_node_class") or "")
    for node_id, node in evidence.nodes.items():
        mapped = str(mappings.get(node_id) or "")
        if mapped:
            node.instance_type = mapped
        elif not node.instance_type and default:
            node.instance_type = default


def apply_catalog_costs(applications: list[dict], catalog: PriceCatalog) -> None:
    for application in applications:
        application["worker_cost_currency"] = catalog.currency
        if application.get("complete") is not True:
            application["worker_cost"] = ""
            continue
        seconds_by_class = application["node_equivalent_seconds_by_instance_type"]
        missing = sorted(set(seconds_by_class) - set(catalog.rates_per_hour))
        if missing:
            application["complete"] = False
            application["worker_cost"] = ""
            application["warnings"].append(
                "Price catalog has no rate for node class(es): " + ", ".join(missing)
            )
            continue
        application["worker_cost"] = round(
            sum(
                float(seconds) * catalog.rates_per_hour[node_class] / 3600.0
                for node_class, seconds in seconds_by_class.items()
            ),
            8,
        )


ADAPTERS = ("emr", "dataproc", "on-prem")
