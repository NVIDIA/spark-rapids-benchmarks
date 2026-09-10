#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Typed library API for application-scoped YARN resource accounting."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

import calculate_yarn_job_cost as reporting
from yarn_job_cost_core import calculator_mode, parse_yarn_logs
from yarn_job_cost_discovery import read_event_log_metadata


@dataclass(frozen=True)
class EmrApplicationUsageRequest:
    """Inputs needed to attribute one EMR YARN application."""

    cluster_id: str
    application_id: str
    event_log_uri: str
    region: str
    include_application_master: bool = False

    def __post_init__(self) -> None:
        for name in ("cluster_id", "application_id", "event_log_uri", "region"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} is required")


@dataclass(frozen=True)
class YarnApplicationUsageResult:
    """Resource usage and evidence for one YARN application."""

    application_id: str
    complete: bool
    retryable: bool
    resource_calculator: str = ""
    detected_resource_calculator_class: str = ""
    vcore_seconds: float | None = None
    memory_mb_seconds: float | None = None
    instance_seconds_by_type: dict[str, float] = field(default_factory=dict)
    container_count: int = 0
    expected_container_count: int | None = None
    incomplete_container_count: int = 0
    warnings: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-safe representation."""

        return asdict(self)


def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith(("s3://", "s3a://", "s3n://")):
        raise ValueError(f"Expected an S3 URI, got {uri}")
    bucket_and_key = uri.split("://", 1)[1]
    bucket, separator, key = bucket_and_key.partition("/")
    if not bucket:
        raise ValueError(f"S3 URI has no bucket: {uri}")
    return bucket, key if separator else ""


def _list_s3_objects(s3_client: Any, bucket: str, prefix: str) -> Iterable[dict[str, Any]]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents") or ()


def _safe_relative_key(key: str, prefix: str) -> Path:
    relative = key[len(prefix) :].lstrip("/") if key.startswith(prefix) else key
    parts = [part for part in Path(relative).parts if part not in ("", ".", "..")]
    if not parts:
        parts = [Path(key).name or "download"]
    return Path(*parts)


def _download_objects(
    s3_client: Any,
    bucket: str,
    prefix: str,
    objects: Iterable[dict[str, Any]],
    destination: Path,
) -> list[Path]:
    downloaded = []
    for item in objects:
        key = str(item.get("Key") or "")
        if not key or key.endswith("/"):
            continue
        target = destination / _safe_relative_key(key, prefix)
        target.parent.mkdir(parents=True, exist_ok=True)
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            with target.open("wb") as output:
                shutil.copyfileobj(body, output)
        finally:
            close = getattr(body, "close", None)
            if close:
                close()
        downloaded.append(target)
    return downloaded


def _materialize_event_log(s3_client: Any, uri: str, destination: Path) -> Path:
    if not uri.startswith(("s3://", "s3a://", "s3n://")):
        path = Path(uri).expanduser()
        if not path.exists():
            raise ValueError(f"Event log does not exist: {uri}")
        return path

    bucket, key = _split_s3_uri(uri)
    prefix = key.rstrip("/")
    objects = list(_list_s3_objects(s3_client, bucket, prefix))
    exact_object = next(
        (
            item
            for item in objects
            if str(item.get("Key") or "") == key and not key.endswith("/")
        ),
        None,
    )
    if exact_object is not None:
        downloaded = _download_objects(
            s3_client, bucket, prefix, (exact_object,), destination
        )
        return downloaded[0] if downloaded else destination
    selected = [
        item
        for item in objects
        if Path(str(item.get("Key") or "")).name.startswith("events_")
        or str(item.get("Key") or "") == key
    ]
    if not selected:
        return destination
    prefix_name = Path(prefix).name
    event_destination = (
        destination / prefix_name if prefix_name.startswith("eventlog_") else destination
    )
    _download_objects(s3_client, bucket, prefix, selected, event_destination)
    return destination


def _cluster_log_uri(emr_client: Any, cluster_id: str) -> str:
    cluster = emr_client.describe_cluster(ClusterId=cluster_id).get("Cluster") or {}
    log_uri = str(cluster.get("LogUri") or "").strip()
    if not log_uri:
        raise ValueError(f"EMR cluster {cluster_id} has no LogUri")
    _scheme, separator, bucket_and_key = log_uri.partition("://")
    if not separator or not bucket_and_key:
        raise ValueError(f"EMR cluster {cluster_id} has an invalid LogUri")
    normalized = "s3://" + bucket_and_key
    if normalized.rstrip("/").endswith("/" + cluster_id):
        return normalized.rstrip("/") + "/"
    return normalized.rstrip("/") + f"/{cluster_id}/"


def _materialize_yarn_logs(
    emr_client: Any,
    s3_client: Any,
    cluster_id: str,
    destination: Path,
) -> list[Path]:
    bucket, prefix = _split_s3_uri(_cluster_log_uri(emr_client, cluster_id))
    objects = [
        item
        for item in _list_s3_objects(s3_client, bucket, prefix)
        if any(
            marker in Path(str(item.get("Key") or "")).name
            for marker in ("hadoop-yarn-resourcemanager", "hadoop-yarn-nodemanager")
        )
    ]
    return _download_objects(s3_client, bucket, prefix, objects, destination)


def _empty_result(
    request: EmrApplicationUsageRequest,
    warning: str,
    *,
    retryable: bool,
    detected_calculator: str = "",
) -> YarnApplicationUsageResult:
    return YarnApplicationUsageResult(
        application_id=request.application_id,
        complete=False,
        retryable=retryable,
        detected_resource_calculator_class=detected_calculator,
        warnings=(warning,),
    )


def calculate_emr_application_usage(
    request: EmrApplicationUsageRequest,
    *,
    emr_client: Any,
    s3_client: Any,
) -> YarnApplicationUsageResult:
    """Calculate YARN resource usage for one EMR application.

    Missing or not-yet-complete archived logs are returned as retryable incomplete
    results. Provider authentication and transport errors are allowed to propagate.
    """

    with tempfile.TemporaryDirectory(prefix="yarn-resource-cost-") as directory:
        root = Path(directory)
        event_root = _materialize_event_log(s3_client, request.event_log_uri, root / "events")
        event_metadata = read_event_log_metadata(event_root)
        metadata = event_metadata.get(request.application_id)
        if metadata is None:
            return _empty_result(
                request,
                f"Application {request.application_id} is missing from the Spark event log",
                retryable=True,
            )

        yarn_root = root / "yarn"
        downloaded = _materialize_yarn_logs(emr_client, s3_client, request.cluster_id, yarn_root)
        if not downloaded:
            return _empty_result(
                request,
                f"No archived ResourceManager or NodeManager logs found for cluster {request.cluster_id}",
                retryable=True,
            )

        try:
            evidence = parse_yarn_logs(yarn_root)
        except ValueError as error:
            return _empty_result(request, str(error), retryable=False)
        try:
            mode = calculator_mode(evidence.calculator_class)
        except ValueError as error:
            return _empty_result(
                request,
                str(error),
                retryable=not bool(evidence.calculator_class),
                detected_calculator=evidence.calculator_class,
            )

        executor_containers = {
            executor.container_id
            for executor in metadata.executors.values()
            if executor.container_id
        }
        applications = reporting.calculate_applications(
            evidence,
            mode,
            {request.application_id: metadata.as_metadata()},
            request.include_application_master,
            executor_containers,
        )
        application = next(
            (item for item in applications if item["application_id"] == request.application_id),
            None,
        )
        if application is None:
            return _empty_result(
                request,
                f"Application {request.application_id} is missing from archived YARN logs",
                retryable=True,
                detected_calculator=evidence.calculator_class,
            )

        expected = application.get("expected_total_allocated_containers")
        return YarnApplicationUsageResult(
            application_id=request.application_id,
            complete=bool(application["complete"]),
            retryable=bool(application["retryable"]),
            resource_calculator=mode,
            detected_resource_calculator_class=evidence.calculator_class,
            vcore_seconds=float(application["vcore_seconds"]),
            memory_mb_seconds=float(application["memory_mb_seconds"]),
            instance_seconds_by_type={
                str(instance_type): float(seconds)
                for instance_type, seconds in application[
                    "node_equivalent_seconds_by_instance_type"
                ].items()
            },
            container_count=int(application["container_count"]),
            expected_container_count=int(expected) if expected != "" else None,
            incomplete_container_count=int(application["incomplete_container_count"]),
            warnings=tuple(str(warning) for warning in application["warnings"]),
        )


__all__ = [
    "EmrApplicationUsageRequest",
    "YarnApplicationUsageResult",
    "calculate_emr_application_usage",
]
