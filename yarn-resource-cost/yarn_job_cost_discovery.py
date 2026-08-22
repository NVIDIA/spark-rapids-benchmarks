#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Discover benchmark applications and EMR logs for YARN cost accounting."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import shlex
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


from yarn_job_cost_eventlog import iter_eventlog_streams, iter_text_lines


APPLICATION_ID_RE = re.compile(r"application_\d+_\d+")
EVENTLOG_DIRECTORY_RE = re.compile(r"(eventlog_v2_(application_\d+_\d+))/")
EVENT_SEGMENT_RE = re.compile(r"(?:^|/)events_(?P<segment>\d+)(?:_|\.)")
CONTAINER_ID_RE = re.compile(r"container_\d+_\d+_\d+_\d+")
JOB_ID_RE = re.compile(r"(?:^|__)j0*(?P<job_id>\d+)(?:__|\Z)", re.IGNORECASE)
BENCHMARK_OBJECT_JOB_ID_RE = re.compile(r"\bj0*(?P<job_id>\d+)__", re.IGNORECASE)
EVENT_LOG_COLUMN_CANDIDATES = (
    "eventlog benchmark",
    "event log benchmark",
    "eventlog",
    "event log",
    "eventlog uri",
    "event log uri",
)


@dataclass
class SparkExecutor:
    executor_id: str
    container_id: str = ""
    total_cores: int | None = None
    resource_profile_id: int = 0


@dataclass
class EventLogApplication:
    application_id: str
    application_name: str = ""
    job_id: str = ""
    cluster_id: str = ""
    emr_release_label: str = ""
    spark_version: str = ""
    start_ms: int | None = None
    end_ms: int | None = None
    application_ended: bool = False
    configured_executor_cores: int | None = None
    task_cpus: int = 1
    successful_task_attempt_count: int = 0
    task_duration_sum_ms: int = 0
    task_duration_ms_by_executor: dict[str, int] = field(default_factory=dict)
    executors: dict[str, SparkExecutor] = field(default_factory=dict)
    event_segments: set[int] = field(default_factory=set)
    task_metric_warnings: list[str] = field(default_factory=list)
    unsupported_resource_profile_ids: set[int] = field(default_factory=set)

    def event_task_metrics_complete(self) -> bool:
        if not self.application_ended or self.task_metric_warnings:
            return False
        if not self.event_segments:
            return False
        expected = set(range(1, max(self.event_segments) + 1))
        return self.event_segments == expected

    def as_metadata(self) -> dict[str, str | float]:
        duration: str | float = ""
        if self.start_ms is not None and self.end_ms is not None:
            duration = round((self.end_ms - self.start_ms) / 1000.0, 6)
        return {
            "job id": self.job_id,
            "job name": self.application_name,
            "spark_duration_seconds": duration,
            "emr_cluster_id": self.cluster_id,
            "emr_release_label": self.emr_release_label,
            "spark_version": self.spark_version,
            "configured_spark_executor_cores": (
                self.configured_executor_cores
                if self.configured_executor_cores is not None
                else ""
            ),
            "spark_task_cpus": self.task_cpus,
        }


def aws_command(
    aws_profile: str | None, aws_region: str | None = None
) -> list[str]:
    command = ["aws"]
    if aws_profile:
        command += ["--profile", aws_profile]
    if aws_region:
        command += ["--region", aws_region]
    return command


class AwsCliError(RuntimeError):
    """An AWS CLI failure with the captured diagnostic output preserved."""


def run_aws(command: list[str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode:
        diagnostic = completed.stderr.strip() or completed.stdout.strip()
        if not diagnostic:
            diagnostic = "(AWS CLI produced no diagnostic output)"
        raise AwsCliError(
            f"AWS CLI failed with exit code {completed.returncode}:\n"
            f"{diagnostic}\n"
            f"Command: {shlex.join(command)}"
        )
    return completed


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith(("s3://", "s3a://", "s3n://")):
        raise ValueError(f"Expected an S3 URI, got {uri}")
    bucket_and_key = uri.split("://", 1)[1]
    bucket, _, key = bucket_and_key.partition("/")
    if not bucket:
        raise ValueError(f"S3 URI has no bucket: {uri}")
    return bucket, key.rstrip("/") + "/"


def event_segment_number(uri: str) -> int:
    match = EVENT_SEGMENT_RE.search(uri)
    if not match:
        raise ValueError(f"Cannot determine event segment number from {uri}")
    return int(match.group("segment"))


def list_event_log_objects(
    event_log_root: str, aws_profile: str | None, aws_region: str | None
) -> dict[str, list[str]]:
    bucket, prefix = split_s3_uri(event_log_root)
    command = aws_command(aws_profile, aws_region) + [
        "s3api",
        "list-objects-v2",
        "--bucket",
        bucket,
        "--prefix",
        prefix,
        "--output",
        "json",
    ]
    completed = run_aws(command)
    payload = json.loads(completed.stdout)
    objects: dict[str, list[str]] = defaultdict(list)
    for item in payload.get("Contents") or []:
        key = str(item.get("Key") or "")
        relative = key[len(prefix) :] if key.startswith(prefix) else key
        match = EVENTLOG_DIRECTORY_RE.match(relative)
        if match and Path(key).name.startswith("events_"):
            objects[match.group(2)].append(f"s3://{bucket}/{key}")
    if not objects:
        raise ValueError(
            f"No eventlog_v2_application_* directories found under {event_log_root}"
        )
    return {
        app_id: sorted(uris, key=event_segment_number)
        for app_id, uris in objects.items()
    }


def materialize_event_metadata_files(
    event_log_root: str,
    cache_dir: Path,
    aws_profile: str | None,
    aws_region: str | None,
) -> tuple[Path, set[str]]:
    if not event_log_root.startswith(("s3://", "s3a://", "s3n://")):
        path = Path(event_log_root).expanduser()
        if not path.exists():
            raise FileNotFoundError(path)
        application_ids: set[str] = set()
        for match in APPLICATION_ID_RE.finditer(path.as_posix()):
            application_ids.add(match.group())
        for child in path.glob("eventlog_v2_application_*"):
            match = APPLICATION_ID_RE.search(child.name)
            if match:
                application_ids.add(match.group())
        return path, application_ids

    objects = list_event_log_objects(event_log_root, aws_profile, aws_region)
    digest = hashlib.sha256(event_log_root.rstrip("/").encode()).hexdigest()[:16]
    target = cache_dir / f"event-root-{digest}"
    target.mkdir(parents=True, exist_ok=True)
    for app_id, uris in objects.items():
        for uri in uris:
            app_dir = target / f"eventlog_v2_{app_id}"
            app_dir.mkdir(parents=True, exist_ok=True)
            destination = app_dir / Path(uri).name
            if destination.is_file():
                continue
            command = aws_command(aws_profile, aws_region) + [
                "s3",
                "cp",
                uri,
                str(destination),
                "--only-show-errors",
            ]
            run_aws(command)
    return target, set(objects)


def derive_job_id(application_name: str) -> str:
    match = JOB_ID_RE.search(application_name)
    if not match:
        return ""
    job_id = int(match.group("job_id"))
    if "rewrite" in application_name.lower():
        job_id += 900000
    return str(job_id)


def benchmark_application_signature(application_name: str) -> str:
    parent = Path(application_name).parent.name.lower()
    parent = re.sub(r"^\d+_", "", parent)
    return parent.replace("__rewrite", "")


def task_end_succeeded(event: dict) -> bool:
    reason = event.get("Task End Reason")
    if isinstance(reason, dict):
        return reason.get("Reason") == "Success"
    return reason == "Success"


def integer_property(properties: dict, name: str, default: int | None) -> int | None:
    value = properties.get(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def read_event_log_metadata(path: Path) -> dict[str, EventLogApplication]:
    applications: dict[str, EventLogApplication] = {}
    by_directory: dict[str, EventLogApplication] = {}
    for app_dir, member_name, stream, compressed in iter_eventlog_streams(path):
        current = by_directory.setdefault(
            app_dir, EventLogApplication(application_id="")
        )
        segment_match = EVENT_SEGMENT_RE.search(member_name)
        if segment_match:
            current.event_segments.add(int(segment_match.group("segment")))
        for line in iter_text_lines(stream, compressed):
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            event_name = event.get("Event")
            if event_name == "SparkListenerLogStart":
                current.spark_version = str(event.get("Spark Version") or "")
            elif event_name == "SparkListenerEnvironmentUpdate":
                properties = event.get("Spark Properties") or {}
                current.cluster_id = str(properties.get("spark.emr.clusterId") or "")
                current.emr_release_label = str(
                    properties.get("spark.emr.releaseLabel") or ""
                )
                current.configured_executor_cores = integer_property(
                    properties, "spark.executor.cores", None
                )
                task_cpus = integer_property(properties, "spark.task.cpus", 1)
                if task_cpus is None or task_cpus <= 0:
                    current.task_metric_warnings.append(
                        "spark.task.cpus is missing or invalid"
                    )
                else:
                    current.task_cpus = task_cpus
            elif event_name == "SparkListenerApplicationStart":
                current.application_id = str(event.get("App ID") or "")
                current.application_name = str(event.get("App Name") or "")
                current.job_id = derive_job_id(current.application_name)
                try:
                    current.start_ms = int(event.get("Timestamp"))
                except (TypeError, ValueError):
                    pass
            elif event_name == "SparkListenerApplicationEnd":
                current.application_ended = True
                try:
                    current.end_ms = int(event.get("Timestamp"))
                except (TypeError, ValueError):
                    pass
            elif event_name == "SparkListenerExecutorAdded":
                executor_id = str(event.get("Executor ID") or "")
                info = event.get("Executor Info") or {}
                attributes = info.get("Attributes") or {}
                container_id = str(attributes.get("CONTAINER_ID") or "")
                if not container_id:
                    match = CONTAINER_ID_RE.search(line)
                    container_id = match.group() if match else ""
                try:
                    total_cores = int(info.get("Total Cores"))
                except (TypeError, ValueError):
                    total_cores = None
                try:
                    resource_profile_id = int(info.get("Resource Profile Id") or 0)
                except (TypeError, ValueError):
                    resource_profile_id = -1
                if resource_profile_id != 0:
                    current.unsupported_resource_profile_ids.add(resource_profile_id)
                if executor_id:
                    current.executors[executor_id] = SparkExecutor(
                        executor_id=executor_id,
                        container_id=container_id,
                        total_cores=total_cores,
                        resource_profile_id=resource_profile_id,
                    )
            elif event_name == "SparkListenerResourceProfileAdded":
                try:
                    profile_id = int(event.get("Resource Profile Id") or 0)
                except (TypeError, ValueError):
                    profile_id = -1
                if profile_id != 0:
                    current.unsupported_resource_profile_ids.add(profile_id)
            elif event_name == "SparkListenerStageSubmitted":
                stage_info = event.get("Stage Info") or {}
                try:
                    profile_id = int(stage_info.get("Resource Profile Id") or 0)
                except (TypeError, ValueError):
                    profile_id = -1
                if profile_id != 0:
                    current.unsupported_resource_profile_ids.add(profile_id)
            elif event_name == "SparkListenerTaskEnd" and task_end_succeeded(event):
                task_info = event.get("Task Info") or {}
                if task_info.get("Failed") or task_info.get("Killed"):
                    current.task_metric_warnings.append(
                        "A successful task event is marked failed or killed"
                    )
                    continue
                executor_id = str(task_info.get("Executor ID") or "")
                try:
                    launch_ms = int(task_info.get("Launch Time"))
                    finish_ms = int(task_info.get("Finish Time"))
                except (TypeError, ValueError):
                    current.task_metric_warnings.append(
                        "A successful task event has invalid timestamps"
                    )
                    continue
                if not executor_id or finish_ms < launch_ms:
                    current.task_metric_warnings.append(
                        "A successful task event has invalid executor or duration"
                    )
                    continue
                duration_ms = finish_ms - launch_ms
                current.successful_task_attempt_count += 1
                current.task_duration_sum_ms += duration_ms
                current.task_duration_ms_by_executor[executor_id] = (
                    current.task_duration_ms_by_executor.get(executor_id, 0)
                    + duration_ms
                )
            if not current.job_id:
                object_match = BENCHMARK_OBJECT_JOB_ID_RE.search(line)
                if object_match:
                    job_id = int(object_match.group("job_id"))
                    if "rewrite" in current.application_name.lower():
                        job_id += 900000
                    current.job_id = str(job_id)
        if current.application_id:
            applications[current.application_id] = current

    for current in applications.values():
        if not current.application_ended:
            current.task_metric_warnings.append(
                "SparkListenerApplicationEnd is missing"
            )
        if not current.event_segments:
            current.task_metric_warnings.append(
                "Event-log segment numbers are unavailable"
            )
        else:
            expected = set(range(1, max(current.event_segments) + 1))
            missing = sorted(expected - current.event_segments)
            if missing:
                current.task_metric_warnings.append(
                    "Missing event-log segments: " + ", ".join(map(str, missing))
                )
        if current.unsupported_resource_profile_ids:
            current.task_metric_warnings.append(
                "Unsupported non-default Spark resource profiles: "
                + ", ".join(
                    map(str, sorted(current.unsupported_resource_profile_ids))
                )
            )
        current.task_metric_warnings = list(
            dict.fromkeys(current.task_metric_warnings)
        )

    known_by_signature: dict[str, set[int]] = defaultdict(set)
    for application in applications.values():
        if application.job_id:
            signature = benchmark_application_signature(
                application.application_name
            )
            known_by_signature[signature].add(int(application.job_id) % 900000)
    for application in applications.values():
        candidates = known_by_signature.get(
            benchmark_application_signature(application.application_name), set()
        )
        if not application.job_id and len(candidates) == 1:
            application.job_id = str(next(iter(candidates)))
    return applications


def resolve_emr_log_uri(
    cluster_id: str, aws_profile: str | None, aws_region: str | None
) -> str:
    command = aws_command(aws_profile, aws_region) + [
        "emr",
        "describe-cluster",
        "--cluster-id",
        cluster_id,
        "--query",
        "Cluster.LogUri",
        "--output",
        "text",
    ]
    completed = run_aws(command)
    log_root = completed.stdout.strip()
    if not log_root or log_root == "None":
        raise ValueError(f"EMR cluster {cluster_id} has no LogUri")
    normalized = "s3://" + log_root.split("://", 1)[1]
    if normalized.rstrip("/").endswith("/" + cluster_id):
        return normalized.rstrip("/") + "/"
    return normalized.rstrip("/") + f"/{cluster_id}/"


def normalize_header(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.lower()).split())


def find_event_log_column(headers: Iterable[str], explicit: str | None) -> str:
    headers = list(headers)
    if explicit:
        if explicit not in headers:
            raise ValueError(f"CSV has no event-log column {explicit!r}")
        return explicit
    normalized = {normalize_header(header): header for header in headers}
    for candidate in EVENT_LOG_COLUMN_CANDIDATES:
        if candidate in normalized:
            return normalized[candidate]
    raise ValueError(
        "Could not find the event-log column. Pass --event-log-column; "
        f"available columns: {', '.join(headers)}"
    )


def row_value(row: dict[str, str], normalized_name: str) -> str:
    for key, value in row.items():
        if normalize_header(key) == normalized_name and str(value).strip():
            return str(value).strip()
    return ""


def load_csv_metadata(
    path: Path | None, event_log_column: str | None
) -> dict[str, dict[str, str]]:
    if path is None:
        return {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header")
        column = find_event_log_column(reader.fieldnames, event_log_column)
        metadata = {}
        for row in reader:
            match = APPLICATION_ID_RE.search(str(row.get(column, "")))
            if not match:
                continue
            metadata[match.group()] = {
                "job id": row_value(row, "job id"),
                "job name": row_value(row, "job name"),
            }
        return metadata
