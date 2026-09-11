#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Calculate per-application cost from a Spark event-log run and EMR YARN logs.

The preferred input is one Spark event-log root, for example:

  s3://bucket/experiments/RUN/spark-events/benchmark/

The event logs identify the selected applications and EMR cluster. The EMR API
then resolves the cluster archived log URI. ResourceManager logs remain the
authoritative source for container accounting. An exact EMR log URI is also
accepted for offline or diagnostic use.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from yarn_job_cost_discovery import (  # noqa: E402
    AwsCliError,
    EventLogApplication,
    load_csv_metadata,
    materialize_event_metadata_files,
    read_event_log_metadata,
    resolve_emr_log_uri,
    run_aws,
)
from yarn_job_cost_defaults import (  # noqa: E402
    DEFAULT_AWS_PROFILE,
    DEFAULT_AWS_REGION,
)

from yarn_job_cost_adapters import (  # noqa: E402
    ADAPTERS,
    AdapterCommandError,
    apply_catalog_costs,
    apply_node_class_map,
    load_price_catalog,
    materialize_gcs,
    materialize_hdfs,
    materialize_yarn_logs,
)

TIMESTAMP = r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})"
CONTAINER = r"(?P<container>container_\d+_\d+_\d+_\d+)"
START_RE = re.compile(
    rf"^{TIMESTAMP} .*Start request for {CONTAINER} .* resource "
    r"<memory:(?P<memory>\d+)(?:, max memory:(?P<max_memory>\d+))?, "
    r"vCores:(?P<vcores>\d+)(?:, max vCores:(?P<max_vcores>\d+))?"
    r"(?P<resources>[^>]*)>"
)
DONE_RE = re.compile(rf"^{TIMESTAMP} .*Container {CONTAINER} transitioned from .* to DONE\b")
RM_ASSIGN_RE = re.compile(
    rf"^{TIMESTAMP} .*Assigned container {CONTAINER} of capacity "
    r"<memory:(?P<memory>\d+)(?:, max memory:(?P<max_memory>\d+))?, "
    r"vCores:(?P<vcores>\d+)(?:, max vCores:(?P<max_vcores>\d+))?"
    r"(?P<resources>[^>]*)> "
    r"on host (?P<host>[^:,\s]+):\d+"
)
RM_TERMINAL_RE = re.compile(
    rf"^{TIMESTAMP} .*{CONTAINER} Container Transitioned from .* to (?:COMPLETED|RELEASED|KILLED|EXPIRED)\b"
)
APPLICATION_SUMMARY_RE = re.compile(
    r"appId=(?P<application>application_\d+_\d+),name=(?P<name>.*?),user=.*?"
    r"finalStatus=(?P<final_status>[^,]+).*?totalAllocatedContainers=(?P<containers>\d+)"
)
NODE_RE = re.compile(
    r"Registered with ResourceManager .* total resource of "
    r"<memory:(?P<memory>\d+), vCores:(?P<vcores>\d+)(?P<resources>[^>]*)>.*"
    r"instanceType\(STRING\)=(?P<instance_type>[^}\]]+)"
)
RM_NODE_RE = re.compile(
    r"NodeManager from node (?P<host>[^(]+)\(cmPort: \d+ httpPort: \d+\) "
    r"registered with capability: <memory:(?P<memory>\d+), "
    r"vCores:(?P<vcores>\d+)(?P<resources>[^>]*)>.*"
    r"instanceType\(STRING\)=(?P<instance_type>[^}\]\s]+)"
)
GPU_RESOURCE_RE = re.compile(r"(?:^|,\s*)yarn\.io/gpu:\s*(?P<gpus>\d+)")
CALCULATOR_RE = re.compile(
    r"Initialized CapacityScheduler with calculator=class "
    r"org\.apache\.hadoop\.yarn\.util\.resource\.(?P<calculator>\w+)"
)
NODE_ID_RE = re.compile(r"(?:^|/)node/(?P<node_id>i-[^/]+)/")
CONTAINER_PARTS_RE = re.compile(
    r"container_(?P<cluster>\d+)_(?P<application>\d+)_(?P<attempt>\d+)_(?P<sequence>\d+)"
)
OUTPUT_FIELDS = (
    "job id",
    "job name",
    "application_id",
    "application_name",
    "spark_duration_seconds",
    "emr_cluster_id",
    "emr_release_label",
    "spark_version",
    "configured_spark_executor_cores",
    "spark_task_cpus",
    "final_status",
    "resource_calculator",
    "container_count",
    "expected_total_allocated_containers",
    "observed_total_allocated_containers",
    "incomplete_container_count",
    "nodemanager_start_fallback_container_count",
    "nodemanager_finish_fallback_container_count",
    "container_seconds",
    "memory_mb_seconds",
    "vcore_seconds",
    "gpu_seconds",
    "node_equivalent_seconds",
    "instance_vcore_seconds",
    "instance_vcore_seconds_expression",
    "task_metrics_complete",
    "perfect_packing_complete",
    "successful_task_attempt_count",
    "task_duration_sum_seconds",
    "perfect_packing_node_seconds",
    "perfect_packing_cost_expression",
    "perfect_packing_ec2_ondemand_usd",
    "perfect_packing_emr_usd",
    "perfect_packing_ec2_plus_emr_usd",
    "packing_efficiency",
    "actual_to_perfect_cost_factor",
    "actual_to_perfect_cost_overhead_percent",
    "cost_expression",
    "ec2_ondemand_usd",
    "emr_usd",
    "ec2_plus_emr_usd",
    "first_container_start_utc",
    "last_container_finish_utc",
    "complete",
    "task_metric_warnings",
    "warnings",
)


CONSOLE_FIELDS = (
    ("Job ID", "job id"),
    ("Application", "application_id"),
    ("Status", "final_status"),
    ("Spark sec", "spark_duration_seconds"),
    ("Containers", "container_count"),
    ("GPU sec", "gpu_seconds"),
    ("Node-equivalent sec", "node_equivalent_seconds"),
    ("Instance-vcore", "instance_vcore_seconds_expression"),
    ("Task sec", "task_duration_sum_seconds"),
    ("Perfect USD", "perfect_packing_ec2_plus_emr_usd"),
    ("Packing", "packing_efficiency"),
    ("Actual/perfect", "actual_to_perfect_cost_factor"),
    ("Cost", "cost_expression"),
    ("EC2+EMR USD", "ec2_plus_emr_usd"),
    ("Complete", "complete"),
    ("Warnings", "warnings"),
)


COMPARISON_FIELDS = (
    "job_id",
    "baseline_application_id",
    "test_application_id",
    "baseline_instance_type",
    "test_instance_type",
    "baseline_event_log_root",
    "test_event_log_root",
    "baseline_final_status",
    "test_final_status",
    "baseline_complete",
    "test_complete",
    "baseline_warnings",
    "test_warnings",
    "baseline_wall_clock_seconds",
    "test_wall_clock_seconds",
    "wall_clock_factor",
    "wall_clock_delta_seconds",
    "baseline_task_metrics_complete",
    "test_task_metrics_complete",
    "baseline_perfect_packing_complete",
    "test_perfect_packing_complete",
    "baseline_task_duration_sum_seconds",
    "test_task_duration_sum_seconds",
    "task_duration_factor",
    "baseline_perfect_packing_node_seconds",
    "test_perfect_packing_node_seconds",
    "baseline_perfect_packing_ec2_plus_emr_usd",
    "test_perfect_packing_ec2_plus_emr_usd",
    "perfect_packing_cost_factor",
    "perfect_packing_cost_delta_usd",
    "baseline_packing_efficiency",
    "test_packing_efficiency",
    "packing_efficiency_delta",
    "baseline_actual_to_perfect_cost_factor",
    "test_actual_to_perfect_cost_factor",
    "baseline_actual_to_perfect_cost_overhead_percent",
    "test_actual_to_perfect_cost_overhead_percent",
    "baseline_node_equivalent_seconds",
    "test_node_equivalent_seconds",
    "baseline_instance_vcore_seconds",
    "test_instance_vcore_seconds",
    "instance_vcore_seconds_factor",
    "instance_vcore_seconds_delta",
    "node_equivalent_delta_seconds",
    "baseline_ec2_ondemand_usd",
    "test_ec2_ondemand_usd",
    "ec2_ondemand_cost_factor",
    "ec2_ondemand_delta_usd",
    "baseline_emr_usd",
    "test_emr_usd",
    "baseline_ec2_plus_emr_usd",
    "test_ec2_plus_emr_usd",
    "ec2_plus_emr_cost_factor",
    "ec2_plus_emr_delta_usd",
)
COMPARISON_CONSOLE_FIELDS = (
    ("Job ID", "job_id"),
    ("Base instance", "baseline_instance_type"),
    ("Test instance", "test_instance_type"),
    ("Base complete", "baseline_complete"),
    ("Test complete", "test_complete"),
    ("Base sec", "baseline_wall_clock_seconds"),
    ("Test sec", "test_wall_clock_seconds"),
    ("Wall factor", "wall_clock_factor"),
    ("Wall delta", "wall_clock_delta_seconds"),
    ("Task factor", "task_duration_factor"),
    ("Perfect factor", "perfect_packing_cost_factor"),
    ("Base packing", "baseline_packing_efficiency"),
    ("Test packing", "test_packing_efficiency"),
    ("Base actual/perfect", "baseline_actual_to_perfect_cost_factor"),
    ("Test actual/perfect", "test_actual_to_perfect_cost_factor"),
    ("Base node-sec", "baseline_node_equivalent_seconds"),
    ("Test node-sec", "test_node_equivalent_seconds"),
    ("Node-sec delta", "node_equivalent_delta_seconds"),
    ("Base instance-vcore sec", "baseline_instance_vcore_seconds"),
    ("Test instance-vcore sec", "test_instance_vcore_seconds"),
    ("Instance-vcore factor", "instance_vcore_seconds_factor"),
    ("Base EC2+EMR USD", "baseline_ec2_plus_emr_usd"),
    ("Test EC2+EMR USD", "test_ec2_plus_emr_usd"),
    ("Cost factor", "ec2_plus_emr_cost_factor"),
    ("USD delta", "ec2_plus_emr_delta_usd"),
)


@dataclass
class Node:
    node_id: str
    instance_type: str = ""
    memory_mb: int | None = None
    vcores: int | None = None
    gpus: int = 0


@dataclass
class Container:
    container_id: str
    application_id: str
    node_id: str
    start_ms: int
    memory_mb: int
    node_memory_mb: int
    vcores: int
    node_vcores: int
    gpus: int = 0
    node_gpus: int = 0
    finish_ms: int | None = None
    finish_source: str = ""
    source: str = "nodemanager"

    @property
    def sequence(self) -> int:
        match = CONTAINER_PARTS_RE.fullmatch(self.container_id)
        if not match:
            raise ValueError(f"Unexpected container ID {self.container_id}")
        return int(match.group("sequence"))


@dataclass
class ApplicationSummary:
    application_id: str
    name: str
    final_status: str
    total_allocated_containers: int




@dataclass
class YarnEvidence:
    nodes: dict[str, Node] = field(default_factory=dict)
    containers: dict[str, Container] = field(default_factory=dict)
    calculator_class: str = ""
    application_summaries: dict[str, ApplicationSummary] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def gpu_amount(resources: str | None) -> int:
    match = GPU_RESOURCE_RE.search(resources or "")
    return int(match.group("gpus")) if match else 0


def capacity(value: str | None, fallback: int | None, name: str) -> int:
    if value is not None:
        return int(value)
    if fallback is not None:
        return fallback
    raise ValueError(f"Could not determine node {name} capacity")


def parse_timestamp(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def iso_utc(timestamp_ms: int | None) -> str:
    if timestamp_ms is None:
        return ""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def application_id(container_id: str) -> str:
    match = CONTAINER_PARTS_RE.fullmatch(container_id)
    if not match:
        raise ValueError(f"Unexpected container ID {container_id}")
    return f"application_{match.group('cluster')}_{match.group('application')}"


def node_id_from_path(path: Path) -> str:
    match = NODE_ID_RE.search(path.as_posix())
    return match.group("node_id") if match else path.parent.name


def open_log(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, mode="rt", encoding="utf-8", errors="replace")
    return path.open(encoding="utf-8", errors="replace")


def relevant_log_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(
        file
        for file in path.rglob("*")
        if file.is_file()
        and (
            "hadoop-yarn-nodemanager" in file.name
            or "hadoop-yarn-resourcemanager" in file.name
        )
        and ".log" in file.name
    )


def parse_yarn_logs(path: Path) -> YarnEvidence:
    evidence = YarnEvidence()
    rm_finishes: dict[str, int] = {}
    nm_finishes: dict[str, int] = {}
    files = relevant_log_files(path)
    if not files:
        raise ValueError(f"No NodeManager or ResourceManager log files found under {path}")

    for file in files:
        node_id = node_id_from_path(file)
        node = evidence.nodes.setdefault(node_id, Node(node_id=node_id))
        with open_log(file) as handle:
            for line in handle:
                if "ApplicationSummary" in line and "totalAllocatedContainers=" in line:
                    match = APPLICATION_SUMMARY_RE.search(line)
                    if match:
                        app_id = match.group("application")
                        evidence.application_summaries[app_id] = ApplicationSummary(
                            application_id=app_id,
                            name=match.group("name"),
                            final_status=match.group("final_status"),
                            total_allocated_containers=int(match.group("containers")),
                        )
                    continue
                if "Initialized CapacityScheduler with calculator=" in line:
                    match = CALCULATOR_RE.search(line)
                    if match:
                        calculator = match.group("calculator")
                        if evidence.calculator_class and evidence.calculator_class != calculator:
                            raise ValueError(
                                f"Conflicting ResourceCalculators: "
                                f"{evidence.calculator_class}, {calculator}"
                            )
                        evidence.calculator_class = calculator
                    continue
                if "Registered with ResourceManager" in line:
                    match = NODE_RE.search(line)
                    if match:
                        node.instance_type = match.group("instance_type").strip()
                        node.memory_mb = int(match.group("memory"))
                        node.vcores = int(match.group("vcores"))
                        node.gpus = gpu_amount(match.group("resources"))
                    continue
                if "registered with capability:" in line and "NodeManager from node " in line:
                    match = RM_NODE_RE.search(line)
                    if match:
                        host = match.group("host").strip()
                        evidence.nodes[host] = Node(
                            node_id=host,
                            instance_type=match.group("instance_type").strip(),
                            memory_mb=int(match.group("memory")),
                            vcores=int(match.group("vcores")),
                            gpus=gpu_amount(match.group("resources")),
                        )
                    continue
                if "Assigned container container_" in line:
                    match = RM_ASSIGN_RE.search(line)
                    if not match:
                        continue
                    container_id = match.group("container")
                    host = match.group("host")
                    registered_node = evidence.nodes.get(host)
                    evidence.containers[container_id] = Container(
                        container_id=container_id,
                        application_id=application_id(container_id),
                        node_id=host,
                        start_ms=parse_timestamp(match.group("timestamp")),
                        memory_mb=int(match.group("memory")),
                        node_memory_mb=capacity(
                            match.group("max_memory"),
                            registered_node.memory_mb if registered_node else None,
                            "memory",
                        ),
                        vcores=int(match.group("vcores")),
                        node_vcores=capacity(
                            match.group("max_vcores"),
                            registered_node.vcores if registered_node else None,
                            "vcore",
                        ),
                        gpus=gpu_amount(match.group("resources")),
                        node_gpus=registered_node.gpus if registered_node else 0,
                        source="resourcemanager",
                    )
                    continue
                if " Container Transitioned from " in line:
                    match = RM_TERMINAL_RE.search(line)
                    if match:
                        container_id = match.group("container")
                        finish_ms = parse_timestamp(match.group("timestamp"))
                        existing = rm_finishes.get(container_id)
                        rm_finishes[container_id] = (
                            finish_ms if existing is None else min(existing, finish_ms)
                        )
                    continue
                if "Start request for container_" in line:
                    match = START_RE.search(line)
                    if not match:
                        continue
                    container_id = match.group("container")
                    candidate = Container(
                        container_id=container_id,
                        application_id=application_id(container_id),
                        node_id=node_id,
                        start_ms=parse_timestamp(match.group("timestamp")),
                        memory_mb=int(match.group("memory")),
                        node_memory_mb=capacity(
                            match.group("max_memory"), node.memory_mb, "memory"
                        ),
                        vcores=int(match.group("vcores")),
                        node_vcores=capacity(
                            match.group("max_vcores"), node.vcores, "vcore"
                        ),
                        gpus=gpu_amount(match.group("resources")),
                        node_gpus=node.gpus,
                    )
                    existing = evidence.containers.get(container_id)
                    if existing is None:
                        evidence.containers[container_id] = candidate
                    continue
                if " to DONE" in line and "Container container_" in line:
                    match = DONE_RE.search(line)
                    if match:
                        container_id = match.group("container")
                        finish_ms = parse_timestamp(match.group("timestamp"))
                        existing = nm_finishes.get(container_id)
                        nm_finishes[container_id] = (
                            finish_ms if existing is None else min(existing, finish_ms)
                        )

    for container_id, container in evidence.containers.items():
        if container_id in rm_finishes:
            container.finish_ms = rm_finishes[container_id]
            container.finish_source = "resourcemanager"
        elif container_id in nm_finishes:
            container.finish_ms = nm_finishes[container_id]
            container.finish_source = "nodemanager"
    for node_id, node in evidence.nodes.items():
        if any(container.node_id == node_id for container in evidence.containers.values()):
            if not node.instance_type:
                evidence.warnings.append(f"{node_id}: instance type was not found in registration logs")
    return evidence


def materialize_emr_logs(
    uri: str,
    cache_dir: Path,
    aws_profile: str | None,
    refresh: bool = False,
) -> Path:
    if not uri.startswith(("s3://", "s3a://", "s3n://")):
        path = Path(uri).expanduser()
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    s3_uri = "s3://" + uri.split("://", 1)[1]
    digest = hashlib.sha256(s3_uri.rstrip("/").encode()).hexdigest()[:16]
    cluster_name = s3_uri.rstrip("/").rsplit("/", 1)[-1]
    target = cache_dir / f"{cluster_name}-{digest}"
    marker = target / ".download-complete"
    if marker.is_file() and not refresh:
        return target
    if refresh:
        marker.unlink(missing_ok=True)
    target.mkdir(parents=True, exist_ok=True)
    command = ["aws"]
    if aws_profile:
        command += ["--profile", aws_profile]
    command += [
        "s3",
        "cp",
        "--recursive",
        s3_uri.rstrip("/") + "/",
        str(target),
        "--exclude",
        "*",
        "--include",
        "*hadoop-yarn-nodemanager*.log*",
        "--include",
        "*hadoop-yarn-resourcemanager*.log*",
    ]
    subprocess.run(command, check=True)
    if not relevant_log_files(target):
        raise ValueError(f"No YARN logs downloaded from {s3_uri}")
    marker.write_text(s3_uri + "\n")
    return target


def aws_command(aws_profile: str | None, aws_region: str | None = None) -> list[str]:
    command = ["aws"]
    if aws_profile:
        command += ["--profile", aws_profile]
    if aws_region:
        command += ["--region", aws_region]
    return command


def resolve_aws_region(
    explicit_region: str | None, aws_profile: str | None
) -> str:
    if explicit_region:
        return explicit_region
    for variable in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        value = os.environ.get(variable, "").strip()
        if value:
            return value
    command = aws_command(aws_profile) + ["configure", "get", "region"]
    completed = subprocess.run(command, capture_output=True, text=True)
    configured = completed.stdout.strip() if completed.returncode == 0 else ""
    if configured:
        return configured
    profile_hint = f" for profile {aws_profile!r}" if aws_profile else ""
    raise ValueError(
        "AWS region is not configured" + profile_hint + "; pass --aws-region "
        "or set AWS_REGION/AWS_DEFAULT_REGION"
    )

def calculator_mode(detected_class: str) -> str:
    if detected_class == "DefaultResourceCalculator":
        return "default"
    if detected_class == "DominantResourceCalculator":
        return "dominant"
    raise ValueError(
        "Could not auto-detect DefaultResourceCalculator or "
        "DominantResourceCalculator from the ResourceManager log"
    )


def container_node_share(container: Container, mode: str) -> float:
    memory_share = container.memory_mb / container.node_memory_mb
    vcore_share = container.vcores / container.node_vcores
    gpu_share = (
        container.gpus / container.node_gpus if container.node_gpus else 0.0
    )
    if mode == "default":
        return memory_share
    if mode == "dominant":
        return max(memory_share, vcore_share, gpu_share)
    raise ValueError(f"Unsupported detected calculator mode {mode}")


def container_instance_type(evidence: YarnEvidence, container: Container) -> str:
    node = evidence.nodes.get(container.node_id)
    if node and node.instance_type:
        return node.instance_type
    return f"unknown:{container.node_id}"


# Use the provider-neutral ledger while retaining the established output layer.
from yarn_job_cost_core import (  # noqa: E402
    Container as CoreContainer,
    Node as CoreNode,
    YarnEvidence as CoreYarnEvidence,
    calculator_mode as core_calculator_mode,
    container_instance_type as core_container_instance_type,
    container_node_share as core_container_node_share,
    parse_yarn_logs as core_parse_yarn_logs,
    relevant_log_files as core_relevant_log_files,
)

Container = CoreContainer
Node = CoreNode
YarnEvidence = CoreYarnEvidence
calculator_mode = core_calculator_mode
container_instance_type = core_container_instance_type
container_node_share = core_container_node_share
parse_yarn_logs = core_parse_yarn_logs
relevant_log_files = core_relevant_log_files

def calculate_applications(
    evidence: YarnEvidence,
    mode: str,
    csv_metadata: dict[str, dict[str, str]],
    include_application_master: bool,
    known_executor_container_ids: set[str] | None = None,
) -> list[dict]:
    executor_container_ids = known_executor_container_ids or set()
    by_application: dict[str, list[Container]] = defaultdict(list)
    total_containers_by_application = Counter(
        container.application_id for container in evidence.containers.values()
    )
    for container in evidence.containers.values():
        if (
            not include_application_master
            and container.sequence == 1
            and container.container_id not in executor_container_ids
        ):
            continue
        by_application[container.application_id].append(container)

    results = []
    for app_id, containers in sorted(by_application.items()):
        complete_containers = [
            container for container in containers if container.finish_ms is not None
        ]
        warnings = list(evidence.warnings)
        sequence_one_executors = [
            container.container_id
            for container in containers
            if container.sequence == 1
            and container.container_id in executor_container_ids
        ]
        if sequence_one_executors:
            warnings.append(
                "Included sequence-1 container because Spark identifies it "
                "as an executor: " + ", ".join(sequence_one_executors)
            )
        summary = evidence.application_summaries.get(app_id)
        observed_total = total_containers_by_application[app_id]
        coverage_complete = (
            summary is not None and summary.total_allocated_containers == observed_total
        )
        if summary is None:
            warnings.append("ResourceManager ApplicationSummary is missing")
        elif summary.total_allocated_containers != observed_total:
            warnings.append(
                f"ResourceManager summary reports {summary.total_allocated_containers} "
                f"allocated containers but logs contain {observed_total}"
            )
        incomplete = len(containers) - len(complete_containers)
        if incomplete:
            warnings.append(
                f"{incomplete} container(s) have no terminal timestamp and were omitted"
            )
        nm_start_fallbacks = sum(
            container.source != "resourcemanager" for container in containers
        )
        if nm_start_fallbacks:
            warnings.append(
                f"{nm_start_fallbacks} container allocation(s) use a "
                "NodeManager start fallback"
            )
        nm_finish_fallbacks = sum(
            container.finish_source == "nodemanager"
            for container in complete_containers
        )
        if nm_finish_fallbacks:
            warnings.append(
                f"{nm_finish_fallbacks} container terminal timestamp(s) use a "
                "NodeManager DONE fallback"
            )

        by_instance_type: dict[str, float] = defaultdict(float)
        by_instance_vcore_seconds: dict[str, float] = defaultdict(float)
        container_seconds = 0.0
        memory_mb_seconds = 0.0
        vcore_seconds = 0.0
        gpu_seconds = 0.0
        missing_gpu_capacity = any(
            container.gpus > 0 and container.node_gpus <= 0
            for container in complete_containers
        )
        if missing_gpu_capacity:
            warnings.append("A GPU allocation has no registered node GPU capacity")
        resource_capacity_errors: list[str] = []
        for container in complete_containers:
            duration = max(0.0, (container.finish_ms - container.start_ms) / 1000.0)
            try:
                share = container_node_share(container, mode)
            except ValueError as error:
                resource_capacity_errors.append(str(error))
                share = 0.0
            instance_type = container_instance_type(evidence, container)
            by_instance_type[instance_type] += duration * share
            by_instance_vcore_seconds[instance_type] += (
                duration * share * container.node_vcores
            )
            container_seconds += duration
            memory_mb_seconds += duration * container.memory_mb
            vcore_seconds += duration * container.vcores
            gpu_seconds += duration * container.gpus

        warnings.extend(resource_capacity_errors)
        expression = " + ".join(
            f"{seconds:.6f} {instance_type}-seconds"
            for instance_type, seconds in sorted(by_instance_type.items())
        )
        instance_vcore_expression = " + ".join(
            f"{seconds:.6f} {instance_type}-vcore-seconds"
            for instance_type, seconds in sorted(
                by_instance_vcore_seconds.items()
            )
        )
        unknown_instance_type = any(
            instance_type.startswith("unknown:") for instance_type in by_instance_type
        )
        if unknown_instance_type:
            warnings.append("One or more allocated containers have an unknown instance type")
        transient_incomplete_evidence = (
            incomplete > 0
            or not coverage_complete
            or nm_start_fallbacks > 0
            or nm_finish_fallbacks > 0
            or unknown_instance_type
            or missing_gpu_capacity
            or bool(resource_capacity_errors)
        )
        permanent_incomplete_evidence = evidence.accounting_policy_ambiguous
        complete = not transient_incomplete_evidence and not permanent_incomplete_evidence
        starts = [container.start_ms for container in containers]
        finishes = [
            container.finish_ms for container in complete_containers if container.finish_ms is not None
        ]
        result = {
            **csv_metadata.get(app_id, {"job id": "", "job name": ""}),
            "application_id": app_id,
            "application_name": summary.name if summary else "",
            "final_status": summary.final_status if summary else "",
            "resource_calculator": mode,
            "detected_resource_calculator_class": evidence.calculator_class,
            "container_count": len(containers),
            "expected_total_allocated_containers": (
                summary.total_allocated_containers if summary else ""
            ),
            "observed_total_allocated_containers": observed_total,
            "incomplete_container_count": incomplete,
            "nodemanager_start_fallback_container_count": nm_start_fallbacks,
            "nodemanager_finish_fallback_container_count": nm_finish_fallbacks,
            "container_seconds": round(container_seconds, 6),
            "memory_mb_seconds": round(memory_mb_seconds, 6),
            "vcore_seconds": round(vcore_seconds, 6),
            "gpu_seconds": round(gpu_seconds, 6),
            "node_equivalent_seconds": round(sum(by_instance_type.values()), 6),
            "node_equivalent_seconds_by_instance_type": dict(sorted(by_instance_type.items())),
            "instance_vcore_seconds": round(
                sum(by_instance_vcore_seconds.values()), 6
            ),
            "instance_vcore_seconds_by_instance_type": dict(
                sorted(by_instance_vcore_seconds.items())
            ),
            "instance_vcore_seconds_expression": instance_vcore_expression,
            "cost_expression": expression,
            "first_container_start_utc": iso_utc(min(starts) if starts else None),
            "last_container_finish_utc": iso_utc(max(finishes) if finishes else None),
            "complete": complete,
            # A fresh archive snapshot can resolve missing summaries, allocations,
            # terminal transitions, and incomplete node registration metadata. It
            # cannot resolve conflicting accounting-policy evidence already present.
            "retryable": not complete and not permanent_incomplete_evidence,
            "warnings": warnings,
        }
        results.append(result)
    return results


def add_task_packing_metrics(
    applications: list[dict],
    event_metadata: dict[str, EventLogApplication],
    evidence: YarnEvidence,
    mode: str,
) -> None:
    for application in applications:
        application.update(
            {
                "task_metrics_complete": False,
                "perfect_packing_complete": False,
                "successful_task_attempt_count": "",
                "task_duration_sum_seconds": "",
                "perfect_packing_node_seconds": "",
                "perfect_packing_node_seconds_by_instance_type": {},
                "perfect_packing_cost_expression": "",
                "perfect_packing_ec2_ondemand_usd": "",
                "perfect_packing_emr_usd": "",
                "perfect_packing_ec2_plus_emr_usd": "",
                "packing_efficiency": "",
                "actual_to_perfect_cost_factor": "",
                "actual_to_perfect_cost_overhead_percent": "",
                "task_metric_warnings": [],
            }
        )
        app_id = application["application_id"]
        metadata = event_metadata.get(app_id)
        warnings: list[str] = []
        if metadata is None:
            warnings.append("Spark event log is required for task metrics")
        else:
            application["successful_task_attempt_count"] = (
                metadata.successful_task_attempt_count
            )
            warnings.extend(metadata.task_metric_warnings)
            if metadata.event_task_metrics_complete():
                application["task_metrics_complete"] = True
                application["task_duration_sum_seconds"] = round(
                    metadata.task_duration_sum_ms / 1000.0, 6
                )
                if not application.get("complete"):
                    warnings.append(
                        "Complete YARN accounting is required for "
                        "perfect-packing metrics"
                    )
                else:
                    by_instance_type: dict[str, float] = defaultdict(float)
                    mapping_warnings = []
                    for executor_id, duration_ms in (
                        metadata.task_duration_ms_by_executor.items()
                    ):
                        executor = metadata.executors.get(executor_id)
                        if executor is None:
                            mapping_warnings.append(
                                f"Executor {executor_id} has tasks but no "
                                "ExecutorAdded event"
                            )
                            continue
                        if not executor.container_id:
                            mapping_warnings.append(
                                f"Executor {executor_id} has no YARN container ID"
                            )
                            continue
                        if executor.total_cores is None or executor.total_cores <= 0:
                            mapping_warnings.append(
                                f"Executor {executor_id} has invalid total cores"
                            )
                            continue
                        container = evidence.containers.get(executor.container_id)
                        if container is None:
                            mapping_warnings.append(
                                f"Executor {executor_id} container "
                                f"{executor.container_id} is missing from YARN logs"
                            )
                            continue
                        if container.application_id != app_id:
                            mapping_warnings.append(
                                f"Executor {executor_id} maps to a non-executor "
                                "YARN container"
                            )
                            continue
                        instance_type = container_instance_type(
                            evidence, container
                        )
                        if instance_type.startswith("unknown:"):
                            mapping_warnings.append(
                                f"Executor {executor_id} has an unknown instance type"
                            )
                            continue
                        perfect_executor_seconds = (
                            duration_ms
                            / 1000.0
                            * metadata.task_cpus
                            / executor.total_cores
                        )
                        by_instance_type[instance_type] += (
                            perfect_executor_seconds
                            * container_node_share(container, mode)
                        )
                    warnings.extend(mapping_warnings)
                    if not mapping_warnings:
                        perfect_node_seconds = sum(by_instance_type.values())
                        actual_node_seconds = float(
                            application["node_equivalent_seconds"]
                        )
                        application["perfect_packing_complete"] = True
                        application[
                            "perfect_packing_node_seconds_by_instance_type"
                        ] = {
                            instance_type: round(seconds, 6)
                            for instance_type, seconds in sorted(
                                by_instance_type.items()
                            )
                        }
                        application["perfect_packing_node_seconds"] = round(
                            perfect_node_seconds, 6
                        )
                        application["perfect_packing_cost_expression"] = (
                            " + ".join(
                                f"{seconds:.6f} {instance_type}-seconds"
                                for instance_type, seconds in sorted(
                                    by_instance_type.items()
                                )
                            )
                        )
                        if actual_node_seconds > 0:
                            efficiency = (
                                perfect_node_seconds / actual_node_seconds
                            )
                            application["packing_efficiency"] = round(
                                efficiency, 8
                            )
                            if efficiency > 1.05:
                                warnings.append(
                                    "Packing efficiency exceeds 1.05; Spark "
                                    "and YARN clock boundaries may be inconsistent"
                                )
        warnings = list(dict.fromkeys(warnings))
        application["task_metric_warnings"] = warnings
        application["warnings"].extend(
            f"Task metrics: {warning}" for warning in warnings
        )


def current_ondemand_hourly_price(
    instance_type: str, product_region: str, aws_profile: str | None
) -> dict:
    filters = [
        f"Type=TERM_MATCH,Field=instanceType,Value={instance_type}",
        f"Type=TERM_MATCH,Field=regionCode,Value={product_region}",
        "Type=TERM_MATCH,Field=operatingSystem,Value=Linux",
        "Type=TERM_MATCH,Field=tenancy,Value=Shared",
        "Type=TERM_MATCH,Field=preInstalledSw,Value=NA",
        "Type=TERM_MATCH,Field=capacitystatus,Value=Used",
    ]
    command = aws_command(aws_profile, "us-east-1") + [
        "pricing",
        "get-products",
        "--service-code",
        "AmazonEC2",
        "--filters",
        *filters,
        "--max-results",
        "100",
        "--output",
        "json",
    ]
    completed = run_aws(command)
    payload = json.loads(completed.stdout)
    matches = []
    for encoded_product in payload.get("PriceList") or []:
        product = json.loads(encoded_product)
        if product.get("product", {}).get("productFamily") != "Compute Instance":
            continue
        for term in product.get("terms", {}).get("OnDemand", {}).values():
            for dimension in term.get("priceDimensions", {}).values():
                if dimension.get("unit") != "Hrs" or dimension.get("beginRange") != "0":
                    continue
                matches.append(
                    {
                        "usd_per_hour": float(dimension["pricePerUnit"]["USD"]),
                        "effective_date": term.get("effectiveDate", ""),
                        "description": dimension.get("description", ""),
                    }
                )
    rates = {match["usd_per_hour"] for match in matches}
    if len(rates) != 1:
        raise ValueError(
            f"Expected one current Linux on-demand rate for {instance_type} in "
            f"{product_region}; found {sorted(rates)}"
        )
    return matches[0]


def current_emr_hourly_price(
    instance_type: str, product_region: str, aws_profile: str | None
) -> dict:
    filters = [
        f"Type=TERM_MATCH,Field=instanceType,Value={instance_type}",
        f"Type=TERM_MATCH,Field=regionCode,Value={product_region}",
    ]
    command = aws_command(aws_profile, "us-east-1") + [
        "pricing",
        "get-products",
        "--service-code",
        "ElasticMapReduce",
        "--filters",
        *filters,
        "--max-results",
        "100",
        "--output",
        "json",
    ]
    completed = run_aws(command)
    payload = json.loads(completed.stdout)
    matches = []
    for encoded_product in payload.get("PriceList") or []:
        product = json.loads(encoded_product)
        for term in product.get("terms", {}).get("OnDemand", {}).values():
            for dimension in term.get("priceDimensions", {}).values():
                if (
                    dimension.get("unit") != "Hrs"
                    or dimension.get("beginRange") != "0"
                ):
                    continue
                matches.append(
                    {
                        "usd_per_hour": float(
                            dimension["pricePerUnit"]["USD"]
                        ),
                        "effective_date": term.get("effectiveDate", ""),
                        "description": dimension.get("description", ""),
                    }
                )
    rates = {match["usd_per_hour"] for match in matches}
    if len(rates) != 1:
        raise ValueError(
            f"Expected one current EMR rate for {instance_type} in "
            f"{product_region}; found {sorted(rates)}"
        )
    return matches[0]


def instance_price_catalog(
    instance_types: set[str], product_region: str, aws_profile: str | None
) -> dict[str, dict[str, dict]]:
    return {
        "ec2": {
            instance_type: current_ondemand_hourly_price(
                instance_type, product_region, aws_profile
            )
            for instance_type in sorted(instance_types)
        },
        "emr": {
            instance_type: current_emr_hourly_price(
                instance_type, product_region, aws_profile
            )
            for instance_type in sorted(instance_types)
        },
    }


def application_resource_cost(
    application: dict, prices: dict[str, dict], seconds_field: str
) -> float:
    return sum(
        float(seconds) * prices[instance_type]["usd_per_hour"] / 3600.0
        for instance_type, seconds in application[seconds_field].items()
    )


def application_ondemand_cost(application: dict, prices: dict[str, dict]) -> float:
    return application_resource_cost(
        application, prices, "node_equivalent_seconds_by_instance_type"
    )


def perfect_packing_ondemand_cost(
    application: dict, prices: dict[str, dict]
) -> float:
    return application_resource_cost(
        application,
        prices,
        "perfect_packing_node_seconds_by_instance_type",
    )


def add_ondemand_costs(
    applications: list[dict], product_region: str, aws_profile: str
) -> dict[str, dict[str, dict]]:
    instance_types = {
        instance_type
        for application in applications
        if application.get("complete") is True
        for instance_type in application[
            "node_equivalent_seconds_by_instance_type"
        ]
    }
    prices = instance_price_catalog(
        instance_types, product_region, aws_profile
    )
    for application in applications:
        if application.get("complete") is True:
            ec2_cost = application_ondemand_cost(application, prices["ec2"])
            emr_cost = application_ondemand_cost(application, prices["emr"])
            application["ec2_ondemand_usd"] = round(ec2_cost, 8)
            application["emr_usd"] = round(emr_cost, 8)
            application["ec2_plus_emr_usd"] = round(
                ec2_cost + emr_cost, 8
            )
            if application.get("perfect_packing_complete") is True:
                perfect_ec2 = perfect_packing_ondemand_cost(
                    application, prices["ec2"]
                )
                perfect_emr = perfect_packing_ondemand_cost(
                    application, prices["emr"]
                )
                perfect_total = perfect_ec2 + perfect_emr
                actual_total = ec2_cost + emr_cost
                application["perfect_packing_ec2_ondemand_usd"] = round(
                    perfect_ec2, 8
                )
                application["perfect_packing_emr_usd"] = round(
                    perfect_emr, 8
                )
                application["perfect_packing_ec2_plus_emr_usd"] = round(
                    perfect_total, 8
                )
                if perfect_total > 0:
                    factor = actual_total / perfect_total
                    application["actual_to_perfect_cost_factor"] = round(
                        factor, 8
                    )
                    application[
                        "actual_to_perfect_cost_overhead_percent"
                    ] = round((factor - 1.0) * 100.0, 6)
        else:
            application["ec2_ondemand_usd"] = ""
            application["emr_usd"] = ""
            application["ec2_plus_emr_usd"] = ""
    return prices


def application_warnings(application: dict | None) -> str:
    if not application:
        return ""
    warnings = application.get("warnings", [])
    return " | ".join(warnings) if isinstance(warnings, list) else str(warnings)


def application_instance_types(application: dict | None) -> str:
    if not application:
        return ""
    by_instance_type = application.get(
        "node_equivalent_seconds_by_instance_type", {}
    )
    return ", ".join(sorted(by_instance_type))


def index_applications_by_job_id(applications: list[dict], label: str) -> dict[str, dict]:
    indexed = {}
    for application in applications:
        job_id = str(application.get("job id") or "")
        if not job_id:
            raise ValueError(
                f"{label} application {application.get('application_id')} has no derived Job ID"
            )
        if job_id in indexed:
            raise ValueError(f"{label} has duplicate Job ID {job_id}")
        indexed[job_id] = application
    return indexed


def rounded_delta(test: object, baseline: object, digits: int = 6) -> float | str:
    if test == "" or baseline == "" or test is None or baseline is None:
        return ""
    return round(float(test) - float(baseline), digits)


def rounded_factor(
    test: object, baseline: object, digits: int = 6
) -> float | str:
    if (
        test == ""
        or baseline == ""
        or test is None
        or baseline is None
    ):
        return ""
    baseline_value = float(baseline)
    if baseline_value == 0:
        return ""
    return round(float(test) / baseline_value, digits)


def sort_comparison_rows(rows: list[dict], sort_by: str) -> list[dict]:
    if sort_by == "job-id":
        return sorted(rows, key=lambda row: int(row["job_id"]))
    field = {
        "wall-clock-factor": "wall_clock_factor",
        "cost-factor": "ec2_plus_emr_cost_factor",
        "task-duration-factor": "task_duration_factor",
        "perfect-packing-cost-factor": "perfect_packing_cost_factor",
    }[sort_by]
    return sorted(
        rows,
        key=lambda row: (
            row[field] == ""
            or row["baseline_final_status"] != "SUCCEEDED"
            or row["test_final_status"] != "SUCCEEDED",
            float(row[field]) if row[field] != "" else float("inf"),
            int(row["job_id"]),
        ),
    )


def build_comparison_rows(
    baseline_applications: list[dict],
    test_applications: list[dict],
    prices: dict[str, dict],
) -> list[dict]:
    baseline = index_applications_by_job_id(baseline_applications, "baseline")
    test = index_applications_by_job_id(test_applications, "test")
    job_ids = sorted(set(baseline) | set(test), key=lambda value: int(value))
    rows = []
    for job_id in job_ids:
        base = baseline.get(job_id)
        other = test.get(job_id)
        base_wall = base.get("spark_duration_seconds", "") if base else ""
        other_wall = other.get("spark_duration_seconds", "") if other else ""
        base_node = base.get("node_equivalent_seconds", "") if base else ""
        other_node = other.get("node_equivalent_seconds", "") if other else ""
        base_instance_vcore = (
            base.get("instance_vcore_seconds", "") if base else ""
        )
        test_instance_vcore = (
            other.get("instance_vcore_seconds", "") if other else ""
        )
        base_complete = base.get("complete") is True if base else False
        test_complete = other.get("complete") is True if other else False
        base_task_complete = (
            base.get("task_metrics_complete") is True if base else False
        )
        test_task_complete = (
            other.get("task_metrics_complete") is True if other else False
        )
        base_perfect_complete = (
            base.get("perfect_packing_complete") is True if base else False
        )
        test_perfect_complete = (
            other.get("perfect_packing_complete") is True if other else False
        )
        base_task_duration = (
            base.get("task_duration_sum_seconds", "") if base else ""
        )
        test_task_duration = (
            other.get("task_duration_sum_seconds", "") if other else ""
        )
        base_perfect_node = (
            base.get("perfect_packing_node_seconds", "") if base else ""
        )
        test_perfect_node = (
            other.get("perfect_packing_node_seconds", "") if other else ""
        )
        base_ec2_cost = (
            application_ondemand_cost(base, prices["ec2"])
            if base_complete
            else ""
        )
        other_ec2_cost = (
            application_ondemand_cost(other, prices["ec2"])
            if test_complete
            else ""
        )
        base_emr_cost = (
            application_ondemand_cost(base, prices["emr"])
            if base_complete
            else ""
        )
        other_emr_cost = (
            application_ondemand_cost(other, prices["emr"])
            if test_complete
            else ""
        )
        base_total_cost = (
            base_ec2_cost + base_emr_cost if base_complete else ""
        )
        other_total_cost = (
            other_ec2_cost + other_emr_cost if test_complete else ""
        )
        base_perfect_cost = (
            perfect_packing_ondemand_cost(base, prices["ec2"])
            + perfect_packing_ondemand_cost(base, prices["emr"])
            if base_complete and base_perfect_complete
            else ""
        )
        test_perfect_cost = (
            perfect_packing_ondemand_cost(other, prices["ec2"])
            + perfect_packing_ondemand_cost(other, prices["emr"])
            if test_complete and test_perfect_complete
            else ""
        )
        base_actual_to_perfect = (
            base_total_cost / base_perfect_cost
            if base_perfect_cost not in ("", 0)
            else ""
        )
        test_actual_to_perfect = (
            other_total_cost / test_perfect_cost
            if test_perfect_cost not in ("", 0)
            else ""
        )
        rows.append(
            {
                "job_id": job_id,
                "baseline_application_id": base.get("application_id", "") if base else "",
                "test_application_id": other.get("application_id", "") if other else "",
                "baseline_instance_type": application_instance_types(base),
                "test_instance_type": application_instance_types(other),
                "baseline_event_log_root": (
                    base.get("source_event_log_root", "") if base else ""
                ),
                "test_event_log_root": (
                    other.get("source_event_log_root", "") if other else ""
                ),
                "baseline_final_status": base.get("final_status", "") if base else "MISSING",
                "test_final_status": other.get("final_status", "") if other else "MISSING",
                "baseline_complete": base_complete if base else "",
                "test_complete": test_complete if other else "",
                "baseline_warnings": application_warnings(base),
                "test_warnings": application_warnings(other),
                "baseline_wall_clock_seconds": base_wall,
                "test_wall_clock_seconds": other_wall,
                "wall_clock_factor": rounded_factor(other_wall, base_wall),
                "wall_clock_delta_seconds": rounded_delta(other_wall, base_wall),
                "baseline_task_metrics_complete": (
                    base_task_complete if base else ""
                ),
                "test_task_metrics_complete": (
                    test_task_complete if other else ""
                ),
                "baseline_perfect_packing_complete": (
                    base_perfect_complete if base else ""
                ),
                "test_perfect_packing_complete": (
                    test_perfect_complete if other else ""
                ),
                "baseline_task_duration_sum_seconds": base_task_duration,
                "test_task_duration_sum_seconds": test_task_duration,
                "task_duration_factor": rounded_factor(
                    test_task_duration, base_task_duration
                ),
                "baseline_perfect_packing_node_seconds": base_perfect_node,
                "test_perfect_packing_node_seconds": test_perfect_node,
                "baseline_perfect_packing_ec2_plus_emr_usd": (
                    round(base_perfect_cost, 8)
                    if base_perfect_cost != ""
                    else ""
                ),
                "test_perfect_packing_ec2_plus_emr_usd": (
                    round(test_perfect_cost, 8)
                    if test_perfect_cost != ""
                    else ""
                ),
                "perfect_packing_cost_factor": rounded_factor(
                    test_perfect_cost, base_perfect_cost
                ),
                "perfect_packing_cost_delta_usd": rounded_delta(
                    test_perfect_cost, base_perfect_cost, 8
                ),
                "baseline_packing_efficiency": (
                    base.get("packing_efficiency", "") if base else ""
                ),
                "test_packing_efficiency": (
                    other.get("packing_efficiency", "") if other else ""
                ),
                "packing_efficiency_delta": rounded_delta(
                    other.get("packing_efficiency", "") if other else "",
                    base.get("packing_efficiency", "") if base else "",
                    8,
                ),
                "baseline_actual_to_perfect_cost_factor": (
                    round(base_actual_to_perfect, 8)
                    if base_actual_to_perfect != ""
                    else ""
                ),
                "test_actual_to_perfect_cost_factor": (
                    round(test_actual_to_perfect, 8)
                    if test_actual_to_perfect != ""
                    else ""
                ),
                "baseline_actual_to_perfect_cost_overhead_percent": (
                    round((base_actual_to_perfect - 1.0) * 100.0, 6)
                    if base_actual_to_perfect != ""
                    else ""
                ),
                "test_actual_to_perfect_cost_overhead_percent": (
                    round((test_actual_to_perfect - 1.0) * 100.0, 6)
                    if test_actual_to_perfect != ""
                    else ""
                ),
                "baseline_node_equivalent_seconds": base_node,
                "test_node_equivalent_seconds": other_node,
                "node_equivalent_delta_seconds": rounded_delta(other_node, base_node),
                "baseline_instance_vcore_seconds": base_instance_vcore,
                "test_instance_vcore_seconds": test_instance_vcore,
                "instance_vcore_seconds_factor": rounded_factor(
                    test_instance_vcore if test_complete else "",
                    base_instance_vcore if base_complete else "",
                ),
                "instance_vcore_seconds_delta": rounded_delta(
                    test_instance_vcore if test_complete else "",
                    base_instance_vcore if base_complete else "",
                ),
                "baseline_ec2_ondemand_usd": (
                    round(base_ec2_cost, 8) if base_ec2_cost != "" else ""
                ),
                "test_ec2_ondemand_usd": (
                    round(other_ec2_cost, 8) if other_ec2_cost != "" else ""
                ),
                "ec2_ondemand_cost_factor": rounded_factor(
                    other_ec2_cost, base_ec2_cost
                ),
                "ec2_ondemand_delta_usd": rounded_delta(
                    other_ec2_cost, base_ec2_cost, 8
                ),
                "baseline_emr_usd": (
                    round(base_emr_cost, 8) if base_emr_cost != "" else ""
                ),
                "test_emr_usd": (
                    round(other_emr_cost, 8) if other_emr_cost != "" else ""
                ),
                "baseline_ec2_plus_emr_usd": (
                    round(base_total_cost, 8)
                    if base_total_cost != ""
                    else ""
                ),
                "test_ec2_plus_emr_usd": (
                    round(other_total_cost, 8)
                    if other_total_cost != ""
                    else ""
                ),
                "ec2_plus_emr_cost_factor": rounded_factor(
                    other_total_cost, base_total_cost
                ),
                "ec2_plus_emr_delta_usd": rounded_delta(
                    other_total_cost, base_total_cost, 8
                ),
            }
        )
    return rows


def summarize_applications(applications: list[dict]) -> dict:
    successful_costed = [
        application
        for application in applications
        if application.get("final_status") == "SUCCEEDED"
        and application.get("complete") is True
    ]
    successful_incomplete_count = sum(
        application.get("final_status") == "SUCCEEDED"
        and application.get("complete") is not True
        for application in applications
    )
    instance_vcore_seconds_by_type: dict[str, float] = defaultdict(float)
    for application in successful_costed:
        for instance_type, seconds in application.get(
            "instance_vcore_seconds_by_instance_type", {}
        ).items():
            instance_vcore_seconds_by_type[instance_type] += float(seconds)
    total_instance_vcore_seconds = sum(
        instance_vcore_seconds_by_type.values()
    )
    total_instance_vcore_expression = " + ".join(
        f"{seconds:.6f} {instance_type}-vcore-seconds"
        for instance_type, seconds in sorted(
            instance_vcore_seconds_by_type.items()
        )
    )

    eligible: list[dict] = []
    excluded = []
    for application in applications:
        reasons = []
        if application.get("final_status") != "SUCCEEDED":
            reasons.append("application did not succeed")
        if application.get("complete") is not True:
            reasons.append("YARN accounting is incomplete")
        if application.get("task_metrics_complete") is not True:
            reasons.append("task metrics are incomplete")
        if application.get("perfect_packing_complete") is not True:
            reasons.append("perfect-packing metrics are incomplete")
        if reasons:
            excluded.append(
                {
                    "job_id": str(application.get("job id") or ""),
                    "application_id": application.get("application_id", ""),
                    "reasons": reasons,
                }
            )
        else:
            eligible.append(application)

    actual_node_seconds = sum(
        float(application["node_equivalent_seconds"])
        for application in eligible
    )
    perfect_node_seconds = sum(
        float(application["perfect_packing_node_seconds"])
        for application in eligible
    )
    actual_cost = sum(
        float(application["ec2_plus_emr_usd"]) for application in eligible
    )
    perfect_cost = sum(
        float(application["perfect_packing_ec2_plus_emr_usd"])
        for application in eligible
    )
    actual_to_perfect = (
        actual_cost / perfect_cost if perfect_cost > 0 else None
    )
    return {
        "eligible_job_count": len(eligible),
        "excluded_job_count": len(excluded),
        "excluded_jobs": excluded,
        "successful_complete_job_count": len(successful_costed),
        "successful_incomplete_job_count": successful_incomplete_count,
        "total_instance_vcore_seconds": round(
            total_instance_vcore_seconds, 6
        ),
        "total_instance_vcore_seconds_by_instance_type": {
            instance_type: round(seconds, 6)
            for instance_type, seconds in sorted(
                instance_vcore_seconds_by_type.items()
            )
        },
        "total_instance_vcore_seconds_expression": (
            total_instance_vcore_expression or "0 instance-vcore-seconds"
        ),
        "successful_task_attempt_count": sum(
            int(application["successful_task_attempt_count"])
            for application in eligible
        ),
        "task_duration_sum_seconds": round(
            sum(
                float(application["task_duration_sum_seconds"])
                for application in eligible
            ),
            6,
        ),
        "perfect_packing_node_seconds": round(perfect_node_seconds, 6),
        "actual_node_equivalent_seconds": round(actual_node_seconds, 6),
        "perfect_packing_ec2_plus_emr_usd": round(perfect_cost, 8),
        "actual_ec2_plus_emr_usd": round(actual_cost, 8),
        "packing_efficiency": (
            round(perfect_node_seconds / actual_node_seconds, 8)
            if actual_node_seconds > 0
            else None
        ),
        "actual_to_perfect_cost_factor": (
            round(actual_to_perfect, 8)
            if actual_to_perfect is not None
            else None
        ),
        "actual_to_perfect_cost_overhead_percent": (
            round((actual_to_perfect - 1.0) * 100.0, 6)
            if actual_to_perfect is not None
            else None
        ),
    }


def build_comparison_summary(rows: list[dict]) -> dict:
    eligible = []
    excluded = []
    for row in rows:
        reasons = []
        if row.get("baseline_final_status") != "SUCCEEDED":
            reasons.append("baseline application did not succeed")
        if row.get("test_final_status") != "SUCCEEDED":
            reasons.append("test application did not succeed")
        if row.get("baseline_complete") is not True:
            reasons.append("baseline YARN accounting is incomplete")
        if row.get("test_complete") is not True:
            reasons.append("test YARN accounting is incomplete")
        if row.get("baseline_task_metrics_complete") is not True:
            reasons.append("baseline task metrics are incomplete")
        if row.get("test_task_metrics_complete") is not True:
            reasons.append("test task metrics are incomplete")
        if row.get("baseline_perfect_packing_complete") is not True:
            reasons.append("baseline perfect-packing metrics are incomplete")
        if row.get("test_perfect_packing_complete") is not True:
            reasons.append("test perfect-packing metrics are incomplete")
        if reasons:
            excluded.append({"job_id": row["job_id"], "reasons": reasons})
        else:
            eligible.append(row)

    def side(prefix: str) -> dict:
        task_seconds = sum(
            float(row[f"{prefix}_task_duration_sum_seconds"])
            for row in eligible
        )
        perfect_node_seconds = sum(
            float(row[f"{prefix}_perfect_packing_node_seconds"])
            for row in eligible
        )
        actual_node_seconds = sum(
            float(row[f"{prefix}_node_equivalent_seconds"])
            for row in eligible
        )
        perfect_cost = sum(
            float(row[f"{prefix}_perfect_packing_ec2_plus_emr_usd"])
            for row in eligible
        )
        actual_cost = sum(
            float(row[f"{prefix}_ec2_plus_emr_usd"])
            for row in eligible
        )
        factor = actual_cost / perfect_cost if perfect_cost > 0 else None
        return {
            "task_duration_sum_seconds": round(task_seconds, 6),
            "perfect_packing_node_seconds": round(perfect_node_seconds, 6),
            "actual_node_equivalent_seconds": round(actual_node_seconds, 6),
            "perfect_packing_ec2_plus_emr_usd": round(perfect_cost, 8),
            "actual_ec2_plus_emr_usd": round(actual_cost, 8),
            "packing_efficiency": (
                round(perfect_node_seconds / actual_node_seconds, 8)
                if actual_node_seconds > 0
                else None
            ),
            "actual_to_perfect_cost_factor": (
                round(factor, 8) if factor is not None else None
            ),
            "actual_to_perfect_cost_overhead_percent": (
                round((factor - 1.0) * 100.0, 6)
                if factor is not None
                else None
            ),
        }

    baseline = side("baseline")
    test = side("test")
    return {
        "eligible_job_count": len(eligible),
        "excluded_job_count": len(excluded),
        "excluded_jobs": excluded,
        "baseline": baseline,
        "test": test,
        "task_duration_factor": rounded_factor(
            test["task_duration_sum_seconds"],
            baseline["task_duration_sum_seconds"],
        ),
        "perfect_packing_cost_factor": rounded_factor(
            test["perfect_packing_ec2_plus_emr_usd"],
            baseline["perfect_packing_ec2_plus_emr_usd"],
        ),
        "actual_cost_factor": rounded_factor(
            test["actual_ec2_plus_emr_usd"],
            baseline["actual_ec2_plus_emr_usd"],
        ),
    }


def print_summary(summary: dict, heading: str = "Summary") -> None:
    print(
        "{}: {} packing-eligible job(s), {} excluded".format(
            heading,
            summary["eligible_job_count"],
            summary["excluded_job_count"],
        )
    )
    for name in (
        "successful_complete_job_count",
        "successful_incomplete_job_count",
        "total_instance_vcore_seconds",
        "total_instance_vcore_seconds_expression",
        "task_duration_sum_seconds",
        "perfect_packing_node_seconds",
        "actual_node_equivalent_seconds",
        "perfect_packing_ec2_plus_emr_usd",
        "actual_ec2_plus_emr_usd",
        "packing_efficiency",
        "actual_to_perfect_cost_factor",
        "actual_to_perfect_cost_overhead_percent",
    ):
        print("  {}: {}".format(name, summary[name]))


def print_comparison_summary(summary: dict) -> None:
    print(
        "Comparison summary: {} eligible job(s), {} excluded".format(
            summary["eligible_job_count"],
            summary["excluded_job_count"],
        )
    )
    for name in (
        "task_duration_factor",
        "perfect_packing_cost_factor",
        "actual_cost_factor",
    ):
        print(f"  {name}: {summary[name]}")
    for label, side in (("Baseline", "baseline"), ("Test", "test")):
        print(f"  {label} totals:")
        for name, value in summary[side].items():
            print(f"    {name}: {value}")


def analyze_event_root_for_comparison(root: str, args: argparse.Namespace) -> dict:
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / "analysis.json"
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--event-log-root",
            root,
            "--output-json",
            str(output),
            "--aws-profile",
            args.aws_profile or "",
            "--aws-region",
            args.aws_region,
            "--cache-dir",
            str(args.cache_dir),
            "--event-cache-dir",
            str(args.event_cache_dir),
        ]
        if args.include_application_master:
            command.append("--include-application-master")
        if args.refresh_emr_log_cache:
            command.append("--refresh-emr-log-cache")
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode:
            raise RuntimeError(
                f"Analysis failed for {root}:\n{completed.stderr.strip()}"
            )
        return json.loads(output.read_text())


def print_comparison_table(rows: list[dict]) -> None:
    rendered = [
        [str(row.get(field, "")) for _, field in COMPARISON_CONSOLE_FIELDS]
        for row in rows
    ]
    widths = [
        max(len(heading), *(len(row[index]) for row in rendered))
        for index, (heading, _) in enumerate(COMPARISON_CONSOLE_FIELDS)
    ]
    print(
        "  ".join(
            heading.ljust(widths[index])
            for index, (heading, _) in enumerate(COMPARISON_CONSOLE_FIELDS)
        )
    )
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print(
            "  ".join(
                value.ljust(widths[index]) for index, value in enumerate(row)
            )
        )


def merge_test_analyses(
    roots: list[str], analyses: list[dict]
) -> tuple[list[dict], list[dict]]:
    merged: dict[str, dict] = {}
    overrides = []
    for root, analysis in zip(roots, analyses, strict=True):
        for application in analysis["applications"]:
            job_id = str(application.get("job id") or "")
            if not job_id:
                raise ValueError(
                    f"Test application {application.get('application_id')} "
                    f"from {root} has no derived Job ID"
                )
            enriched = dict(application)
            enriched["source_event_log_root"] = root
            previous = merged.get(job_id)
            if previous is not None:
                overrides.append(
                    {
                        "job_id": job_id,
                        "replaced_application_id": previous.get("application_id", ""),
                        "replaced_event_log_root": previous["source_event_log_root"],
                        "winning_application_id": enriched.get("application_id", ""),
                        "winning_event_log_root": root,
                    }
                )
            merged[job_id] = enriched
    return (
        [merged[job_id] for job_id in sorted(merged, key=lambda value: int(value))],
        overrides,
    )


def run_comparison(args: argparse.Namespace) -> int:
    if not args.event_log_root:
        raise ValueError("--test-event-log-root requires --event-log-root")
    baseline = analyze_event_root_for_comparison(args.event_log_root, args)
    test_roots = args.test_event_log_root
    test_runs = [
        analyze_event_root_for_comparison(root, args) for root in test_roots
    ]
    baseline_applications = []
    for application in baseline["applications"]:
        enriched = dict(application)
        enriched["source_event_log_root"] = args.event_log_root
        baseline_applications.append(enriched)
    test_applications, test_overrides = merge_test_analyses(
        test_roots, test_runs
    )
    instance_types = {
        instance_type
        for application in baseline_applications + test_applications
        if application.get("complete") is True
        for instance_type in application[
            "node_equivalent_seconds_by_instance_type"
        ]
    }
    prices = instance_price_catalog(
        instance_types, args.aws_region, args.aws_profile
    )
    rows = build_comparison_rows(
        baseline_applications, test_applications, prices
    )
    rows = sort_comparison_rows(rows, args.sort_by)
    summary = build_comparison_summary(rows)
    test_merged_summary = summarize_applications(test_applications)
    if args.output_csv:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COMPARISON_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "baseline_event_log_root": args.event_log_root,
            "test_event_log_roots": test_roots,
            "pricing_product_region": args.aws_region,
            "pricing_queried_at_utc": datetime.now(timezone.utc).isoformat(),
            "ec2_ondemand_prices": prices["ec2"],
            "emr_prices": prices["emr"],
            "comparison": rows,
            "comparison_summary": summary,
            "baseline_run_summary": baseline["summary"],
            "baseline": baseline,
            "test_runs": [
                {"event_log_root": root, "analysis": analysis}
                for root, analysis in zip(
                    test_roots, test_runs, strict=True
                )
            ],
            "test_merged_applications": test_applications,
            "test_merged_summary": test_merged_summary,
            "test_overrides": test_overrides,
        }
        args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if not args.output_csv and not args.output_json:
        print_comparison_table(rows)
    elif args.output_csv:
        print(f"Wrote {len(rows)} job comparison row(s) to {args.output_csv}")
    else:
        print(f"Wrote {len(rows)} job comparison row(s) to {args.output_json}")
    print_comparison_summary(summary)
    print_summary(baseline["summary"], "Baseline run summary")
    print_summary(test_merged_summary, "Test overlay summary")
    return 0


def print_console_table(results: list[dict]) -> None:
    rows = []
    for result in results:
        row = []
        for _, field in CONSOLE_FIELDS:
            value = result.get(field, "")
            if field == "warnings":
                value = " | ".join(value)
            row.append(str(value))
        rows.append(row)

    widths = [
        max(len(heading), *(len(row[index]) for row in rows))
        for index, (heading, _) in enumerate(CONSOLE_FIELDS)
    ]
    print(
        "  ".join(
            heading.ljust(widths[index])
            for index, (heading, _) in enumerate(CONSOLE_FIELDS)
        )
    )
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print(
            "  ".join(
                value.ljust(widths[index]) for index, value in enumerate(row)
            )
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--event-log-root", help="Spark event-log run directory (preferred)")
    source.add_argument(
        "--yarn-log-root", "--emr-log-uri", dest="yarn_log_root",
        help="Exact archived YARN daemon-log URI, directory, or archive",
    )
    parser.add_argument(
        "--adapter", choices=ADAPTERS, default="emr",
        help="Platform discovery and node-classification adapter (default: emr)",
    )
    parser.add_argument(
        "--pricing", choices=("none", "catalog", "live"), default="live",
        help="Pricing source; live is currently supported by the EMR adapter",
    )
    parser.add_argument("--price-catalog", type=Path)
    parser.add_argument("--node-class-map", type=Path)
    parser.add_argument(
        "--test-event-log-root",
        action="append",
        help=(
            "Test event-log root; repeat in overlay order, with the last "
            "root containing a Job ID taking precedence"
        ),
    )
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument(
        "--sort-by",
        choices=(
            "job-id",
            "wall-clock-factor",
            "cost-factor",
            "task-duration-factor",
            "perfect-packing-cost-factor",
        ),
        default="job-id",
        help="Comparison row order (default: job-id)",
    )
    parser.add_argument("--input-csv", type=Path, help="Optional job metadata/event-log CSV")
    parser.add_argument("--event-log-column")
    parser.add_argument("--include-application-master", action="store_true")
    parser.add_argument(
        "--refresh-emr-log-cache",
        action="store_true",
        help="Refresh cached ResourceManager and NodeManager logs from S3",
    )
    parser.add_argument(
        "--aws-profile",
        default=DEFAULT_AWS_PROFILE,
        help="AWS CLI profile; omit to use the standard credential chain",
    )
    parser.add_argument(
        "--aws-region",
        default=DEFAULT_AWS_REGION,
        help="AWS product region; defaults to the effective AWS CLI region",
    )
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/yarn-resource-cost/yarn-logs"))
    parser.add_argument(
        "--event-cache-dir",
        type=Path,
        default=Path(".cache/yarn-job-cost/event-metadata"),
    )
    args = parser.parse_args()
    args.emr_log_uri = args.yarn_log_root
    return args


def main() -> int:
    args = parse_args()
    args.aws_region = resolve_aws_region(args.aws_region, args.aws_profile)
    if args.test_event_log_root:
        return run_comparison(args)
    event_metadata: dict[str, EventLogApplication] = {}
    selected_application_ids: set[str] | None = None
    cluster_id = ""
    emr_log_uri = args.emr_log_uri
    local_event_path: Path | None = None

    if args.event_log_root:
        local_event_path, selected_application_ids = materialize_event_metadata_files(
            args.event_log_root, args.event_cache_dir, args.aws_profile, args.aws_region
        )
        event_metadata = read_event_log_metadata(local_event_path)
        if not selected_application_ids:
            selected_application_ids = set(event_metadata)
        missing_metadata = selected_application_ids - set(event_metadata)
        if missing_metadata:
            raise ValueError(
                "Could not read application metadata for: " + ", ".join(sorted(missing_metadata))
            )
        cluster_ids = {app.cluster_id for app in event_metadata.values() if app.cluster_id}
        if len(cluster_ids) != 1:
            raise ValueError(
                "Event-log root must identify exactly one EMR cluster; found: "
                + (", ".join(sorted(cluster_ids)) or "none")
            )
        cluster_id = next(iter(cluster_ids))
        emr_log_uri = resolve_emr_log_uri(cluster_id, args.aws_profile, args.aws_region)

    local_logs = materialize_emr_logs(
        emr_log_uri,
        args.cache_dir,
        args.aws_profile,
        refresh=args.refresh_emr_log_cache,
    )
    evidence = parse_yarn_logs(local_logs)
    mode = calculator_mode(evidence.calculator_class)
    metadata = {app_id: app.as_metadata() for app_id, app in event_metadata.items()}
    csv_metadata = load_csv_metadata(args.input_csv, args.event_log_column)
    for app_id, csv_values in csv_metadata.items():
        target = metadata.setdefault(app_id, {})
        target.update({key: value for key, value in csv_values.items() if value})
    known_executor_container_ids = {
        executor.container_id
        for application in event_metadata.values()
        for executor in application.executors.values()
        if executor.container_id
    }
    results = calculate_applications(
        evidence,
        mode,
        metadata,
        args.include_application_master,
        known_executor_container_ids,
    )
    if selected_application_ids is not None:
        results = [row for row in results if row["application_id"] in selected_application_ids]
        missing_costs = selected_application_ids - {row["application_id"] for row in results}
        if missing_costs:
            raise ValueError(
                "Selected applications are missing from YARN logs: "
                + ", ".join(sorted(missing_costs))
            )
    add_task_packing_metrics(results, event_metadata, evidence, mode)
    prices = add_ondemand_costs(results, args.aws_region, args.aws_profile)
    summary = summarize_applications(results)

    if args.output_csv:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=OUTPUT_FIELDS, extrasaction="ignore"
            )
            writer.writeheader()
            for result in results:
                row = dict(result)
                row["warnings"] = " | ".join(result["warnings"])
                row["task_metric_warnings"] = " | ".join(
                    result["task_metric_warnings"]
                )
                writer.writerow(row)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "event_log_root": args.event_log_root or "",
            "local_event_metadata_path": str(local_event_path) if local_event_path else "",
            "emr_cluster_id": cluster_id,
            "emr_log_uri": emr_log_uri,
            "local_log_path": str(local_logs),
            "resource_calculator": mode,
            "detected_resource_calculator_class": evidence.calculator_class,
            "pricing_product_region": args.aws_region,
            "pricing_queried_at_utc": datetime.now(timezone.utc).isoformat(),
            "ec2_ondemand_prices": prices["ec2"],
            "emr_prices": prices["emr"],
            "nodes": {
                node_id: {
                    "instance_type": node.instance_type,
                    "memory_mb": node.memory_mb,
                    "vcores": node.vcores,
                    "gpus": node.gpus,
                }
                for node_id, node in sorted(evidence.nodes.items())
                if node.instance_type
                and any(container.node_id == node_id for container in evidence.containers.values())
            },
            "applications": results,
            "summary": summary,
        }
        args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if not args.output_csv and not args.output_json:
        print_console_table(results)
    elif args.output_csv:
        print(f"Wrote {len(results)} YARN application cost row(s) to {args.output_csv}")
    else:
        print(f"Wrote {len(results)} YARN application cost row(s) to {args.output_json}")
    print_summary(summary)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AwsCliError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
