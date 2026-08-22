#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral YARN allocation ledger and resource-share accounting."""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO


TIMESTAMP = r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})"
CONTAINER = r"(?P<container>container_\d+_\d+_\d+_\d+)"
RESOURCE = (
    r"<memory:(?P<memory>\d+)(?:, max memory:(?P<max_memory>\d+))?, "
    r"vCores:(?P<vcores>\d+)(?:, max vCores:(?P<max_vcores>\d+))?"
    r"(?P<resources>[^>]*)>"
)
START_RE = re.compile(
    rf"^{TIMESTAMP} .*Start request for {CONTAINER} .* resource {RESOURCE}"
)
DONE_RE = re.compile(
    rf"^{TIMESTAMP} .*Container {CONTAINER} transitioned from .* to DONE\b"
)
RM_ASSIGN_RE = re.compile(
    rf"^{TIMESTAMP} .*Assigned container {CONTAINER} of capacity {RESOURCE} "
    r"on host (?P<host>[^:,\s]+):\d+"
)
RM_TERMINAL_RE = re.compile(
    rf"^{TIMESTAMP} .*{CONTAINER} Container Transitioned from .* to "
    r"(?:COMPLETED|RELEASED|KILLED|EXPIRED)\b"
)
APPLICATION_SUMMARY_RE = re.compile(
    r"appId=(?P<application>application_\d+_\d+),name=(?P<name>.*?),user=.*?"
    r"finalStatus=(?P<final_status>[^,]+).*?"
    r"totalAllocatedContainers=(?P<containers>\d+)"
)
NODE_RE = re.compile(
    r"Registered with ResourceManager .* total resource of "
    r"<memory:(?P<memory>\d+), vCores:(?P<vcores>\d+)(?P<resources>[^>]*)>"
)
RM_NODE_RE = re.compile(
    r"NodeManager from node (?P<host>[^ (]+)(?:\([^)]*\))? registered with "
    r"capability: <memory:(?P<memory>\d+), vCores:(?P<vcores>\d+)"
    r"(?P<resources>[^>]*)>"
)
INSTANCE_TYPE_RE = re.compile(r"instanceType\(STRING\)=(?P<value>[^}\]\s]+)")
RESOURCE_ENTRY_RE = re.compile(
    r"(?:^|,\s*)(?P<name>[A-Za-z0-9_.\-/]+):\s*(?P<value>\d+)"
)
CALCULATOR_RE = re.compile(
    r"(?:calculator=class |resource-calculator(?:=|: )\s*(?:class )?)"
    r"(?:org\.apache\.hadoop\.yarn\.util\.resource\.)?"
    r"(?P<calculator>DefaultResourceCalculator|DominantResourceCalculator)"
)
FAIR_POLICY_RE = re.compile(
    r"(?P<policy>DominantResourceFairnessPolicy|FairSharePolicy|FifoPolicy)"
)
CONTAINER_PARTS_RE = re.compile(
    r"container_(?P<cluster>\d+)_(?P<application>\d+)_"
    r"(?P<attempt>\d+)_(?P<sequence>\d+)"
)


@dataclass
class Node:
    node_id: str
    instance_type: str = ""
    memory_mb: int | None = None
    vcores: int | None = None
    gpus: int = 0
    resources: dict[str, int] = field(default_factory=dict)

    @property
    def node_class(self) -> str:
        return self.instance_type or self.node_id


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
    resources: dict[str, int] = field(default_factory=dict)
    node_resources: dict[str, int] = field(default_factory=dict)
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
    calculator_source: str = ""
    scheduler_policy: str = ""
    accounting_policy_ambiguous: bool = False
    application_summaries: dict[str, ApplicationSummary] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def parse_timestamp(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f").replace(
        tzinfo=timezone.utc
    )
    return int(parsed.timestamp() * 1000)


def application_id(container_id: str) -> str:
    match = CONTAINER_PARTS_RE.fullmatch(container_id)
    if not match:
        raise ValueError(f"Unexpected container ID {container_id}")
    return f"application_{match.group('cluster')}_{match.group('application')}"


def resource_map(memory: int, vcores: int, suffix: str | None) -> dict[str, int]:
    values = {"memory-mb": int(memory), "vcores": int(vcores)}
    for match in RESOURCE_ENTRY_RE.finditer(suffix or ""):
        values[match.group("name")] = int(match.group("value"))
    return values


def gpu_amount(resources: str | None) -> int:
    return resource_map(0, 0, resources).get("yarn.io/gpu", 0)


def capacity(value: str | None, fallback: int | None, name: str) -> int:
    if value is not None:
        return int(value)
    if fallback is not None:
        return fallback
    raise ValueError(f"Could not determine node {name} capacity")


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
            or file.suffix in {".jsonl", ".log"}
        )
    )


def normalize_log_line(line: str) -> str:
    stripped = line.strip()
    if not stripped.startswith("{"):
        return line
    try:
        record = json.loads(stripped)
    except json.JSONDecodeError:
        return line
    payload = record.get("textPayload")
    if payload is None and isinstance(record.get("jsonPayload"), dict):
        payload = record["jsonPayload"].get("message")
    return str(payload) + "\n" if payload is not None else line


def _record_calculator(evidence: YarnEvidence, calculator: str, source: str) -> None:
    if evidence.calculator_class and evidence.calculator_class != calculator:
        raise ValueError(
            "Conflicting ResourceCalculators: "
            f"{evidence.calculator_class}, {calculator}"
        )
    evidence.calculator_class = calculator
    evidence.calculator_source = source


def _node_instance_type(line: str) -> str:
    match = INSTANCE_TYPE_RE.search(line)
    return match.group("value").strip() if match else ""


def _container_from_match(
    match: re.Match[str], node_id: str, node: Node | None, source: str
) -> Container:
    allocated = resource_map(
        int(match.group("memory")),
        int(match.group("vcores")),
        match.group("resources"),
    )
    node_memory = capacity(
        match.group("max_memory"), node.memory_mb if node else None, "memory"
    )
    node_vcores = capacity(
        match.group("max_vcores"), node.vcores if node else None, "vcore"
    )
    node_resources = dict(node.resources) if node else {
        "memory-mb": node_memory,
        "vcores": node_vcores,
    }
    node_resources.setdefault("memory-mb", node_memory)
    node_resources.setdefault("vcores", node_vcores)
    return Container(
        container_id=match.group("container"),
        application_id=application_id(match.group("container")),
        node_id=node_id,
        start_ms=parse_timestamp(match.group("timestamp")),
        memory_mb=allocated["memory-mb"],
        node_memory_mb=node_memory,
        vcores=allocated["vcores"],
        node_vcores=node_vcores,
        gpus=allocated.get("yarn.io/gpu", 0),
        node_gpus=node_resources.get("yarn.io/gpu", 0),
        resources=allocated,
        node_resources=node_resources,
        source=source,
    )


def parse_yarn_logs(path: Path) -> YarnEvidence:
    """Parse RM/NM daemon logs without relying on a cloud-provider layout."""
    evidence = YarnEvidence()
    rm_finishes: dict[str, int] = {}
    nm_finishes: dict[str, int] = {}
    files = relevant_log_files(path)
    if not files:
        raise ValueError(f"No ResourceManager or NodeManager log files found under {path}")

    for file in files:
        path_node_id = file.parent.name
        path_node = evidence.nodes.setdefault(path_node_id, Node(path_node_id))
        with open_log(file) as handle:
            for raw_line in handle:
                line = normalize_log_line(raw_line)
                summary = APPLICATION_SUMMARY_RE.search(line)
                if summary:
                    app_id = summary.group("application")
                    evidence.application_summaries[app_id] = ApplicationSummary(
                        app_id,
                        summary.group("name"),
                        summary.group("final_status"),
                        int(summary.group("containers")),
                    )
                calculator = CALCULATOR_RE.search(line)
                if calculator:
                    _record_calculator(
                        evidence, calculator.group("calculator"), file.name
                    )
                policy = FAIR_POLICY_RE.search(line)
                if policy:
                    name = policy.group("policy")
                    if evidence.scheduler_policy and evidence.scheduler_policy != name:
                        evidence.accounting_policy_ambiguous = True
                        evidence.warnings.append(
                            "Multiple FairScheduler policies were observed; archive "
                            "queue-specific policy evidence for exact accounting"
                        )
                    else:
                        evidence.scheduler_policy = name
                        mapped = (
                            "DominantResourceCalculator"
                            if name == "DominantResourceFairnessPolicy"
                            else "DefaultResourceCalculator"
                        )
                        _record_calculator(evidence, mapped, file.name)

                rm_node = RM_NODE_RE.search(line)
                if rm_node:
                    host = rm_node.group("host").strip()
                    resources = resource_map(
                        int(rm_node.group("memory")),
                        int(rm_node.group("vcores")),
                        rm_node.group("resources"),
                    )
                    evidence.nodes[host] = Node(
                        node_id=host,
                        instance_type=_node_instance_type(line),
                        memory_mb=resources["memory-mb"],
                        vcores=resources["vcores"],
                        gpus=resources.get("yarn.io/gpu", 0),
                        resources=resources,
                    )
                    continue
                node_match = NODE_RE.search(line)
                if node_match:
                    resources = resource_map(
                        int(node_match.group("memory")),
                        int(node_match.group("vcores")),
                        node_match.group("resources"),
                    )
                    path_node.instance_type = _node_instance_type(line)
                    path_node.memory_mb = resources["memory-mb"]
                    path_node.vcores = resources["vcores"]
                    path_node.gpus = resources.get("yarn.io/gpu", 0)
                    path_node.resources = resources
                    continue
                assignment = RM_ASSIGN_RE.search(line)
                if assignment:
                    host = assignment.group("host")
                    candidate = _container_from_match(
                        assignment, host, evidence.nodes.get(host), "resourcemanager"
                    )
                    evidence.containers[candidate.container_id] = candidate
                    continue
                terminal = RM_TERMINAL_RE.search(line)
                if terminal:
                    container_id = terminal.group("container")
                    finish = parse_timestamp(terminal.group("timestamp"))
                    rm_finishes[container_id] = min(
                        finish, rm_finishes.get(container_id, finish)
                    )
                    continue
                start = START_RE.search(line)
                if start:
                    candidate = _container_from_match(
                        start, path_node_id, path_node, "nodemanager"
                    )
                    evidence.containers.setdefault(candidate.container_id, candidate)
                    continue
                done = DONE_RE.search(line)
                if done:
                    container_id = done.group("container")
                    finish = parse_timestamp(done.group("timestamp"))
                    nm_finishes[container_id] = min(
                        finish, nm_finishes.get(container_id, finish)
                    )

    for container_id, container in evidence.containers.items():
        if container_id in rm_finishes:
            container.finish_ms = rm_finishes[container_id]
            container.finish_source = "resourcemanager"
        elif container_id in nm_finishes:
            container.finish_ms = nm_finishes[container_id]
            container.finish_source = "nodemanager"
    return evidence


def calculator_mode(detected_class: str) -> str:
    if detected_class == "DefaultResourceCalculator":
        return "default"
    if detected_class == "DominantResourceCalculator":
        return "dominant"
    raise ValueError(
        "Could not detect DefaultResourceCalculator or "
        "DominantResourceCalculator from archived scheduler evidence"
    )


def container_node_share(container: Container, mode: str) -> float:
    """Return the YARN-scheduled node share without using Spark core counts."""
    if mode == "default":
        return container.memory_mb / container.node_memory_mb
    if mode != "dominant":
        raise ValueError(f"Unsupported detected calculator mode {mode}")
    allocated_resources = container.resources or {
        "memory-mb": container.memory_mb,
        "vcores": container.vcores,
        "yarn.io/gpu": container.gpus,
    }
    node_resources = container.node_resources or {
        "memory-mb": container.node_memory_mb,
        "vcores": container.node_vcores,
        "yarn.io/gpu": container.node_gpus,
    }
    shares = []
    for name, allocated in allocated_resources.items():
        if allocated <= 0:
            continue
        node_capacity = node_resources.get(name, 0)
        if node_capacity <= 0:
            raise ValueError(
                f"Container {container.container_id} allocates {name} but its "
                "node capacity is missing or zero"
            )
        shares.append(allocated / node_capacity)
    if not shares:
        raise ValueError(f"Container {container.container_id} has no resources")
    return max(shares)


def container_instance_type(evidence: YarnEvidence, container: Container) -> str:
    node = evidence.nodes.get(container.node_id)
    if node and node.instance_type:
        return node.instance_type
    return f"unknown:{container.node_id}"
