#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Dataproc cluster metadata adapter."""

from __future__ import annotations

import json

from yarn_job_cost_adapters import run_command
from yarn_job_cost_core import YarnEvidence


def _last_uri_component(value: object) -> str:
    return str(value or "").rstrip("/").rsplit("/", 1)[-1]


def _node_class(config: dict) -> str:
    machine = _last_uri_component(config.get("machineTypeUri"))
    accelerators = []
    for accelerator in config.get("accelerators") or []:
        name = _last_uri_component(accelerator.get("acceleratorTypeUri"))
        count = int(accelerator.get("acceleratorCount") or 0)
        if name and count:
            accelerators.append(f"{count}x{name}")
    suffix = "+" + "+".join(sorted(accelerators)) if accelerators else ""
    return f"gcp:dataproc:{machine}{suffix}" if machine else ""


def describe_node_classes(
    cluster: str, region: str, project: str | None
) -> tuple[dict[str, str], dict[str, object]]:
    command = [
        "gcloud", "dataproc", "clusters", "describe", cluster,
        "--region", region, "--format", "json",
    ]
    if project:
        command += ["--project", project]
    payload = json.loads(run_command(command).stdout)
    config = payload.get("config") or {}
    mappings: dict[str, str] = {}
    for group_name in ("workerConfig", "secondaryWorkerConfig"):
        group = config.get(group_name) or {}
        node_class = _node_class(group)
        for name in group.get("instanceNames") or []:
            if node_class:
                mappings[str(name)] = node_class
    provenance = {
        "cluster": cluster,
        "region": region,
        "project": project or payload.get("projectId") or "",
        "cluster_uuid": payload.get("clusterUuid") or "",
        "source": "gcloud dataproc clusters describe",
    }
    return mappings, provenance


def classify_nodes(
    evidence: YarnEvidence, cluster: str, region: str, project: str | None
) -> dict[str, object]:
    mappings, provenance = describe_node_classes(cluster, region, project)
    for node_id, node in evidence.nodes.items():
        short = node_id.split(".", 1)[0]
        node.instance_type = mappings.get(node_id) or mappings.get(short) or node.instance_type
    return provenance
