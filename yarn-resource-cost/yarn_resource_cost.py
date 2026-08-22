#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Attribute Spark application worker consumption from Spark and YARN logs."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import calculate_yarn_job_cost as reporting
from yarn_job_cost_adapters import (
    ADAPTERS,
    AdapterCommandError,
    PriceCatalog,
    apply_catalog_costs,
    apply_node_class_map,
    load_price_catalog,
    materialize_gcs,
    materialize_hdfs,
    materialize_yarn_logs,
)
from yarn_job_cost_core import calculator_mode, parse_yarn_logs
from yarn_job_cost_dataproc import classify_nodes as classify_dataproc_nodes
from yarn_job_cost_discovery import (
    AwsCliError,
    EventLogApplication,
    materialize_event_metadata_files,
    read_event_log_metadata,
    resolve_emr_log_uri,
)


PORTABLE_FIELDS = (
    "comparison_key",
    "application_id",
    "application_name",
    "final_status",
    "spark_duration_seconds",
    "resource_calculator",
    "container_count",
    "container_seconds",
    "memory_mb_seconds",
    "vcore_seconds",
    "gpu_seconds",
    "node_equivalent_seconds",
    "resource_expression",
    "worker_cost",
    "worker_cost_currency",
    "complete",
    "warnings",
)

COMPARISON_FIELDS = (
    "comparison_key",
    "baseline_application_id",
    "test_application_id",
    "baseline_instance_types",
    "test_instance_types",
    "baseline_wall_clock_seconds",
    "test_wall_clock_seconds",
    "wall_clock_factor",
    "baseline_node_equivalent_seconds",
    "test_node_equivalent_seconds",
    "node_equivalent_factor",
    "baseline_worker_cost",
    "test_worker_cost",
    "worker_cost_factor",
    "baseline_complete",
    "test_complete",
    "warnings",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter", choices=ADAPTERS, required=True)
    parser.add_argument("--event-log-root", required=True)
    parser.add_argument("--yarn-log-root")
    parser.add_argument("--test-event-log-root", action="append", default=[])
    parser.add_argument("--test-yarn-log-root", action="append", default=[])
    parser.add_argument("--node-class-map", type=Path)
    parser.add_argument("--pricing", choices=("none", "catalog", "live"), default="none")
    parser.add_argument("--price-catalog", type=Path)
    parser.add_argument("--aws-profile")
    parser.add_argument("--aws-region")
    parser.add_argument("--gcp-project")
    parser.add_argument("--gcp-region")
    parser.add_argument("--dataproc-cluster")
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument(
        "--cache-dir", type=Path, default=Path(".cache/yarn-resource-cost")
    )
    parser.add_argument("--include-application-master", action="store_true")
    parser.add_argument(
        "--comparison-key", choices=("application-name", "application-id", "regex"),
        default="application-name",
    )
    parser.add_argument("--comparison-key-regex")
    parser.add_argument(
        "--sort-by", choices=("comparison-key", "wall-clock-factor", "cost-factor"),
        default="comparison-key",
    )
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--output-json", type=Path)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.pricing == "catalog" and not args.price_catalog:
        raise ValueError("--pricing catalog requires --price-catalog")
    if args.pricing != "catalog" and args.price_catalog:
        raise ValueError("--price-catalog requires --pricing catalog")
    if args.pricing == "live" and args.adapter != "emr":
        raise ValueError("Live pricing is currently supported only by --adapter emr")
    if args.dataproc_cluster and args.adapter != "dataproc":
        raise ValueError("--dataproc-cluster requires --adapter dataproc")
    if args.dataproc_cluster and not args.gcp_region:
        raise ValueError("--dataproc-cluster requires --gcp-region")
    if args.comparison_key == "regex" and not args.comparison_key_regex:
        raise ValueError("--comparison-key regex requires --comparison-key-regex")
    if args.test_yarn_log_root and (
        len(args.test_yarn_log_root) != len(args.test_event_log_root)
    ):
        raise ValueError(
            "--test-yarn-log-root must be supplied once per --test-event-log-root"
        )
    if args.adapter != "emr" and not args.yarn_log_root:
        raise ValueError(f"--adapter {args.adapter} requires --yarn-log-root")


def materialize_event_root(root: str, args: argparse.Namespace) -> tuple[Path, set[str]]:
    event_cache = args.cache_dir / "spark-events"
    if root.startswith("gs://"):
        if args.adapter != "dataproc":
            raise ValueError("gs:// Spark event logs require --adapter dataproc")
        local = materialize_gcs(root, event_cache, args.refresh_cache)
        return materialize_event_metadata_files(local.as_posix(), event_cache, None, None)
    if root.startswith("hdfs://"):
        if args.adapter != "on-prem":
            raise ValueError("HDFS Spark event logs require --adapter on-prem")
        local = materialize_hdfs(root, event_cache, args.refresh_cache)
        return materialize_event_metadata_files(local.as_posix(), event_cache, None, None)
    return materialize_event_metadata_files(
        root, event_cache, args.aws_profile, args.aws_region
    )


def resolve_yarn_root(
    explicit: str | None,
    applications: dict[str, EventLogApplication],
    args: argparse.Namespace,
) -> tuple[str, str]:
    if explicit:
        return explicit, "explicit"
    if args.adapter != "emr":
        raise ValueError(f"--adapter {args.adapter} requires an explicit YARN log root")
    cluster_ids = {app.cluster_id for app in applications.values() if app.cluster_id}
    if len(cluster_ids) != 1:
        raise ValueError(
            "Spark event logs must identify exactly one EMR cluster; found: "
            + (", ".join(sorted(cluster_ids)) or "none")
        )
    cluster_id = next(iter(cluster_ids))
    return (
        resolve_emr_log_uri(cluster_id, args.aws_profile, args.aws_region),
        f"spark.emr.clusterId={cluster_id}",
    )


def application_key(application: dict, args: argparse.Namespace) -> str:
    if args.comparison_key == "application-id":
        return str(application.get("application_id") or "")
    name = str(application.get("application_name") or application.get("job name") or "")
    if args.comparison_key == "application-name":
        return name
    match = re.search(args.comparison_key_regex, name)
    if not match:
        return ""
    if "key" in match.groupdict():
        return str(match.group("key"))
    return str(match.group(1) if match.groups() else match.group())


def summarize_applications(applications: list[dict]) -> dict:
    complete = [app for app in applications if app.get("complete") is True]
    return {
        "application_count": len(applications),
        "complete_application_count": len(complete),
        "incomplete_application_count": len(applications) - len(complete),
        "node_equivalent_seconds": round(
            sum(float(app.get("node_equivalent_seconds") or 0) for app in complete), 6
        ),
        "worker_cost": round(
            sum(float(app.get("worker_cost") or 0) for app in complete), 8
        ),
    }


def analyze_run(
    event_root: str,
    yarn_root: str | None,
    args: argparse.Namespace,
    catalog: PriceCatalog | None,
) -> dict:
    local_events, selected_ids = materialize_event_root(event_root, args)
    event_metadata = read_event_log_metadata(local_events)
    missing = selected_ids - set(event_metadata)
    if missing:
        raise ValueError("Missing Spark event-log metadata for: " + ", ".join(sorted(missing)))
    resolved_yarn_root, discovery_source = resolve_yarn_root(
        yarn_root, event_metadata, args
    )
    local_yarn = materialize_yarn_logs(
        args.adapter,
        resolved_yarn_root,
        args.cache_dir / "yarn-logs",
        args.refresh_cache,
        args.aws_profile,
    )
    evidence = parse_yarn_logs(local_yarn)
    node_classification: dict[str, object] = {
        "source": "node-class-map" if args.node_class_map else "daemon-log attributes"
    }
    if args.adapter == "dataproc" and args.dataproc_cluster:
        node_classification = classify_dataproc_nodes(
            evidence, args.dataproc_cluster, args.gcp_region, args.gcp_project
        )
    apply_node_class_map(evidence, args.node_class_map)
    mode = calculator_mode(evidence.calculator_class)
    metadata = {app_id: app.as_metadata() for app_id, app in event_metadata.items()}
    executor_containers = {
        executor.container_id
        for app in event_metadata.values()
        for executor in app.executors.values()
        if executor.container_id
    }
    applications = reporting.calculate_applications(
        evidence,
        mode,
        metadata,
        args.include_application_master,
        executor_containers,
    )
    applications = [app for app in applications if app["application_id"] in selected_ids]
    missing_yarn = selected_ids - {app["application_id"] for app in applications}
    if missing_yarn:
        raise ValueError("Applications missing from YARN logs: " + ", ".join(sorted(missing_yarn)))
    reporting.add_task_packing_metrics(applications, event_metadata, evidence, mode)
    pricing_provenance: dict[str, object] = {"mode": args.pricing}
    if args.pricing == "catalog":
        apply_catalog_costs(applications, catalog)
        pricing_provenance.update(catalog.provenance)
    elif args.pricing == "live":
        region = reporting.resolve_aws_region(args.aws_region, args.aws_profile)
        prices = reporting.add_ondemand_costs(applications, region, args.aws_profile)
        for app in applications:
            app["worker_cost"] = app.get("ec2_plus_emr_usd", "")
            app["worker_cost_currency"] = "USD"
        pricing_provenance = {
            "mode": "live",
            "provider": "aws",
            "region": region,
            "queried_at_utc": datetime.now(timezone.utc).isoformat(),
            "components": prices,
        }
    else:
        for app in applications:
            app["worker_cost"] = ""
            app["worker_cost_currency"] = ""
    for app in applications:
        app["comparison_key"] = application_key(app, args)
        app["resource_expression"] = app.get("cost_expression", "")
    return {
        "schema_version": 1,
        "adapter": args.adapter,
        "event_log_root": event_root,
        "yarn_log_root": resolved_yarn_root,
        "yarn_log_discovery_source": discovery_source,
        "resource_calculator": mode,
        "calculator_evidence": {
            "class": evidence.calculator_class,
            "source": evidence.calculator_source,
            "fair_scheduler_policy": evidence.scheduler_policy,
        },
        "pricing": pricing_provenance,
        "node_classification": node_classification,
        "nodes": {
            node_id: {
                "node_class": node.node_class,
                "resources": node.resources or {
                    "memory-mb": node.memory_mb,
                    "vcores": node.vcores,
                    "yarn.io/gpu": node.gpus,
                },
            }
            for node_id, node in sorted(evidence.nodes.items())
        },
        "applications": applications,
        "summary": summarize_applications(applications),
    }


def factor(test: object, baseline: object) -> float | str:
    if baseline in ("", None, 0) or test in ("", None):
        return ""
    return round(float(test) / float(baseline), 8)


def instance_types(application: dict) -> str:
    return " + ".join(sorted(application["node_equivalent_seconds_by_instance_type"]))


def compare_runs(baseline: dict, tests: list[dict], args: argparse.Namespace) -> list[dict]:
    baseline_index = {
        app["comparison_key"]: app for app in baseline["applications"] if app["comparison_key"]
    }
    test_index: dict[str, dict] = {}
    for run in tests:
        for app in run["applications"]:
            if app["comparison_key"]:
                test_index[app["comparison_key"]] = app
    rows = []
    for key in sorted(set(baseline_index) | set(test_index)):
        base = baseline_index.get(key)
        test = test_index.get(key)
        complete = bool(base and test and base.get("complete") and test.get("complete"))
        base_cost = base.get("worker_cost", "") if complete else ""
        test_cost = test.get("worker_cost", "") if complete else ""
        row = {
            "comparison_key": key,
            "baseline_application_id": base.get("application_id", "") if base else "",
            "test_application_id": test.get("application_id", "") if test else "",
            "baseline_instance_types": instance_types(base) if base else "",
            "test_instance_types": instance_types(test) if test else "",
            "baseline_wall_clock_seconds": base.get("spark_duration_seconds", "") if base else "",
            "test_wall_clock_seconds": test.get("spark_duration_seconds", "") if test else "",
            "baseline_node_equivalent_seconds": (
                base.get("node_equivalent_seconds", "") if base else ""
            ),
            "test_node_equivalent_seconds": test.get("node_equivalent_seconds", "") if test else "",
            "baseline_worker_cost": base_cost,
            "test_worker_cost": test_cost,
            "baseline_complete": base.get("complete", False) if base else False,
            "test_complete": test.get("complete", False) if test else False,
            "warnings": " | ".join(
                (["missing baseline application"] if not base else [])
                + (["missing test application"] if not test else [])
            ),
        }
        row["wall_clock_factor"] = factor(
            row["test_wall_clock_seconds"], row["baseline_wall_clock_seconds"]
        )
        row["node_equivalent_factor"] = (
            factor(
                row["test_node_equivalent_seconds"],
                row["baseline_node_equivalent_seconds"],
            )
            if complete
            else ""
        )
        row["worker_cost_factor"] = factor(test_cost, base_cost)
        rows.append(row)
    sort_field = {
        "comparison-key": "comparison_key",
        "wall-clock-factor": "wall_clock_factor",
        "cost-factor": "worker_cost_factor",
    }[args.sort_by]
    return sorted(rows, key=lambda row: (row[sort_field] == "", row[sort_field]))


def write_csv(path: Path, rows: list[dict], fields: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for source in rows:
            row = dict(source)
            if isinstance(row.get("warnings"), list):
                row["warnings"] = " | ".join(row["warnings"])
            writer.writerow(row)


def print_table(rows: list[dict], fields: tuple[str, ...]) -> None:
    rendered = [[str(row.get(field, "")) for field in fields] for row in rows]
    widths = [
        max(len(field), *(len(row[index]) for row in rendered))
        for index, field in enumerate(fields)
    ]
    print("  ".join(field.ljust(widths[i]) for i, field in enumerate(fields)))
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


def main() -> int:
    args = parse_args()
    validate_args(args)
    catalog = load_price_catalog(args.price_catalog) if args.price_catalog else None
    baseline = analyze_run(args.event_log_root, args.yarn_log_root, args, catalog)
    test_runs = []
    for index, root in enumerate(args.test_event_log_root):
        explicit = args.test_yarn_log_root[index] if args.test_yarn_log_root else None
        test_runs.append(analyze_run(root, explicit, args, catalog))
    if test_runs:
        rows = compare_runs(baseline, test_runs, args)
        fields = COMPARISON_FIELDS
        payload: dict = {
            "schema_version": 1,
            "baseline": baseline,
            "test_runs": test_runs,
            "comparison": rows,
        }
    else:
        rows = baseline["applications"]
        fields = PORTABLE_FIELDS
        payload = baseline
    if args.output_csv:
        write_csv(args.output_csv, rows, fields)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if not args.output_csv and not args.output_json:
        print_table(rows, fields)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AdapterCommandError, AwsCliError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
