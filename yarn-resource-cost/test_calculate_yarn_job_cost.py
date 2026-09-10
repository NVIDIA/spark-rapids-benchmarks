#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import ast
import contextlib
import csv
import gzip
import importlib.util
import io
import json
import os
import tarfile
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


SCRIPT = Path(__file__).with_name("calculate_yarn_job_cost.py")
DISCOVERY = Path(__file__).with_name("yarn_job_cost_discovery.py")
EVENTLOG_SCRIPT = Path(__file__).with_name("yarn_job_cost_eventlog.py")
DEFAULTS_SCRIPT = Path(__file__).with_name("yarn_job_cost_defaults.py")
APP_ID = "application_123_0002"
SPEC = importlib.util.spec_from_file_location("calculate_yarn_job_cost", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
import yarn_job_cost_eventlog as EVENTLOG
import yarn_job_cost_discovery as DISCOVERY_MODULE


class CalculateYarnJobCostTest(unittest.TestCase):
    def test_scripts_parse_as_python_310_and_311(self):
        for script in (SCRIPT, DISCOVERY, EVENTLOG_SCRIPT, DEFAULTS_SCRIPT):
            source = script.read_text()
            ast.parse(source, filename=str(script), feature_version=(3, 10))
            ast.parse(source, filename=str(script), feature_version=(3, 11))

    def test_aws_cli_error_surfaces_captured_stderr(self):
        failure = subprocess.CompletedProcess(
            ["aws", "--profile", "example-profile", "s3api", "list-objects-v2"],
            255,
            stdout="",
            stderr="Error loading SSO Token: Token has expired",
        )
        with mock.patch.object(MODULE.subprocess, "run", return_value=failure):
            with self.assertRaisesRegex(
                MODULE.AwsCliError, "Error loading SSO Token: Token has expired"
            ) as raised:
                MODULE.run_aws(failure.args)
        self.assertIn("exit code 255", str(raised.exception))
        self.assertIn("--profile example-profile", str(raised.exception))

    def test_emr_logs_are_authoritative_and_csv_only_enriches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            yarn_dir = root / "node" / "i-core" / "applications" / "hadoop-yarn"
            yarn_dir.mkdir(parents=True)
            node_log = yarn_dir / "hadoop-yarn-nodemanager-worker.log.gz"
            lines = [
                "2026-01-01 00:00:00,000 INFO X: Registered with ResourceManager "
                "as worker:8041 with total resource of <memory:100, vCores:4> "
                "and with following Node attribute(s) : "
                "{[nm.yarn.io/instanceType(STRING)=test.1xlarge]}",
                "2026-01-01 00:00:00,500 INFO ApplicationSummary: "
                "appId=application_123_0002,name=test,user=hadoop,queue=root.default,"
                "state=FINISHED,finalStatus=SUCCEEDED,totalAllocatedContainers=3",
                "2026-01-01 00:00:01,000 INFO X: Start request for "
                "container_123_0002_01_000001 by user appattempt_123_0002_000001 "
                "with resource <memory:10, max memory:100, vCores:1, max vCores:4>",
                "2026-01-01 00:00:03,000 INFO X: Container "
                "container_123_0002_01_000001 transitioned from RUNNING to DONE",
                "2026-01-01 00:00:10,000 INFO X: Start request for "
                "container_123_0002_01_000002 by user appattempt_123_0002_000001 "
                "with resource <memory:40, max memory:100, vCores:2, max vCores:4>",
                "2026-01-01 00:00:20,000 INFO X: Container "
                "container_123_0002_01_000002 transitioned from RUNNING to DONE",
                "2026-01-01 00:00:30,000 INFO X: Start request for "
                "container_123_0002_01_000003 by user appattempt_123_0002_000001 "
                "with resource <memory:40, max memory:100, vCores:2, max vCores:4>",
                "2026-01-01 00:00:50,000 INFO X: Container "
                "container_123_0002_01_000003 transitioned from RUNNING to DONE",
            ]
            with gzip.open(node_log, "wt") as handle:
                handle.write("\n".join(lines) + "\n")
            rm_log = yarn_dir / "hadoop-yarn-resourcemanager-master.log.gz"
            with gzip.open(rm_log, "wt") as handle:
                rm_lines = [
                    "2026-01-01 00:00:00,000 INFO CapacityScheduler: "
                    "Initialized CapacityScheduler with calculator=class "
                    "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator, "
                    "minimumAllocation=<memory:1, vCores:1>",
                    "2026-01-01 00:00:00,100 INFO X: NodeManager from node "
                    "worker(cmPort: 8041 httpPort: 8042) registered with capability: "
                    "<memory:100, vCores:4> and attributes "
                    "[nm.yarn.io/instanceType(STRING)=test.1xlarge]",
                ]
                allocations = [
                    ("000001", "00:00:01,000", "00:00:03,000", 10, 1),
                    ("000002", "00:00:10,000", "00:00:20,000", 40, 2),
                    ("000003", "00:00:30,000", "00:00:50,000", 40, 2),
                ]
                for sequence, start, finish, memory, vcores in allocations:
                    container_id = f"container_123_0002_01_{sequence}"
                    rm_lines.extend(
                        [
                            f"2026-01-01 {start} INFO X: Assigned container "
                            f"{container_id} of capacity <memory:{memory}, "
                            f"max memory:100, vCores:{vcores}, max vCores:4> "
                            "on host worker:8041",
                            f"2026-01-01 {finish} INFO X: {container_id} Container "
                            "Transitioned from RUNNING to COMPLETED",
                        ]
                    )
                handle.write("\n".join(rm_lines) + "\n")
            input_csv = root / "input.csv"
            with input_csv.open("w", newline="") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=["job id", "job name", "eventlog\n(benchmark)"]
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "job id": "6",
                        "job name": "test job",
                        "eventlog\n(benchmark)": f"s3://bucket/eventlog_v2_{APP_ID}",
                    }
                )

            output_csv = root / "cost.csv"
            output_json = root / "cost.json"
            price = {
                "usd_per_hour": 3.0,
                "effective_date": "2026-01-01",
                "description": "EC2 fixture",
            }
            emr_price = {
                "usd_per_hour": 0.75,
                "effective_date": "2026-01-01",
                "description": "EMR fixture",
            }
            with mock.patch.object(
                sys,
                "argv",
                [
                    str(SCRIPT),
                    "--emr-log-uri",
                    str(root),
                    "--aws-region",
                    "us-west-2",
                    "--input-csv",
                    str(input_csv),
                    "--output-csv",
                    str(output_csv),
                    "--output-json",
                    str(output_json),
                ],
            ), mock.patch.object(
                MODULE, "current_ondemand_hourly_price", return_value=price
            ), mock.patch.object(
                MODULE, "current_emr_hourly_price", return_value=emr_price
            ):
                self.assertEqual(0, MODULE.main())
            with output_csv.open(newline="") as handle:
                row = next(csv.DictReader(handle))
            self.assertEqual(row["application_id"], APP_ID)
            self.assertEqual(row["job id"], "6")
            self.assertEqual(row["job name"], "test job")
            self.assertEqual(int(row["container_count"]), 2)
            self.assertEqual(float(row["container_seconds"]), 30.0)
            self.assertEqual(float(row["node_equivalent_seconds"]), 12.0)
            self.assertEqual(float(row["instance_vcore_seconds"]), 48.0)
            self.assertEqual(
                row["instance_vcore_seconds_expression"],
                "48.000000 test.1xlarge-vcore-seconds",
            )
            self.assertEqual(float(row["ec2_ondemand_usd"]), 0.01)
            self.assertEqual(float(row["emr_usd"]), 0.0025)
            self.assertEqual(float(row["ec2_plus_emr_usd"]), 0.0125)
            self.assertEqual(row["resource_calculator"], "default")
            self.assertEqual(row["complete"], "True")
            payload = json.loads(output_json.read_text())
            self.assertEqual("us-west-2", payload["pricing_product_region"])
            self.assertEqual(
                3.0,
                payload["ec2_ondemand_prices"]["test.1xlarge"]["usd_per_hour"],
            )
            self.assertEqual(
                0.75,
                payload["emr_prices"]["test.1xlarge"]["usd_per_hour"],
            )
            self.assertEqual(
                0.01, payload["applications"][0]["ec2_ondemand_usd"]
            )
            self.assertEqual(
                0.0025, payload["applications"][0]["emr_usd"]
            )
            self.assertEqual(
                0.0125, payload["applications"][0]["ec2_plus_emr_usd"]
            )
            self.assertEqual(
                48.0, payload["summary"]["total_instance_vcore_seconds"]
            )
            self.assertEqual(
                "48.000000 test.1xlarge-vcore-seconds",
                payload["summary"][
                    "total_instance_vcore_seconds_expression"
                ],
            )
            self.assertEqual(
                "dominant",
                MODULE.calculator_mode("DominantResourceCalculator"),
            )
            with self.assertRaises(ValueError):
                MODULE.calculator_mode("")

            stdout = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                [
                    str(SCRIPT),
                    "--emr-log-uri",
                    str(root),
                    "--aws-region",
                    "us-west-2",
                ],
            ), mock.patch.object(
                MODULE, "current_ondemand_hourly_price", return_value=price
            ), mock.patch.object(
                MODULE, "current_emr_hourly_price", return_value=emr_price
            ), contextlib.redirect_stdout(stdout):
                self.assertEqual(0, MODULE.main())
            console = stdout.getvalue()
            self.assertIn("Job ID", console)
            self.assertIn("Node-equivalent sec", console)
            self.assertIn("EC2+EMR USD", console)
            self.assertIn("12.000000 test.1xlarge-seconds", console)
            self.assertIn(
                "48.000000 test.1xlarge-vcore-seconds", console
            )
            self.assertIn("0.01", console)
            self.assertIn(APP_ID, console)

    def test_single_run_does_not_price_incomplete_ledgers(self):
        applications = [
            {
                "complete": False,
                "node_equivalent_seconds_by_instance_type": {
                    "test.1xlarge": 12.0
                },
            }
        ]
        with mock.patch.object(
            MODULE, "current_ondemand_hourly_price"
        ) as ec2_lookup, mock.patch.object(
            MODULE, "current_emr_hourly_price"
        ) as emr_lookup:
            prices = MODULE.add_ondemand_costs(
                applications, "us-west-2", "example-profile"
            )
        self.assertEqual({"ec2": {}, "emr": {}}, prices)
        self.assertEqual("", applications[0]["ec2_ondemand_usd"])
        self.assertEqual("", applications[0]["emr_usd"])
        self.assertEqual("", applications[0]["ec2_plus_emr_usd"])
        ec2_lookup.assert_not_called()
        emr_lookup.assert_not_called()

    def test_rm_terminal_precedes_nm_done_and_nm_fallback_is_not_final(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            yarn_dir = root / "node" / "i-core" / "applications" / "hadoop-yarn"
            yarn_dir.mkdir(parents=True)
            rm_log = yarn_dir / "hadoop-yarn-resourcemanager-master.log.gz"
            rm_lines = [
                "2026-01-01 00:00:00,000 INFO X: NodeManager from node "
                "worker(cmPort: 8041 httpPort: 8042) registered with capability: "
                "<memory:100, vCores:4> and attributes "
                "[nm.yarn.io/instanceType(STRING)=test.1xlarge]",
                "2026-01-01 00:00:00,100 INFO ApplicationSummary: "
                f"appId={APP_ID},name=test,user=hadoop,queue=root.default,"
                "state=FINISHED,finalStatus=SUCCEEDED,totalAllocatedContainers=2",
                "2026-01-01 00:00:00,200 INFO CapacityScheduler: "
                "Initialized CapacityScheduler with calculator=class "
                "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator, "
                "minimumAllocation=<memory:1, vCores:1>",
                "2026-01-01 00:00:01,000 INFO X: Assigned container "
                "container_123_0002_01_000002 of capacity <memory:50, "
                "max memory:100, vCores:2, max vCores:4> on host worker:8041",
                "2026-01-01 00:00:05,000 INFO X: "
                "container_123_0002_01_000002 Container Transitioned from "
                "RUNNING to RELEASED",
                "2026-01-01 00:00:02,000 INFO X: Assigned container "
                "container_123_0002_01_000003 of capacity <memory:50, "
                "max memory:100, vCores:2, max vCores:4> on host worker:8041",
            ]
            with gzip.open(rm_log, "wt") as handle:
                handle.write("\n".join(rm_lines) + "\n")
            nm_log = yarn_dir / "hadoop-yarn-nodemanager-worker.log.gz"
            nm_lines = [
                "2026-01-01 00:00:10,000 INFO X: Container "
                "container_123_0002_01_000002 transitioned from RUNNING to DONE",
                "2026-01-01 00:00:08,000 INFO X: Container "
                "container_123_0002_01_000003 transitioned from RUNNING to DONE",
            ]
            with gzip.open(nm_log, "wt") as handle:
                handle.write("\n".join(nm_lines) + "\n")

            evidence = MODULE.parse_yarn_logs(root)
            rm_container = evidence.containers["container_123_0002_01_000002"]
            nm_container = evidence.containers["container_123_0002_01_000003"]
            self.assertEqual(
                MODULE.parse_timestamp("2026-01-01 00:00:05,000"),
                rm_container.finish_ms,
            )
            self.assertEqual("resourcemanager", rm_container.finish_source)
            self.assertEqual(
                MODULE.parse_timestamp("2026-01-01 00:00:08,000"),
                nm_container.finish_ms,
            )
            self.assertEqual("nodemanager", nm_container.finish_source)

            result = MODULE.calculate_applications(evidence, "default", {}, False)[0]
            self.assertEqual(10.0, result["container_seconds"])
            self.assertEqual(1, result["nodemanager_finish_fallback_container_count"])
            self.assertFalse(result["complete"])
            self.assertTrue(result["retryable"])
            self.assertIn("NodeManager DONE fallback", " | ".join(result["warnings"]))

    def test_ambiguous_accounting_policy_is_not_retryable(self):
        container = MODULE.Container(
            container_id="container_123_0002_01_000002",
            application_id=APP_ID,
            node_id="worker",
            start_ms=1000,
            finish_ms=2000,
            memory_mb=40,
            node_memory_mb=100,
            vcores=1,
            node_vcores=4,
            source="resourcemanager",
            finish_source="resourcemanager",
        )
        summary = MODULE.ApplicationSummary(APP_ID, "test", "SUCCEEDED", 1)

        evidence = MODULE.YarnEvidence(
            nodes={"worker": MODULE.Node("worker", "cpu.test", 100, 4, 0)},
            containers={container.container_id: container},
            calculator_class="DefaultResourceCalculator",
            accounting_policy_ambiguous=True,
            application_summaries={APP_ID: summary},
        )

        result = MODULE.calculate_applications(evidence, "default", {}, False)[0]

        self.assertFalse(result["complete"])
        self.assertFalse(result["retryable"])

    def test_emr_log_cache_can_be_refreshed(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            uri = "s3://bucket/logs/j-TEST"
            digest = MODULE.hashlib.sha256(uri.encode()).hexdigest()[:16]
            target = cache / f"j-TEST-{digest}"
            target.mkdir(parents=True)
            marker = target / ".download-complete"
            marker.write_text(uri + "\n")
            (target / "hadoop-yarn-resourcemanager.log").write_text("fixture\n")

            with mock.patch.object(MODULE.subprocess, "run") as run:
                self.assertEqual(
                    target,
                    MODULE.materialize_emr_logs(uri, cache, None),
                )
                run.assert_not_called()

            with mock.patch.object(MODULE.subprocess, "run") as run:
                self.assertEqual(
                    target,
                    MODULE.materialize_emr_logs(uri, cache, None, refresh=True),
                )
                run.assert_called_once()
                self.assertTrue(marker.is_file())

            with mock.patch.object(
                MODULE.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, ["aws"]),
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    MODULE.materialize_emr_logs(uri, cache, None, refresh=True)
                self.assertFalse(marker.exists())

    def test_dominant_resource_cost_includes_gpu_share(self):
        assignment = MODULE.RM_ASSIGN_RE.search(
            "2026-07-24 23:39:05,028 INFO X: Assigned container "
            "container_123_0002_01_000002 of capacity "
            "<memory:49152, vCores:15, yarn.io/gpu: 1> on host worker:8041"
        )
        self.assertIsNotNone(assignment)
        self.assertEqual(1, MODULE.gpu_amount(assignment.group("resources")))

        container = MODULE.Container(
            container_id="container_123_0002_01_000002",
            application_id=APP_ID,
            node_id="worker",
            start_ms=1000,
            finish_ms=11000,
            memory_mb=49152,
            node_memory_mb=53248,
            vcores=15,
            node_vcores=16,
            gpus=1,
            node_gpus=1,
            source="resourcemanager",
            finish_source="resourcemanager",
        )
        evidence = MODULE.YarnEvidence(
            nodes={"worker": MODULE.Node("worker", "g6.4xlarge", 53248, 16, 1)},
            containers={container.container_id: container},
            calculator_class="DominantResourceCalculator",
            application_summaries={
                APP_ID: MODULE.ApplicationSummary(APP_ID, "test", "SUCCEEDED", 1)
            },
        )
        result = MODULE.calculate_applications(evidence, "dominant", {}, False)[0]
        self.assertEqual(10.0, result["gpu_seconds"])
        self.assertEqual(10.0, result["node_equivalent_seconds"])
        self.assertEqual(160.0, result["instance_vcore_seconds"])
        self.assertEqual(
            "160.000000 g6.4xlarge-vcore-seconds",
            result["instance_vcore_seconds_expression"],
        )
        self.assertEqual("10.000000 g6.4xlarge-seconds", result["cost_expression"])

    def test_comparison_joins_job_ids_and_calculates_deltas(self):
        baseline = [
            {
                "job id": "6",
                "application_id": "application_base_6",
                "final_status": "SUCCEEDED",
                "spark_duration_seconds": 100.0,
                "node_equivalent_seconds": 200.0,
                "node_equivalent_seconds_by_instance_type": {"cpu.test": 200.0},
                "instance_vcore_seconds": 19200.0,
                "complete": True,
                "warnings": [],
            },
            {
                "job id": "8",
                "application_id": "application_base_8",
                "final_status": "SUCCEEDED",
                "spark_duration_seconds": 50.0,
                "node_equivalent_seconds": 50.0,
                "node_equivalent_seconds_by_instance_type": {"cpu.test": 50.0},
                "complete": True,
                "warnings": [],
            },
        ]
        test = [
            {
                "job id": "6",
                "application_id": "application_test_6",
                "final_status": "SUCCEEDED",
                "spark_duration_seconds": 80.0,
                "node_equivalent_seconds": 300.0,
                "node_equivalent_seconds_by_instance_type": {"gpu.test": 300.0},
                "instance_vcore_seconds": 4800.0,
                "complete": True,
                "warnings": [],
            },
            {
                "job id": "9",
                "application_id": "application_test_9",
                "final_status": "KILLED",
                "spark_duration_seconds": "",
                "node_equivalent_seconds": 10.0,
                "node_equivalent_seconds_by_instance_type": {"gpu.test": 10.0},
                "complete": True,
                "warnings": [],
            },
        ]
        prices = {
            "ec2": {
                "cpu.test": {"usd_per_hour": 7.2},
                "gpu.test": {"usd_per_hour": 1.2},
            },
            "emr": {
                "cpu.test": {"usd_per_hour": 1.8},
                "gpu.test": {"usd_per_hour": 0.3},
            },
        }
        rows = MODULE.build_comparison_rows(baseline, test, prices)
        self.assertEqual(["6", "8", "9"], [row["job_id"] for row in rows])
        matched = rows[0]
        self.assertEqual("cpu.test", matched["baseline_instance_type"])
        self.assertEqual("gpu.test", matched["test_instance_type"])
        self.assertEqual(-20.0, matched["wall_clock_delta_seconds"])
        self.assertEqual(0.8, matched["wall_clock_factor"])
        self.assertEqual(100.0, matched["node_equivalent_delta_seconds"])
        self.assertEqual(19200.0, matched["baseline_instance_vcore_seconds"])
        self.assertEqual(4800.0, matched["test_instance_vcore_seconds"])
        self.assertEqual(0.25, matched["instance_vcore_seconds_factor"])
        self.assertEqual(-14400.0, matched["instance_vcore_seconds_delta"])
        self.assertEqual(0.4, matched["baseline_ec2_ondemand_usd"])
        self.assertEqual(0.1, matched["test_ec2_ondemand_usd"])
        self.assertEqual(0.25, matched["ec2_ondemand_cost_factor"])
        self.assertEqual(-0.3, matched["ec2_ondemand_delta_usd"])
        self.assertEqual(0.1, matched["baseline_emr_usd"])
        self.assertEqual(0.025, matched["test_emr_usd"])
        self.assertEqual(0.5, matched["baseline_ec2_plus_emr_usd"])
        self.assertEqual(0.125, matched["test_ec2_plus_emr_usd"])
        self.assertEqual(0.25, matched["ec2_plus_emr_cost_factor"])
        self.assertEqual(-0.375, matched["ec2_plus_emr_delta_usd"])
        self.assertEqual("MISSING", rows[1]["test_final_status"])
        self.assertEqual("MISSING", rows[2]["baseline_final_status"])
        self.assertEqual(
            ["6", "8", "9"],
            [
                row["job_id"]
                for row in MODULE.sort_comparison_rows(rows, "wall-clock-factor")
            ],
        )
        self.assertEqual(
            ["6", "8", "9"],
            [
                row["job_id"]
                for row in MODULE.sort_comparison_rows(rows, "cost-factor")
            ],
        )
        sortable = [
            {
                "job_id": "1",
                "baseline_final_status": "SUCCEEDED",
                "test_final_status": "SUCCEEDED",
                "wall_clock_factor": 2.0,
                "ec2_plus_emr_cost_factor": 0.5,
            },
            {
                "job_id": "2",
                "baseline_final_status": "SUCCEEDED",
                "test_final_status": "SUCCEEDED",
                "wall_clock_factor": 0.5,
                "ec2_plus_emr_cost_factor": 2.0,
            },
            {
                "job_id": "3",
                "baseline_final_status": "SUCCEEDED",
                "test_final_status": "KILLED",
                "wall_clock_factor": 0.1,
                "ec2_plus_emr_cost_factor": 0.1,
            },
        ]
        self.assertEqual(
            ["2", "1", "3"],
            [
                row["job_id"]
                for row in MODULE.sort_comparison_rows(
                    sortable, "wall-clock-factor"
                )
            ],
        )
        self.assertEqual(
            ["1", "2", "3"],
            [
                row["job_id"]
                for row in MODULE.sort_comparison_rows(sortable, "cost-factor")
            ],
        )
        self.assertEqual(
            "a.test, z.test",
            MODULE.application_instance_types(
                {
                    "node_equivalent_seconds_by_instance_type": {
                        "z.test": 1.0,
                        "a.test": 2.0,
                    }
                }
            ),
        )

    def test_comparison_does_not_price_incomplete_ledgers(self):
        def application(job_id, instance_type, complete, warning):
            return {
                "job id": str(job_id),
                "application_id": f"application_{instance_type}_{job_id}",
                "final_status": "SUCCEEDED",
                "spark_duration_seconds": 10.0,
                "node_equivalent_seconds": 20.0,
                "node_equivalent_seconds_by_instance_type": {instance_type: 20.0},
                "complete": complete,
                "warnings": [warning] if warning else [],
            }

        baseline = [
            application(1, "cpu.test", False, "baseline partial"),
            application(2, "cpu.test", True, ""),
        ]
        test = [
            application(1, "gpu.test", True, ""),
            application(2, "gpu.test", False, "test partial"),
        ]
        prices = {
            "ec2": {
                "cpu.test": {"usd_per_hour": 7.2},
                "gpu.test": {"usd_per_hour": 1.8},
            },
            "emr": {
                "cpu.test": {"usd_per_hour": 1.8},
                "gpu.test": {"usd_per_hour": 0.45},
            },
        }

        rows = MODULE.build_comparison_rows(baseline, test, prices)

        self.assertFalse(rows[0]["baseline_complete"])
        self.assertEqual("baseline partial", rows[0]["baseline_warnings"])
        self.assertEqual("", rows[0]["baseline_ec2_ondemand_usd"])
        self.assertNotEqual("", rows[0]["test_ec2_ondemand_usd"])
        self.assertEqual("", rows[0]["ec2_ondemand_cost_factor"])
        self.assertEqual("", rows[0]["baseline_emr_usd"])
        self.assertNotEqual("", rows[0]["test_emr_usd"])
        self.assertEqual("", rows[0]["baseline_ec2_plus_emr_usd"])
        self.assertNotEqual("", rows[0]["test_ec2_plus_emr_usd"])
        self.assertEqual("", rows[0]["ec2_plus_emr_cost_factor"])
        self.assertFalse(rows[1]["test_complete"])
        self.assertEqual("test partial", rows[1]["test_warnings"])
        self.assertNotEqual("", rows[1]["baseline_ec2_ondemand_usd"])
        self.assertEqual("", rows[1]["test_ec2_ondemand_usd"])
        self.assertEqual("", rows[1]["ec2_ondemand_delta_usd"])
        self.assertNotEqual("", rows[1]["baseline_emr_usd"])
        self.assertEqual("", rows[1]["test_emr_usd"])
        self.assertNotEqual("", rows[1]["baseline_ec2_plus_emr_usd"])
        self.assertEqual("", rows[1]["test_ec2_plus_emr_usd"])
        self.assertEqual("", rows[1]["ec2_plus_emr_delta_usd"])

    def test_roots_overlay_in_order_and_last_root_wins(self):
        roots = ["s3://bucket/base", "s3://bucket/patch"]
        analyses = [
            {
                "applications": [
                    {"job id": "6", "application_id": "application_old_6"},
                    {"job id": "8", "application_id": "application_8"},
                ]
            },
            {
                "applications": [
                    {"job id": "6", "application_id": "application_new_6"},
                    {"job id": "9", "application_id": "application_9"},
                ]
            },
        ]

        merged, overrides = MODULE.merge_test_analyses(roots, analyses)

        self.assertEqual(["6", "8", "9"], [row["job id"] for row in merged])
        self.assertEqual("application_new_6", merged[0]["application_id"])
        self.assertEqual("s3://bucket/patch", merged[0]["source_event_log_root"])
        self.assertEqual(
            [
                {
                    "job_id": "6",
                    "replaced_application_id": "application_old_6",
                    "replaced_event_log_root": "s3://bucket/base",
                    "winning_application_id": "application_new_6",
                    "winning_event_log_root": "s3://bucket/patch",
                }
            ],
            overrides,
        )

    def test_event_logs_derive_identity_cluster_versions_and_duration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixtures = [
                (
                    "application_123_0002",
                    "/run/005_hitch.source_triplify/job.py",
                    "plain plan without a benchmark table",
                ),
                (
                    "application_123_0003",
                    "/run/006_hitch.source_triplify__rewrite/job.py",
                    "scan benchmark_100pct.j0380__input",
                ),
            ]
            for offset, (app_id, app_name, detail) in enumerate(fixtures):
                app_dir = root / f"eventlog_v2_{app_id}"
                app_dir.mkdir()
                events = [
                    {"Event": "SparkListenerLogStart", "Spark Version": "4.0.2-amzn-0"},
                    {
                        "Event": "SparkListenerEnvironmentUpdate",
                        "Spark Properties": {
                            "spark.emr.clusterId": "j-TEST",
                            "spark.emr.releaseLabel": "emr-spark-8.0.0",
                        },
                    },
                    {
                        "Event": "SparkListenerApplicationStart",
                        "App ID": app_id,
                        "App Name": app_name,
                        "Timestamp": 1000 + offset,
                    },
                    {"Event": "SyntheticPlan", "detail": detail},
                    {"Event": "SparkListenerApplicationEnd", "Timestamp": 6000 + offset},
                ]
                (app_dir / f"events_1_{app_id}").write_text(
                    "\n".join(json.dumps(event) for event in events) + "\n"
                )

            metadata = MODULE.read_event_log_metadata(root)
            self.assertEqual("380", metadata["application_123_0002"].job_id)
            self.assertEqual("900380", metadata["application_123_0003"].job_id)
            self.assertEqual("j-TEST", metadata["application_123_0002"].cluster_id)
            self.assertEqual(
                "emr-spark-8.0.0", metadata["application_123_0002"].emr_release_label
            )
            self.assertEqual("4.0.2-amzn-0", metadata["application_123_0002"].spark_version)
            self.assertEqual(
                5.0,
                metadata["application_123_0002"].as_metadata()["spark_duration_seconds"],
            )


    def test_event_logs_sum_successful_task_duration_and_require_all_segments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            app_dir = root / f"eventlog_v2_{APP_ID}"
            app_dir.mkdir()
            first = [
                {
                    "Event": "SparkListenerEnvironmentUpdate",
                    "Spark Properties": {
                        "spark.executor.cores": "4",
                        "spark.task.cpus": "1",
                    },
                },
                {
                    "Event": "SparkListenerApplicationStart",
                    "App ID": APP_ID,
                    "App Name": "/run/j0144__job.py",
                    "Timestamp": 1000,
                },
                {
                    "Event": "SparkListenerExecutorAdded",
                    "Executor ID": "7",
                    "Executor Info": {
                        "Total Cores": 4,
                        "Resource Profile Id": 0,
                        "Attributes": {
                            "CONTAINER_ID": "container_123_0002_01_000002"
                        },
                    },
                },
                {
                    "Event": "SparkListenerTaskEnd",
                    "Task End Reason": {"Reason": "Success"},
                    "Task Info": {
                        "Executor ID": "7",
                        "Launch Time": 2000,
                        "Finish Time": 10000,
                    },
                },
                {
                    "Event": "SparkListenerTaskEnd",
                    "Task End Reason": {"Reason": "ExceptionFailure"},
                    "Task Info": {
                        "Executor ID": "7",
                        "Launch Time": 2000,
                        "Finish Time": 12000,
                        "Failed": True,
                    },
                },
            ]
            second = [
                {
                    "Event": "SparkListenerApplicationEnd",
                    "Timestamp": 11000,
                }
            ]
            for number, events in ((1, first), (2, second)):
                (app_dir / f"events_{number}_{APP_ID}").write_text(
                    "\n".join(json.dumps(event) for event in events) + "\n"
                )

            metadata = MODULE.read_event_log_metadata(root)[APP_ID]
            self.assertTrue(metadata.event_task_metrics_complete())
            self.assertEqual(1, metadata.successful_task_attempt_count)
            self.assertEqual(8000, metadata.task_duration_sum_ms)
            self.assertEqual({"7": 8000}, metadata.task_duration_ms_by_executor)
            self.assertEqual(4, metadata.configured_executor_cores)
            self.assertEqual(1, metadata.task_cpus)
            self.assertEqual(
                "container_123_0002_01_000002",
                metadata.executors["7"].container_id,
            )

            (app_dir / f"events_2_{APP_ID}").rename(
                app_dir / f"events_3_{APP_ID}"
            )
            incomplete = MODULE.read_event_log_metadata(root)[APP_ID]
            self.assertFalse(incomplete.event_task_metrics_complete())
            self.assertIn(
                "Missing event-log segments: 2",
                incomplete.task_metric_warnings,
            )

    def test_task_packing_metrics_use_executor_cores_and_yarn_node_share(self):
        container = MODULE.Container(
            container_id="container_123_0002_01_000002",
            application_id=APP_ID,
            node_id="worker",
            start_ms=1000,
            finish_ms=11000,
            memory_mb=40,
            node_memory_mb=100,
            vcores=1,
            node_vcores=4,
            source="resourcemanager",
            finish_source="resourcemanager",
        )
        evidence = MODULE.YarnEvidence(
            nodes={"worker": MODULE.Node("worker", "cpu.test", 100, 4, 0)},
            containers={container.container_id: container},
            calculator_class="DefaultResourceCalculator",
            application_summaries={
                APP_ID: MODULE.ApplicationSummary(APP_ID, "test", "SUCCEEDED", 1)
            },
        )
        application = MODULE.calculate_applications(
            evidence, "default", {}, False
        )[0]
        event = MODULE.EventLogApplication(
            application_id=APP_ID,
            application_ended=True,
            task_cpus=1,
            successful_task_attempt_count=1,
            task_duration_sum_ms=8000,
            task_duration_ms_by_executor={"7": 8000},
            executors={
                "7": DISCOVERY_MODULE.SparkExecutor(
                    "7", container.container_id, 4, 0
                )
            },
            event_segments={1},
        )

        partial = dict(application)
        partial["complete"] = False
        partial["warnings"] = list(application["warnings"])
        MODULE.add_task_packing_metrics(
            [partial], {APP_ID: event}, evidence, "default"
        )
        self.assertTrue(partial["task_metrics_complete"])
        self.assertFalse(partial["perfect_packing_complete"])
        self.assertEqual(8.0, partial["task_duration_sum_seconds"])
        self.assertEqual("", partial["perfect_packing_node_seconds"])

        MODULE.add_task_packing_metrics(
            [application], {APP_ID: event}, evidence, "default"
        )
        self.assertTrue(application["task_metrics_complete"])
        self.assertTrue(application["perfect_packing_complete"])
        self.assertEqual(8.0, application["task_duration_sum_seconds"])
        self.assertEqual(0.8, application["perfect_packing_node_seconds"])
        self.assertEqual(0.2, application["packing_efficiency"])

        ec2 = {
            "usd_per_hour": 3.6,
            "effective_date": "2026-01-01",
            "description": "fixture",
        }
        emr = {
            "usd_per_hour": 0.9,
            "effective_date": "2026-01-01",
            "description": "fixture",
        }
        with mock.patch.object(
            MODULE, "current_ondemand_hourly_price", return_value=ec2
        ), mock.patch.object(
            MODULE, "current_emr_hourly_price", return_value=emr
        ):
            MODULE.add_ondemand_costs(
                [application], "us-west-2", "example-profile"
            )
        self.assertEqual(0.005, application["ec2_plus_emr_usd"])
        self.assertEqual(0.001, application["perfect_packing_ec2_plus_emr_usd"])
        self.assertEqual(5.0, application["actual_to_perfect_cost_factor"])
        self.assertEqual(400.0, application["actual_to_perfect_cost_overhead_percent"])

    def test_dominant_gpu_task_packing_charges_full_node_share(self):
        container = MODULE.Container(
            container_id="container_123_0002_01_000001",
            application_id=APP_ID,
            node_id="worker",
            start_ms=1000,
            finish_ms=11000,
            memory_mb=49152,
            node_memory_mb=53248,
            vcores=15,
            node_vcores=16,
            gpus=1,
            node_gpus=1,
            source="resourcemanager",
            finish_source="resourcemanager",
        )
        evidence = MODULE.YarnEvidence(
            nodes={"worker": MODULE.Node("worker", "g6.4xlarge", 53248, 16, 1)},
            containers={container.container_id: container},
            calculator_class="DominantResourceCalculator",
            application_summaries={
                APP_ID: MODULE.ApplicationSummary(APP_ID, "test", "SUCCEEDED", 1)
            },
        )
        application = MODULE.calculate_applications(
            evidence, "dominant", {}, False, {container.container_id}
        )[0]
        event = MODULE.EventLogApplication(
            application_id=APP_ID,
            application_ended=True,
            task_cpus=1,
            successful_task_attempt_count=15,
            task_duration_sum_ms=15000,
            task_duration_ms_by_executor={"7": 15000},
            executors={
                "7": DISCOVERY_MODULE.SparkExecutor(
                    "7", container.container_id, 15, 0
                )
            },
            event_segments={1},
        )

        MODULE.add_task_packing_metrics(
            [application], {APP_ID: event}, evidence, "dominant"
        )
        self.assertTrue(application["perfect_packing_complete"])
        self.assertEqual(1.0, application["perfect_packing_node_seconds"])
        self.assertEqual(10.0, application["node_equivalent_seconds"])
        self.assertIn(
            "Included sequence-1 container", " | ".join(application["warnings"])
        )
        self.assertEqual(0.1, application["packing_efficiency"])

    def test_comparison_summary_uses_only_complete_matched_jobs(self):
        complete = {
            "job_id": "1",
            "baseline_final_status": "SUCCEEDED",
            "test_final_status": "SUCCEEDED",
            "baseline_complete": True,
            "test_complete": True,
            "baseline_task_metrics_complete": True,
            "test_task_metrics_complete": True,
            "baseline_perfect_packing_complete": True,
            "test_perfect_packing_complete": True,
            "baseline_task_duration_sum_seconds": 100.0,
            "test_task_duration_sum_seconds": 50.0,
            "baseline_perfect_packing_node_seconds": 20.0,
            "test_perfect_packing_node_seconds": 10.0,
            "baseline_node_equivalent_seconds": 40.0,
            "test_node_equivalent_seconds": 25.0,
            "baseline_perfect_packing_ec2_plus_emr_usd": 4.0,
            "test_perfect_packing_ec2_plus_emr_usd": 3.0,
            "baseline_ec2_plus_emr_usd": 8.0,
            "test_ec2_plus_emr_usd": 6.0,
        }
        incomplete = dict(complete, job_id="2", test_task_metrics_complete=False)

        summary = MODULE.build_comparison_summary([complete, incomplete])

        self.assertEqual(1, summary["eligible_job_count"])
        self.assertEqual(1, summary["excluded_job_count"])
        self.assertEqual(0.5, summary["task_duration_factor"])
        self.assertEqual(0.75, summary["perfect_packing_cost_factor"])
        self.assertEqual(0.75, summary["actual_cost_factor"])
        self.assertEqual(0.5, summary["baseline"]["packing_efficiency"])
        self.assertEqual(0.4, summary["test"]["packing_efficiency"])
        self.assertEqual(2.0, summary["baseline"]["actual_to_perfect_cost_factor"])

    def test_instance_vcore_total_uses_all_successful_complete_jobs(self):
        applications = [
            {
                "job id": "1",
                "application_id": "application_success_1",
                "final_status": "SUCCEEDED",
                "complete": True,
                "task_metrics_complete": False,
                "perfect_packing_complete": False,
                "instance_vcore_seconds_by_instance_type": {
                    "r7a.24xlarge": 100.0
                },
            },
            {
                "job id": "2",
                "application_id": "application_success_2",
                "final_status": "SUCCEEDED",
                "complete": True,
                "task_metrics_complete": False,
                "perfect_packing_complete": False,
                "instance_vcore_seconds_by_instance_type": {
                    "r7a.24xlarge": 20.0,
                    "g6.4xlarge": 30.0,
                },
            },
            {
                "job id": "3",
                "application_id": "application_failed",
                "final_status": "FAILED",
                "complete": True,
                "task_metrics_complete": False,
                "perfect_packing_complete": False,
                "instance_vcore_seconds_by_instance_type": {
                    "r7a.24xlarge": 1000.0
                },
            },
            {
                "job id": "4",
                "application_id": "application_partial",
                "final_status": "SUCCEEDED",
                "complete": False,
                "task_metrics_complete": False,
                "perfect_packing_complete": False,
                "instance_vcore_seconds_by_instance_type": {
                    "r7a.24xlarge": 2000.0
                },
            },
        ]

        summary = MODULE.summarize_applications(applications)

        self.assertEqual(2, summary["successful_complete_job_count"])
        self.assertEqual(1, summary["successful_incomplete_job_count"])
        self.assertEqual(150.0, summary["total_instance_vcore_seconds"])
        self.assertEqual(
            {"g6.4xlarge": 30.0, "r7a.24xlarge": 120.0},
            summary["total_instance_vcore_seconds_by_instance_type"],
        )
        self.assertEqual(
            "30.000000 g6.4xlarge-vcore-seconds + "
            "120.000000 r7a.24xlarge-vcore-seconds",
            summary["total_instance_vcore_seconds_expression"],
        )

    def test_new_comparison_sort_modes(self):
        rows = [
            {
                "job_id": "1",
                "baseline_final_status": "SUCCEEDED",
                "test_final_status": "SUCCEEDED",
                "task_duration_factor": 2.0,
                "perfect_packing_cost_factor": 0.5,
            },
            {
                "job_id": "2",
                "baseline_final_status": "SUCCEEDED",
                "test_final_status": "SUCCEEDED",
                "task_duration_factor": 0.5,
                "perfect_packing_cost_factor": 2.0,
            },
        ]
        self.assertEqual(
            ["2", "1"],
            [
                row["job_id"]
                for row in MODULE.sort_comparison_rows(
                    rows, "task-duration-factor"
                )
            ],
        )
        self.assertEqual(
            ["1", "2"],
            [
                row["job_id"]
                for row in MODULE.sort_comparison_rows(
                    rows, "perfect-packing-cost-factor"
                )
            ],
        )

    def test_event_materializer_downloads_every_segment(self):
        objects = {
            APP_ID: [
                f"s3://bucket/root/eventlog_v2_{APP_ID}/events_{number}_{APP_ID}"
                for number in (1, 2, 3)
            ]
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch(
            "yarn_job_cost_discovery.list_event_log_objects",
            return_value=objects,
        ), mock.patch(
            "yarn_job_cost_discovery.run_aws",
            return_value=subprocess.CompletedProcess([], 0, "", ""),
        ) as run_aws:
            _, application_ids = MODULE.materialize_event_metadata_files(
                "s3://bucket/root", Path(directory), None, "us-west-2"
            )
        self.assertEqual({APP_ID}, application_ids)
        self.assertEqual(3, run_aws.call_count)
        self.assertEqual(
            [
                objects[APP_ID][0],
                objects[APP_ID][1],
                objects[APP_ID][2],
            ],
            [call.args[0][-3] for call in run_aws.call_args_list],
        )

    def test_cli_defaults_come_from_defaults_module(self):
        with mock.patch.object(
            sys, "argv", ["calculate_yarn_job_cost.py", "--emr-log-uri", "/tmp/logs"]
        ):
            args = MODULE.parse_args()
        self.assertEqual(MODULE.DEFAULT_AWS_PROFILE, args.aws_profile)
        self.assertEqual(MODULE.DEFAULT_AWS_REGION, args.aws_region)

    def test_resolve_aws_region_precedence(self):
        with mock.patch.object(MODULE.subprocess, "run") as run:
            self.assertEqual(
                "eu-west-1", MODULE.resolve_aws_region("eu-west-1", None)
            )
            run.assert_not_called()

        with mock.patch.dict(os.environ, {"AWS_REGION": "ap-southeast-2"}, clear=False):
            self.assertEqual(
                "ap-southeast-2", MODULE.resolve_aws_region(None, None)
            )

        environment = dict(os.environ)
        environment.pop("AWS_REGION", None)
        environment.pop("AWS_DEFAULT_REGION", None)
        configured = subprocess.CompletedProcess(
            ["aws", "configure", "get", "region"], 0, stdout="us-east-2\n", stderr=""
        )
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(
            MODULE.subprocess, "run", return_value=configured
        ) as run:
            self.assertEqual(
                "us-east-2", MODULE.resolve_aws_region(None, "example-profile")
            )
        self.assertEqual(
            ["aws", "--profile", "example-profile", "configure", "get", "region"],
            run.call_args.args[0],
        )

    def test_resolve_aws_region_rejects_missing_region(self):
        environment = dict(os.environ)
        environment.pop("AWS_REGION", None)
        environment.pop("AWS_DEFAULT_REGION", None)
        missing = subprocess.CompletedProcess(
            ["aws", "configure", "get", "region"], 1, stdout="", stderr=""
        )
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(
            MODULE.subprocess, "run", return_value=missing
        ):
            with self.assertRaisesRegex(ValueError, "pass --aws-region"):
                MODULE.resolve_aws_region(None, None)

    def test_portable_eventlog_reader_handles_raw_lz4block(self):
        payload = b"first\nsecond\n"
        header = (
            EVENTLOG.LZ4_BLOCK_MAGIC
            + bytes([EVENTLOG.RAW_BLOCK])
            + len(payload).to_bytes(4, "little")
            + len(payload).to_bytes(4, "little")
            + bytes(4)
        )
        self.assertEqual(
            ["first", "second"],
            list(EVENTLOG.iter_text_lines(io.BytesIO(header + payload), True)),
        )

    def test_portable_eventlog_reader_handles_literal_lz4_block(self):
        payload = b"spark"
        compressed = bytes([len(payload) << 4]) + payload
        self.assertEqual(
            payload, EVENTLOG.lz4_decompress_block(compressed, len(payload))
        )

    def test_portable_eventlog_reader_handles_tar_bundle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events_1_application_123_0002"
            source.write_text("one\ntwo\n")
            archive = root / "events.tar.gz"
            with tarfile.open(archive, "w:gz") as bundle:
                bundle.add(
                    source,
                    arcname=(
                        "eventlog_v2_application_123_0002/"
                        "events_1_application_123_0002"
                    ),
                )
            streams = []
            for app_dir, _, stream, compressed in EVENTLOG.iter_eventlog_streams(
                archive
            ):
                streams.append(
                    (app_dir, list(EVENTLOG.iter_text_lines(stream, compressed)))
                )
            self.assertEqual(
                [("eventlog_v2_application_123_0002", ["one", "two"])], streams
            )

if __name__ == "__main__":
    unittest.main()
