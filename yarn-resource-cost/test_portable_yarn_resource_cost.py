#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yarn_job_cost_adapters as adapters
import yarn_job_cost_core as core


ROOT = Path(__file__).resolve().parent
FIXTURE = ROOT / "tests" / "fixtures" / "on_prem"
SCRIPT = ROOT / "yarn_resource_cost.py"


class ProviderNeutralAccountingTest(unittest.TestCase):
    def container(self, **updates) -> core.Container:
        values = {
            "container_id": "container_1_0001_01_000002",
            "application_id": "application_1_0001",
            "node_id": "worker1",
            "start_ms": 0,
            "finish_ms": 1000,
            "memory_mb": 2048,
            "node_memory_mb": 8192,
            "vcores": 2,
            "node_vcores": 8,
            "resources": {"memory-mb": 2048, "vcores": 2},
            "node_resources": {"memory-mb": 8192, "vcores": 8},
        }
        values.update(updates)
        return core.Container(**values)

    def test_default_calculator_uses_memory_only(self):
        container = self.container(
            vcores=8,
            resources={"memory-mb": 2048, "vcores": 8, "yarn.io/gpu": 1},
            node_resources={"memory-mb": 8192, "vcores": 8, "yarn.io/gpu": 1},
        )
        self.assertEqual(0.25, core.container_node_share(container, "default"))

    def test_dominant_calculator_uses_arbitrary_custom_resource(self):
        container = self.container(
            resources={"memory-mb": 2048, "vcores": 2, "vendor/device": 3},
            node_resources={"memory-mb": 8192, "vcores": 8, "vendor/device": 4},
        )
        self.assertEqual(0.75, core.container_node_share(container, "dominant"))

    def test_dominant_rejects_missing_allocated_resource_capacity(self):
        container = self.container(
            resources={"memory-mb": 2048, "vcores": 2, "yarn.io/gpu": 1}
        )
        with self.assertRaisesRegex(ValueError, "yarn.io/gpu"):
            core.container_node_share(container, "dominant")

    def test_fair_scheduler_drf_policy_is_detected_from_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hadoop-yarn-resourcemanager.log"
            path.write_text(
                "2026-01-01 00:00:00,000 INFO FairScheduler: "
                "policy=DominantResourceFairnessPolicy\n",
                encoding="utf-8",
            )
            evidence = core.parse_yarn_logs(path)
        self.assertEqual("DominantResourceCalculator", evidence.calculator_class)
        self.assertEqual("DominantResourceFairnessPolicy", evidence.scheduler_policy)


class AdapterTest(unittest.TestCase):
    def test_catalog_cost_and_node_mapping(self):
        evidence = core.parse_yarn_logs(FIXTURE / "yarn")
        adapters.apply_node_class_map(evidence, FIXTURE / "node-classes.json")
        self.assertEqual("onprem:worker-8", evidence.nodes["worker1"].node_class)
        catalog = adapters.load_price_catalog(FIXTURE / "prices.json")
        applications = [
            {
                "complete": True,
                "node_equivalent_seconds_by_instance_type": {
                    "onprem:worker-8": 10.0
                },
                "warnings": [],
            }
        ]
        adapters.apply_catalog_costs(applications, catalog)
        self.assertEqual(0.01, applications[0]["worker_cost"])

    def test_missing_catalog_rate_suppresses_final_cost(self):
        catalog = adapters.load_price_catalog(FIXTURE / "prices.json")
        applications = [
            {
                "complete": True,
                "node_equivalent_seconds_by_instance_type": {"unknown": 1.0},
                "warnings": [],
            }
        ]
        adapters.apply_catalog_costs(applications, catalog)
        self.assertFalse(applications[0]["complete"])
        self.assertEqual("", applications[0]["worker_cost"])


class PortableCliTest(unittest.TestCase):
    def test_on_prem_fixture_end_to_end_with_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--adapter",
                    "on-prem",
                    "--event-log-root",
                    str(FIXTURE),
                    "--yarn-log-root",
                    str(FIXTURE / "yarn"),
                    "--node-class-map",
                    str(FIXTURE / "node-classes.json"),
                    "--pricing",
                    "catalog",
                    "--price-catalog",
                    str(FIXTURE / "prices.json"),
                    "--output-json",
                    str(output),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(1, payload["schema_version"])
        self.assertEqual("on-prem", payload["adapter"])
        application = payload["applications"][0]
        self.assertTrue(application["complete"])
        self.assertEqual(2.0, application["node_equivalent_seconds"])
        self.assertEqual(0.002, application["worker_cost"])
        self.assertEqual("USD", application["worker_cost_currency"])


if __name__ == "__main__":
    unittest.main()
