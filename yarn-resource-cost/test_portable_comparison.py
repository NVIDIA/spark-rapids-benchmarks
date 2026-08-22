#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import unittest

import yarn_resource_cost as cli


def application(app_id: str, complete: bool, node_seconds: float, cost: object) -> dict:
    return {
        "comparison_key": "job-1",
        "application_id": app_id,
        "node_equivalent_seconds": node_seconds,
        "node_equivalent_seconds_by_instance_type": {"node-class": node_seconds},
        "spark_duration_seconds": 10.0,
        "worker_cost": cost,
        "complete": complete,
    }


class PortableComparisonTest(unittest.TestCase):
    def test_incomplete_ledger_has_no_resource_or_cost_factor(self):
        args = argparse.Namespace(sort_by="comparison-key")
        baseline = {"applications": [application("application_1_1", True, 10.0, 1.0)]}
        test = {"applications": [application("application_2_1", False, 5.0, "")]}
        row = cli.compare_runs(baseline, [test], args)[0]
        self.assertEqual("", row["node_equivalent_factor"])
        self.assertEqual("", row["worker_cost_factor"])
        self.assertEqual(1.0, row["wall_clock_factor"])


if __name__ == "__main__":
    unittest.main()
