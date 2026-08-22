#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import tempfile
import unittest
from pathlib import Path

import yarn_job_cost_core as core


class FairSchedulerPolicyTest(unittest.TestCase):
    def test_conflicting_built_in_policies_are_ambiguous(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hadoop-yarn-resourcemanager.log"
            path.write_text(
                "2026-01-01 00:00:00,000 INFO FairScheduler: "
                "queue=root.a policy=FairSharePolicy\n"
                "2026-01-01 00:00:01,000 INFO FairScheduler: "
                "queue=root.b policy=DominantResourceFairnessPolicy\n",
                encoding="utf-8",
            )
            evidence = core.parse_yarn_logs(path)
        self.assertTrue(evidence.accounting_policy_ambiguous)
        self.assertIn("Multiple FairScheduler policies", " ".join(evidence.warnings))


if __name__ == "__main__":
    unittest.main()
