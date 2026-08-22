#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

from yarn_job_cost_core import normalize_log_line


class DataprocLogNormalizationTest(unittest.TestCase):
    def test_cloud_logging_text_payload_is_unwrapped(self):
        message = (
            "2026-01-01 00:00:00,000 INFO CapacityScheduler: "
            "resource-calculator=DefaultResourceCalculator"
        )
        line = json.dumps({"textPayload": message})
        self.assertEqual(message, normalize_log_line(line).rstrip())

    def test_plain_daemon_log_line_is_unchanged(self):
        line = "2026-01-01 00:00:00,000 INFO ResourceManager: started\n"
        self.assertEqual(line, normalize_log_line(line))


if __name__ == "__main__":
    unittest.main()
