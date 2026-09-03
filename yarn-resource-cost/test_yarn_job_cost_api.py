#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the application-scoped YARN resource accounting API."""

from __future__ import annotations

import io
import unittest
from pathlib import Path

from yarn_job_cost_api import (
    EmrApplicationUsageRequest,
    calculate_emr_application_usage,
)


FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "on_prem"


class FakePaginator:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def paginate(self, **kwargs):
        prefix = kwargs["Prefix"]
        yield {
            "Contents": [
                {"Key": key, "Size": len(value)}
                for key, value in self.objects.items()
                if key.startswith(prefix)
            ]
        }


class FakeS3Client:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def get_paginator(self, operation: str):
        if operation != "list_objects_v2":
            raise AssertionError(operation)
        return FakePaginator(self.objects)

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[kwargs["Key"]])}


class FakeEmrClient:
    def describe_cluster(self, **kwargs):
        return {
            "Cluster": {
                "Id": kwargs["ClusterId"],
                "LogUri": "s3://test-bucket/emr-logs/",
            }
        }


class YarnJobCostApiTest(unittest.TestCase):
    def request(self):
        return EmrApplicationUsageRequest(
            cluster_id="j-TEST",
            application_id="application_1_0001",
            event_log_uri=str(FIXTURE / "eventlog_v2_application_1_0001"),
            region="us-west-2",
        )

    def test_calculates_one_application_from_boto_clients(self):
        log = (FIXTURE / "yarn" / "hadoop-yarn-resourcemanager-rm.log").read_text(
            encoding="utf-8"
        )
        log = log.replace(
            "registered with capability: <memory:8192, vCores:8>",
            "registered with capability: <memory:8192, vCores:8> "
            "instanceType(STRING)=m5.xlarge",
        )
        s3 = FakeS3Client(
            {
                "emr-logs/j-TEST/node/i-1/applications/"
                "hadoop-yarn-resourcemanager-rm.log": log.encode()
            }
        )

        result = calculate_emr_application_usage(
            self.request(), emr_client=FakeEmrClient(), s3_client=s3
        )

        self.assertTrue(result.complete)
        self.assertFalse(result.retryable)
        self.assertEqual("default", result.resource_calculator)
        self.assertEqual(16.0, result.vcore_seconds)
        self.assertEqual(16384.0, result.memory_mb_seconds)
        self.assertEqual({"m5.xlarge": 2.0}, result.instance_seconds_by_type)
        self.assertEqual(1, result.container_count)
        self.assertEqual(2, result.expected_container_count)

    def test_missing_archived_logs_is_retryable(self):
        result = calculate_emr_application_usage(
            self.request(),
            emr_client=FakeEmrClient(),
            s3_client=FakeS3Client({}),
        )

        self.assertFalse(result.complete)
        self.assertTrue(result.retryable)
        self.assertIsNone(result.vcore_seconds)
        self.assertIn("No archived", result.warnings[0])

    def test_request_rejects_missing_identity(self):
        with self.assertRaisesRegex(ValueError, "application_id is required"):
            EmrApplicationUsageRequest(
                cluster_id="j-TEST",
                application_id="",
                event_log_uri="s3://bucket/events",
                region="us-west-2",
            )


if __name__ == "__main__":
    unittest.main()
