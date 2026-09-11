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
    def request(self, event_log_uri=None):
        return EmrApplicationUsageRequest(
            cluster_id="j-TEST",
            application_id="application_1_0001",
            event_log_uri=event_log_uri
            or str(FIXTURE / "eventlog_v2_application_1_0001"),
            region="us-west-2",
        )

    @staticmethod
    def yarn_log_with_instance_type():
        log = (FIXTURE / "yarn" / "hadoop-yarn-resourcemanager-rm.log").read_text(
            encoding="utf-8"
        )
        return log.replace(
            "registered with capability: <memory:8192, vCores:8>",
            "registered with capability: <memory:8192, vCores:8> "
            "instanceType(STRING)=m5.xlarge",
        )

    def test_calculates_one_application_from_boto_clients(self):
        s3 = FakeS3Client(
            {
                "emr-logs/j-TEST/node/i-1/applications/"
                "hadoop-yarn-resourcemanager-rm.log": self.yarn_log_with_instance_type().encode()
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

    def test_missing_calculator_evidence_is_retryable(self):
        log = "\n".join(
            line
            for line in self.yarn_log_with_instance_type().splitlines()
            if "Initialized CapacityScheduler" not in line
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

        self.assertFalse(result.complete)
        self.assertTrue(result.retryable)
        self.assertIn("Could not detect", result.warnings[0])

    def test_conflicting_calculator_evidence_is_structured_and_not_retryable(self):
        log = self.yarn_log_with_instance_type()
        log += (
            "\n2026-01-01 00:00:00,050 INFO CapacityScheduler: "
            "resource-calculator=DominantResourceCalculator\n"
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

        self.assertFalse(result.complete)
        self.assertFalse(result.retryable)
        self.assertIn("Conflicting ResourceCalculators", result.warnings[0])

    def test_missing_node_instance_type_is_retryable(self):
        log = (FIXTURE / "yarn" / "hadoop-yarn-resourcemanager-rm.log").read_bytes()
        s3 = FakeS3Client(
            {
                "emr-logs/j-TEST/node/i-1/applications/"
                "hadoop-yarn-resourcemanager-rm.log": log
            }
        )

        result = calculate_emr_application_usage(
            self.request(), emr_client=FakeEmrClient(), s3_client=s3
        )

        self.assertFalse(result.complete)
        self.assertTrue(result.retryable)
        self.assertIn("unknown instance type", " | ".join(result.warnings))

    def test_s3_rolling_event_log_segments_stay_grouped(self):
        event_prefix = "spark-events/eventlog_v2_application_1_0001"
        segment_1 = "\n".join(
            (
                '{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}',
                '{"Event":"SparkListenerEnvironmentUpdate","Spark Properties":'
                '{"spark.executor.cores":"2","spark.task.cpus":"1"}}',
                '{"Event":"SparkListenerApplicationStart","App Name":'
                '"portable-cost-sample","App ID":"application_1_0001",'
                '"Timestamp":1000}',
            )
        )
        segment_2 = "\n".join(
            (
                '{"Event":"SparkListenerExecutorAdded","Executor ID":"driver",'
                '"Executor Info":{"Total Cores":1,"Resource Profile Id":0,'
                '"Attributes":{"CONTAINER_ID":"container_1_0001_01_000001"}}}',
                '{"Event":"SparkListenerApplicationEnd","Timestamp":11000}',
            )
        )
        s3 = FakeS3Client(
            {
                f"{event_prefix}/events_1_application_1_0001": segment_1.encode(),
                f"{event_prefix}/events_2_application_1_0001": segment_2.encode(),
                "emr-logs/j-TEST/node/i-1/applications/"
                "hadoop-yarn-resourcemanager-rm.log": self.yarn_log_with_instance_type().encode(),
            }
        )

        result = calculate_emr_application_usage(
            self.request(f"s3://test-bucket/{event_prefix}"),
            emr_client=FakeEmrClient(),
            s3_client=s3,
        )

        self.assertTrue(result.complete)
        self.assertEqual(2, result.container_count)
        self.assertEqual(24.0, result.vcore_seconds)
        self.assertEqual(24576.0, result.memory_mb_seconds)
        self.assertEqual({"m5.xlarge": 3.0}, result.instance_seconds_by_type)

    def test_s3_single_file_event_log_is_materialized_as_a_file(self):
        event_key = "spark-events/application_1_0001"
        event_log = (
            FIXTURE
            / "eventlog_v2_application_1_0001"
            / "events_1_application_1_0001"
        ).read_bytes()
        s3 = FakeS3Client(
            {
                event_key: event_log,
                "emr-logs/j-TEST/node/i-1/applications/"
                "hadoop-yarn-resourcemanager-rm.log": self.yarn_log_with_instance_type().encode(),
            }
        )

        result = calculate_emr_application_usage(
            self.request(f"s3://test-bucket/{event_key}"),
            emr_client=FakeEmrClient(),
            s3_client=s3,
        )

        self.assertTrue(result.complete)
        self.assertFalse(result.retryable)
        self.assertEqual(1, result.container_count)
        self.assertEqual({"m5.xlarge": 2.0}, result.instance_seconds_by_type)

    def test_file_uri_event_log_file_and_directory(self):
        s3 = FakeS3Client(
            {
                "emr-logs/j-TEST/node/i-1/applications/"
                "hadoop-yarn-resourcemanager-rm.log": self.yarn_log_with_instance_type().encode()
            }
        )
        event_log_dir = FIXTURE / "eventlog_v2_application_1_0001"
        event_log_file = event_log_dir / "events_1_application_1_0001"

        for event_log in (event_log_dir, event_log_file):
            with self.subTest(event_log=event_log):
                result = calculate_emr_application_usage(
                    self.request(event_log.resolve().as_uri()),
                    emr_client=FakeEmrClient(),
                    s3_client=s3,
                )

                self.assertTrue(result.complete)
                self.assertFalse(result.retryable)
                self.assertEqual({"m5.xlarge": 2.0}, result.instance_seconds_by_type)

    def test_remote_file_uri_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "remote authority"):
            calculate_emr_application_usage(
                self.request("file://remote-host/tmp/events"),
                emr_client=FakeEmrClient(),
                s3_client=FakeS3Client({}),
            )

    def test_later_node_registration_completes_missing_capacity(self):
        full_log = self.yarn_log_with_instance_type()
        registration = next(
            line for line in full_log.splitlines() if "registered with capability" in line
        )
        application_log = "\n".join(
            line for line in full_log.splitlines() if line != registration
        )
        yarn_key = (
            "emr-logs/j-TEST/node/i-1/applications/"
            "hadoop-yarn-resourcemanager-a-application.log"
        )
        registration_key = (
            "emr-logs/j-TEST/node/i-1/applications/"
            "hadoop-yarn-resourcemanager-z-registration.log"
        )

        for name, initial_log in (
            ("embedded max capacity", application_log),
            (
                "missing max capacity",
                application_log.replace(", max memory:8192", "").replace(
                    ", max vCores:8", ""
                ),
            ),
        ):
            with self.subTest(name=name):
                first = calculate_emr_application_usage(
                    self.request(),
                    emr_client=FakeEmrClient(),
                    s3_client=FakeS3Client({yarn_key: initial_log.encode()}),
                )
                second = calculate_emr_application_usage(
                    self.request(),
                    emr_client=FakeEmrClient(),
                    s3_client=FakeS3Client(
                        {
                            yarn_key: initial_log.encode(),
                            registration_key: registration.encode(),
                        }
                    ),
                )

                self.assertFalse(first.complete)
                self.assertTrue(first.retryable)
                self.assertTrue(second.complete)
                self.assertFalse(second.retryable)
                self.assertEqual({"m5.xlarge": 2.0}, second.instance_seconds_by_type)

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
