#
# SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----
#
# Certain portions of the contents of this file are derived from TPC-H version 3.2.0
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
# and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-H Benchmark and as such any results
# obtained using this file are not comparable to published TPC-H Benchmark results, as the results
# obtained from using this file do not comply with the TPC-H Benchmark.
#

import json
import os
import time
import traceback
from typing import Callable
from pyspark.sql import SparkSession
from python_benchmark_reporter.PythonListener import PythonListener


class PysparkBenchReport:
    """
    A utility class to run a Spark benchmark test and generate a performance report.
    """

    def __init__(
        self,
        app_name: str,
        query_func: Callable[[SparkSession], None],
        output_path: str,
        iterations: int = 1,
        cleanup_func: Callable[[SparkSession], None] = None,
    ):
        """
        Initializes the benchmark reporter.

        :param app_name: Name of the Spark application.
        :param query_func: Function that takes a SparkSession and runs the query.
        :param output_path: Path to save the JSON benchmark report.
        :param iterations: Number of times to run the query (default: 1).
        :param cleanup_func: Optional function to clean up state between iterations.
        """
        self.app_name = app_name
        self.query_func = query_func
        self.output_path = output_path
        self.iterations = iterations
        self.cleanup_func = cleanup_func
        self.spark = None
        self.listener = None

    def setup_spark(self):
        """Initializes the Spark session with necessary configurations and attaches the listener."""
        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.join.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        self.listener = PythonListener()
        self.spark.sparkContext.addSparkListener(self.listener)

    def run_query_and_collect_metrics(self):
        """Runs the query function and collects execution metrics from the listener."""
        start_time = time.time()
        try:
            self.query_func(self.spark)
            query_status = "Completed"
        except Exception as e:
            query_status = "Failed"
            print(f"Query failed with exception: {e}")
            traceback.print_exc()
        end_time = time.time()

        # Collect metrics from the listener
        duration_ms = int((end_time - start_time) * 1000)
        task_failures = self.listener.get_task_failures()
        execution_plan = self.listener.get_final_plan()
        query_status = "CompletedWithTaskFailures" if task_failures > 0 else query_status

        return {
            "queryStatus": query_status,
            "durationMs": duration_ms,
            "taskFailures": task_failures,
            "finalExecutionPlan": execution_plan,
        }

    def run(self):
        """Runs the benchmark for the specified number of iterations and saves the report."""
        self.setup_spark()
        results = []

        for i in range(self.iterations):
            print(f"Running iteration {i + 1}/{self.iterations}")
            if self.cleanup_func:
                self.cleanup_func(self.spark)
            self.listener.reset()
            result = self.run_query_and_collect_metrics()
            result["iteration"] = i + 1
            result["appId"] = self.spark.sparkContext.applicationId
            results.append(result)

        # Save results to output path
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        with open(self.output_path, "w") as f:
            json.dump(results, f, indent=2)

        print(f"Benchmark report saved to {self.output_path}")
        self.spark.stop()