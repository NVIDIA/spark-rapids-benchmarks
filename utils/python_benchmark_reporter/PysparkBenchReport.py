// File: utils/python_benchmark_reporter/PysparkBenchReport.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
# obtained from using this file do not conform to the TPC-H Benchmark requirements or specifications.

import json
import time
from typing import Dict, Any, Optional
from utils.python_benchmark_reporter.PythonListener import PythonListener


class PysparkBenchReport:
    """
    A reporter class that collects and formats benchmarking results from PySpark workloads
    using a PythonListener to capture execution events.
    """

    def __init__(self, listener: PythonListener, benchmark_name: str):
        """
        Initialize the reporter with a listener and benchmark name.

        Args:
            listener: An instance of PythonListener to register with Spark.
            benchmark_name: Name of the benchmark being executed.
        """
        if not isinstance(listener, PythonListener):
            raise TypeError("listener must be an instance of PythonListener")
        if not isinstance(benchmark_name, str) or not benchmark_name.strip():
            raise ValueError("benchmark_name must be a non-empty string")

        self.listener = listener
        self.benchmark_name = benchmark_name.strip()
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.metrics: Dict[str, Any] = {}

    def start_benchmark(self) -> None:
        """
        Mark the start of the benchmark and reset internal state.
        """
        self.start_time = time.time()
        self.metrics.clear()

    def end_benchmark(self) -> None:
        """
        Mark the end of the benchmark and collect final metrics.
        """
        self.end_time = time.time()
        if self.start_time is not None:
            self.metrics['duration_seconds'] = self.end_time - self.start_time
        else:
            self.metrics['duration_seconds'] = 0.0

    def collect_metrics(self) -> Dict[str, Any]:
        """
        Collect all available metrics into a serializable dictionary.

        Returns:
            Dictionary containing benchmark metadata and collected metrics.
        """
        report = {
            "benchmark": self.benchmark_name,
            "timestamp": int(time.time()),
            "metrics": dict(self.metrics),
            "success": True
        }

        # Since PythonListener does not expose get_task_failures, get_final_plan, or reset,
        # we rely only on notify-based event collection and do not attempt to call undefined methods.
        # Any additional data must be extracted via side effects captured during notify() calls.

        return report

    def generate_report(self) -> str:
        """
        Generate a JSON-formatted benchmark report.

        Returns:
            JSON string representing the full benchmark report.
        """
        report_data = self.collect_metrics()
        try:
            return json.dumps(report_data, indent=2)
        except (TypeError, ValueError) as e:
            raise RuntimeError(f"Failed to serialize report to JSON: {e}") from e

    def reset(self) -> None:
        """
        Reset the reporter state for reuse in subsequent runs.
        """
        self.start_time = None
        self.end_time = None
        self.metrics.clear()