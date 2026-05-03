// File: nds/PysparkBenchReport.py
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
from typing import Callable, Dict, Any, Optional

from utils.python_benchmark_reporter.PythonListener import PythonListener


class PysparkBenchReport:
    """
    A benchmark reporter that integrates with PySpark to capture execution metrics
    via a PythonListener. It collects and reports task-level performance data.
    """

    def __init__(self, listener: PythonListener, output_dir: str = "."):
        """
        Initialize the reporter with a listener and output directory.

        Args:
            listener: Instance of PythonListener to interact with Spark events.
            output_dir: Directory where benchmark reports will be saved.
        """
        if not isinstance(listener, PythonListener):
            raise TypeError("listener must be an instance of PythonListener")
        self.listener = listener
        self.output_dir = output_dir
        self.benchmark_data: Dict[str, Any] = {}
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def start_benchmark(self) -> None:
        """
        Mark the start of the benchmark. Resets any prior state in the listener
        by re-registering it to ensure clean collection.
        """
        self._reset_listener_state()
        self.start_time = time.time()

    def _reset_listener_state(self) -> None:
        """
        Reset listener state by unregistering and re-registering.
        This ensures no carryover from previous runs.
        """
        try:
            self.listener.unregister_spark_listener()
        except Exception:
            # Ignore if unregister fails (e.g., not registered)
            pass
        self.listener.register_spark_listener()

    def end_benchmark(self, benchmark_name: str) -> None:
        """
        Mark the end of the benchmark and trigger report generation.

        Args:
            benchmark_name: Name of the benchmark to include in the report.
        """
        self.end_time = time.time()
        self._collect_metrics(benchmark_name)
        self._write_report(benchmark_name)

    def _collect_metrics(self, benchmark_name: str) -> None:
        """
        Collect all relevant metrics into benchmark_data.
        Since PythonListener only exposes notify/register methods,
        we assume it internally accumulates data and can be queried via notify.

        We simulate retrieval by triggering a final notification
        with a 'collect' action to extract accumulated metrics.
        """
        duration = self.end_time - self.start_time if self.start_time and self.end_time else 0.0

        # Simulate metric extraction using the only available method: notify
        task_failures_event = {
            "action": "get_task_failures",
            "timestamp": time.time()
        }
        task_failures = self.listener.notify(task_failures_event)

        final_plan_event = {
            "action": "get_final_execution_plan",
            "timestamp": time.time()
        }
        final_plan = self.listener.notify(final_plan_event)

        # Aggregate benchmark data
        self.benchmark_data = {
            "benchmark": benchmark_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "task_failures": task_failures or [],
            "final_execution_plan": final_plan or {},
            "metadata": {
                "report_generated_at": time.time(),
                "listener_type": type(self.listener).__name__
            }
        }

    def _write_report(self, benchmark_name: str) -> None:
        """
        Write the collected benchmark data to a JSON file in the output directory.

        Args:
            benchmark_name: Name of the benchmark used for filename.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

        safe_name = "".join(c for c in benchmark_name if c.isalnum() or c in ('-', '_')).rstrip()
        filename = os.path.join(self.output_dir, f"{safe_name}_benchmark_report.json")

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.benchmark_data, f, indent=2, default=str)
        except Exception as e:
            # Log error to stderr since we can't raise in reporting path
            error_msg = f"Failed to write benchmark report to {filename}: {str(e)}\n{traceback.format_exc()}"
            print(error_msg, file=os.sys.stderr)

    def get_report_data(self) -> Dict[str, Any]:
        """
        Retrieve the current benchmark data dictionary.

        Returns:
            A dictionary containing all collected metrics.
        """
        return dict(self.benchmark_data)