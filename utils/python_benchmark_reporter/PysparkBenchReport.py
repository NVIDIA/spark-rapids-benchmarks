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
# obtained from using this file do not conform to the TPC-H Benchmark requirements.

import json
import logging
from typing import Any, Dict, Optional
from utils.python_benchmark_reporter.PythonListener import PythonListener


class PysparkBenchReport:
    """
    A reporter class that collects and formats benchmarking metrics from PySpark workloads
    using a PythonListener instance to observe execution events.
    """

    def __init__(self, listener: PythonListener) -> None:
        """
        Initialize the reporter with a PythonListener instance.

        Args:
            listener (PythonListener): The listener used to capture Spark events.
        """
        if not isinstance(listener, PythonListener):
            raise TypeError("listener must be an instance of PythonListener")
        self.listener = listener
        self._cached_plan: Optional[str] = None
        self._task_failures: int = 0

    def notify_listener(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Notify the underlying listener of an event.

        Args:
            event_type (str): Type of event (e.g., 'start', 'end', 'failure').
            data (Dict[str, Any]): Event payload.
        """
        if not isinstance(event_type, str):
            raise TypeError("event_type must be a string")
        if not isinstance(data, dict):
            raise TypeError("data must be a dictionary")
        self.listener.notify(event_type, data)

    def register(self, key: str, value: Any) -> None:
        """
        Register a key-value pair with the listener.

        Args:
            key (str): Identifier for the value.
            value (Any): Value to register.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        self.listener.register(key, value)

    def unregister(self, key: str) -> None:
        """
        Unregister a key from the listener.

        Args:
            key (str): Identifier to unregister.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        self.listener.unregister(key)

    def register_spark_listener(self, spark_session: Any) -> None:
        """
        Register the PythonListener as a Spark listener.

        Args:
            spark_session (Any): Active Spark session.
        """
        self.listener.register_spark_listener(spark_session)

    def unregister_spark_listener(self, spark_session: Any) -> None:
        """
        Unregister the PythonListener from the Spark session.

        Args:
            spark_session (Any): Active Spark session.
        """
        self.listener.unregister_spark_listener(spark_session)

    def reset_task_failures(self) -> None:
        """
        Reset the internal task failure counter.
        """
        self._task_failures = 0

    def increment_task_failures(self) -> None:
        """
        Increment the task failure count.
        """
        self._task_failures += 1

    def get_task_failures(self) -> int:
        """
        Get the number of recorded task failures.

        Returns:
            int: Number of task failures.
        """
        return self._task_failures

    def set_final_plan(self, plan: str) -> None:
        """
        Set the final physical plan string.

        Args:
            plan (str): The final execution plan.
        """
        if not isinstance(plan, str):
            raise TypeError("plan must be a string")
        self._cached_plan = plan

    def get_final_plan(self) -> Optional[str]:
        """
        Get the final physical plan captured during execution.

        Returns:
            Optional[str]: The final plan, or None if not set.
        """
        return self._cached_plan

    def reset(self) -> None:
        """
        Reset internal state of the reporter.
        """
        self._cached_plan = None
        self._task_failures = 0

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate a structured benchmark report.

        Returns:
            Dict[str, Any]: Report containing metrics and execution details.
        """
        return {
            "final_execution_plan": self.get_final_plan(),
            "task_failures": self.get_task_failures(),
            "listener_registered": True,  # Could be enhanced with actual status check
        }

    def export_report(self, filepath: str) -> None:
        """
        Export the benchmark report to a JSON file.

        Args:
            filepath (str): Path to save the report.
        """
        if not isinstance(filepath, str):
            raise TypeError("filepath must be a string")

        try:
            report = self.generate_report()
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error("Failed to export report to %s: %s", filepath, str(e))
            raise