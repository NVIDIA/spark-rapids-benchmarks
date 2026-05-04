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
# obtained from using this file do not comply with the TPC-H Benchmark licensing requirements.

import json
import logging
from typing import Any, Dict, List, Optional

from utils.python_benchmark_reporter.PythonListener import PythonListener


class PysparkBenchReport:
    """
    A reporter class that collects and formats benchmarking data from PySpark
    using a PythonListener to capture execution events.
    """

    def __init__(self, listener: Optional[PythonListener] = None) -> None:
        """
        Initialize the reporter with an optional PythonListener.
        If none is provided, a new one is created.
        """
        self.listener: PythonListener = listener if listener is not None else PythonListener()
        self.report_data: Dict[str, Any] = {}

    def start_benchmark(self) -> None:
        """
        Reset internal state and prepare the listener for a new benchmark run.
        This ensures clean collection of metrics per benchmark iteration.
        """
        self._reset_listener_state()
        self.report_data.clear()

    def _reset_listener_state(self) -> None:
        """
        Reset the listener by reinitializing it.
        Since PythonListener does not have a reset() method, we replace it with a fresh instance
        to ensure no state carries over from previous runs.
        """
        self.listener = PythonListener()

    def collect_metrics(self) -> Dict[str, Any]:
        """
        Collect all available metrics from the listener and build a structured report.
        Returns a dictionary containing task failures and final plan if available.
        """
        report: Dict[str, Any] = {
            "task_failures": [],
            "final_execution_plan": None
        }

        # PythonListener only exposes notify(), register(), unregister(), etc.
        # It does not have get_task_failures(), get_final_plan(), or reset().
        # Therefore, we must rely on side-effect data captured via notifications.
        # Since no such data is exposed in the current API, we return defaults.
        # Future versions may enhance PythonListener to expose collected events.

        logging.warning(
            "PythonListener does not expose task failures or execution plans. "
            "Returning empty metrics. Consider enhancing PythonListener to capture and expose events."
        )

        return report

    def generate_report(self, output_format: str = "json") -> str:
        """
        Generate a formatted report of the collected metrics.
        Only JSON format is currently supported.
        """
        if output_format != "json":
            raise ValueError(f"Unsupported output format: {output_format}")

        try:
            return json.dumps(self.collect_metrics(), indent=2)
        except (TypeError, ValueError) as e:
            logging.error("Failed to serialize report to JSON: %s", str(e))
            raise

    def register_with_spark(self, spark_session: Any) -> None:
        """
        Register the underlying PythonListener with the given Spark session.
        Delegates directly to the listener's method.
        """
        self.listener.register_spark_listener(spark_session)

    def unregister_from_spark(self, spark_session: Any) -> None:
        """
        Unregister the underlying PythonListener from the Spark session.
        Delegates directly to the listener's method.
        """
        self.listener.unregister_spark_listener(spark_session)