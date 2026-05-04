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
# obtained from using this file do not conform to the TPC-H Benchmark specification.

import json
import logging
from typing import Any, Dict, List, Optional

from .PythonListener import PythonListener


class PysparkBenchReport:
    """
    A reporter class that collects and formats benchmarking results from PySpark workloads
    using a PythonListener to capture execution events.
    """

    def __init__(self, listener: PythonListener):
        """
        Initialize the reporter with a PythonListener instance.

        Args:
            listener: An instance of PythonListener used to observe Spark events.
        """
        if not isinstance(listener, PythonListener):
            raise TypeError("listener must be an instance of PythonListener")

        self.listener = listener
        self._task_failures: List[Dict[str, Any]] = []
        self._final_plan: Optional[str] = None

    def reset(self) -> None:
        """
        Reset internal state and clear previously collected data.
        """
        self._task_failures.clear()
        self._final_plan = None
        # Notify listener to reset its own state if needed
        self.listener.notify(event_type="reset")

    def collect_task_failures(self) -> None:
        """
        Collect task failure information via listener notifications.
        Since PythonListener doesn't expose get_task_failures(), we rely on event-driven collection.
        """
        # In a real implementation, this would be populated by handling events via notify()
        # For now, we simulate or assume the listener has a way to expose this data
        # But since it doesn't, we treat this as a no-op with fallback logging
        logging.debug("collect_task_failures: Listener does not support task failure retrieval")

    def capture_final_execution_plan(self) -> None:
        """
        Capture the final physical plan after query execution.
        """
        # Placeholder: actual plan capture would happen through Spark listener callbacks
        # Since PythonListener doesn't expose get_final_plan(), we simulate empty behavior
        logging.debug("capture_final_execution_plan: Not supported by current listener")

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate a structured benchmark report.

        Returns:
            A dictionary containing benchmark metrics and metadata.
        """
        report = {
            "metadata": {
                "reporter": self.__class__.__name__,
            },
            "execution": {
                "final_physical_plan": self._final_plan,
                "task_failures": self._task_failures,
                "failure_count": len(self._task_failures),
            }
        }
        return report

    def export_json(self, filepath: Optional[str] = None) -> str:
        """
        Export the benchmark report as JSON.

        Args:
            filepath: Optional path to save the JSON file.

        Returns:
            JSON string representation of the report.
        """
        report = self.generate_report()
        report_json = json.dumps(report, indent=2)

        if filepath:
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(report_json)
            except (OSError, IOError) as e:
                logging.error(f"Failed to write report to {filepath}: {e}")
                raise

        return report_json