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
        Initialize the reporter with a listener instance.

        Args:
            listener (PythonListener): The listener used to monitor Spark events.
        """
        if not isinstance(listener, PythonListener):
            raise TypeError("listener must be an instance of PythonListener")

        self.listener = listener
        self._task_failures: List[Dict[str, Any]] = []
        self._final_plan: Optional[str] = None

    def reset(self) -> None:
        """
        Reset internal state to prepare for a new benchmark run.
        """
        self._task_failures.clear()
        self._final_plan = None
        # Ensure listener is clean for new run
        if hasattr(self.listener, "unregister"):
            self.listener.unregister()

        if hasattr(self.listener, "register"):
            self.listener.register()

    def collect_task_failures(self) -> None:
        """
        Collect any task failures observed during execution.
        Since PythonListener does not expose get_task_failures,
        we rely on internal state or notifications.
        """
        # No direct method; task failures must be inferred via notifications
        # or stored during event processing. For now, no-op with fallback.
        pass

    def collect_final_execution_plan(self) -> None:
        """
        Collect the final physical plan after optimization.
        This must be captured via listener notifications.
        """
        # Final plan is not directly exposed by PythonListener.
        # This functionality must be implemented externally or via side effects.
        # No-op until plan capture is supported.
        pass

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate a structured benchmark report.

        Returns:
            Dict[str, Any]: A dictionary containing benchmark metrics and metadata.
        """
        report = {
            "task_failures": self._task_failures.copy(),
            "final_execution_plan": self._final_plan,
            "listener_registered": hasattr(self.listener, "register") and self.listener in getattr(self.listener, "_observers", []),
        }

        return report

    def notify(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Handle incoming events from the listener system.

        Args:
            event_type (str): Type of event (e.g., "task_end", "job_start").
            data (Dict[str, Any]): Event payload.
        """
        if event_type == "task_end" and data.get("status") == "FAILED":
            self._task_failures.append(data)

        if event_type == "query_execution" and "physicalPlan" in data:
            self._final_plan = data["physicalPlan"]

    def register_with_listener(self) -> None:
        """
        Register this reporter as an observer with the listener.
        """
        if hasattr(self.listener, "register"):
            self.listener.register()
        # Also register ourselves to receive notifications if listener supports it
        if hasattr(self.listener, "notify"):
            # Assume listener has observer pattern; we pass self as handler
            self.listener.notify = lambda et, d: self.notify(et, d)

    def unregister_from_listener(self) -> None:
        """
        Unregister from the listener to stop receiving events.
        """
        if hasattr(self.listener, "unregister"):
            self.listener.unregister()