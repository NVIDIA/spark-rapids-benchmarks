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
import logging
from typing import Dict, Any, Optional

# Configure logger
logger = logging.getLogger(__name__)


class PysparkBenchReport:
    """
    A reporter class that collects and writes benchmarking results from PySpark runs.
    Communicates with a PythonListener to gather execution metrics and plans.
    """

    def __init__(self, listener: Any, output_dir: str):
        """
        Initialize the reporter with a listener and output directory.

        Args:
            listener: An instance of PythonListener used to interface with Spark listeners.
            output_dir: Directory where benchmark reports will be saved.
        """
        if not hasattr(listener, 'notify') or not callable(getattr(listener, 'notify')):
            raise ValueError("Listener must have a 'notify' method")

        self.listener = listener
        self.output_dir = output_dir

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_report(self, benchmark_name: str, query_id: str) -> None:
        """
        Generate a benchmark report using data from the listener.

        This method coordinates the collection of relevant metrics and writes them
        to a structured JSON file in the output directory.

        Args:
            benchmark_name: Name of the benchmark (e.g., TPC-H, TPC-DS).
            query_id: Identifier for the specific query being benchmarked.
        """
        start_time = time.time()
        logger.info(f"Generating benchmark report for {benchmark_name}/{query_id}")

        # Notify listener to prepare final state
        self.listener.notify(event_type="finalizing_report", data={"query_id": query_id})

        # Collect basic metadata
        report_data: Dict[str, Any] = {
            "benchmark": benchmark_name,
            "query_id": query_id,
            "timestamp": int(start_time),
            "results": {}
        }

        # Placeholder for results — in real implementation, extract from Spark context via listener
        # Since PythonListener only supports notify/register/unregister, we rely on side effects
        # or external state mutation that should have been triggered prior to this call.

        # Example placeholder structure
        report_data["results"]["execution_time_ms"] = self._get_execution_time_ms(query_id)
        report_data["results"]["task_failures"] = self._get_task_failures(query_id)
        report_data["results"]["final_physical_plan"] = self._get_final_plan(query_id)

        # Write report to file
        report_path = os.path.join(self.output_dir, f"report_{benchmark_name}_{query_id}.json")
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2)
            logger.info(f"Benchmark report written to {report_path}")
        except OSError as e:
            logger.error(f"Failed to write benchmark report to {report_path}: {e}")
            raise

        duration = time.time() - start_time
        logger.info(f"Benchmark report generation completed in {duration:.2f} seconds")

    def _get_execution_time_ms(self, query_id: str) -> Optional[int]:
        """
        Retrieve execution time in milliseconds for the given query.

        In a real implementation, this would pull from Spark metrics via the listener.

        Args:
            query_id: Query identifier.

        Returns:
            Execution time in milliseconds or None if unavailable.
        """
        # Simulate retrieval via listener pattern
        try:
            response = self.listener.notify(
                event_type="get_metric",
                data={"query_id": query_id, "metric": "execution_time_ms"}
            )
            return int(response.get("value")) if response and "value" in response else None
        except Exception as e:
            logger.warning(f"Could not retrieve execution time for {query_id}: {e}")
            return None

    def _get_task_failures(self, query_id: str) -> int:
        """
        Retrieve the number of task failures for the given query.

        Args:
            query_id: Query identifier.

        Returns:
            Number of task failures; defaults to 0 if not available.
        """
        try:
            response = self.listener.notify(
                event_type="get_metric",
                data={"query_id": query_id, "metric": "task_failures"}
            )
            return int(response.get("value")) if response and "value" in response else 0
        except Exception as e:
            logger.warning(f"Could not retrieve task failures for {query_id}: {e}")
            return 0

    def _get_final_plan(self, query_id: str) -> Optional[str]:
        """
        Retrieve the final physical plan for the given query.

        Args:
            query_id: Query identifier.

        Returns:
            Final physical plan as a string or None if unavailable.
        """
        try:
            response = self.listener.notify(
                event_type="get_plan",
                data={"query_id": query_id, "plan_type": "physical"}
            )
            return response.get("plan") if response and "plan" in response else None
        except Exception as e:
            logger.warning(f"Could not retrieve final plan for {query_id}: {e}")
            return None

    def reset_listener_state(self, query_id: str) -> None:
        """
        Reset listener state after a benchmark run.

        Uses notify pattern to signal reset, since direct reset() is not available.

        Args:
            query_id: Query identifier being reset.
        """
        try:
            self.listener.notify(
                event_type="reset",
                data={"query_id": query_id}
            )
            logger.debug(f"Listener state reset for query {query_id}")
        except Exception as e:
            logger.error(f"Failed to reset listener state for {query_id}: {e}")
            raise