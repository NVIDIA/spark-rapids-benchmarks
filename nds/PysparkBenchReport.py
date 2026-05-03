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
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PysparkBenchReport:
    """
    A reporter class that collects and writes benchmarking metadata
    using a PythonListener for Spark instrumentation.
    """

    def __init__(self, listener, output_dir: str = "."):
        """
        Initialize the reporter with a listener and output directory.

        :param listener: An instance of PythonListener for Spark event monitoring.
        :param output_dir: Directory where report files will be written.
        """
        if not hasattr(listener, 'notify') or not callable(getattr(listener, 'notify')):
            raise ValueError("Listener must have a 'notify' method.")
        if not hasattr(listener, 'register') or not callable(getattr(listener, 'register')):
            raise ValueError("Listener must have a 'register' method.")
        if not hasattr(listener, 'unregister') or not callable(getattr(listener, 'unregister')):
            raise ValueError("Listener must have an 'unregister' method.")

        self.listener = listener
        self.output_dir = output_dir
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.report_data: Dict[str, Any] = {}

    def start_benchmark(self):
        """
        Mark the start of the benchmark and initialize timing.
        """
        self.start_time = time.time()
        logger.info("Benchmark started at %s", self.start_time)

    def end_benchmark(self):
        """
        Mark the end of the benchmark, collect final data, and generate report.
        """
        self.end_time = time.time()
        logger.info("Benchmark ended at %s", self.end_time)

        # Collect final metadata
        self.report_data.update({
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.end_time - self.start_time if self.start_time else None,
            "task_failures": self._get_task_failures_fallback(),
            "final_execution_plan": self._get_final_plan_fallback()
        })

        self._write_report()

    def _get_task_failures_fallback(self) -> int:
        """
        Fallback method to extract task failures.
        Since PythonListener does not expose get_task_failures(), we infer from internal state if possible.
        Otherwise, return 0 as default.
        """
        try:
            # Attempt to access internal listener state if available
            if hasattr(self.listener, '_event_log'):
                return sum(1 for event in self.listener._event_log if event.get('event') == 'TaskFailed')
        except Exception as e:
            logger.warning("Could not extract task failures from listener: %s", str(e))
        return 0

    def _get_final_plan_fallback(self) -> str:
        """
        Fallback method to extract final execution plan.
        Since PythonListener does not expose get_final_plan(), attempt to retrieve last submitted job plan.
        Otherwise, return empty string.
        """
        try:
            if hasattr(self.listener, '_last_execution_plan'):
                return str(self.listener._last_execution_plan)
            if hasattr(self.listener, '_event_log'):
                # Search for last SparkListenerJobEnd with plan description
                for event in reversed(self.listener._event_log):
                    if event.get('event') == 'SparkListenerJobEnd' and 'planDescription' in event:
                        return event['planDescription']
        except Exception as e:
            logger.warning("Could not extract final execution plan: %s", str(e))
        return ""

    def reset_listener_state(self):
        """
        Reset any internal listener state if supported.
        Since PythonListener lacks reset(), we manually clear known state fields if present.
        """
        try:
            if hasattr(self.listener, '_event_log'):
                self.listener._event_log.clear()
            if hasattr(self.listener, '_last_execution_plan'):
                self.listener._last_execution_plan = ""
            logger.debug("Listener state manually reset.")
        except Exception as e:
            logger.warning("Could not reset listener state: %s", str(e))

    def _write_report(self):
        """
        Write the collected benchmark report to a JSON file in the output directory.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        timestamp = int(self.end_time) if self.end_time else int(time.time())
        report_path = os.path.join(self.output_dir, f"benchmark_report_{timestamp}.json")

        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(self.report_data, f, indent=4, sort_keys=True)
            logger.info("Benchmark report written to %s", report_path)
        except Exception as e:
            logger.error("Failed to write benchmark report: %s", str(e))
            raise

    def cleanup(self):
        """
        Perform cleanup actions after benchmark completion.
        Unregister listeners and reset state where possible.
        """
        try:
            self.listener.unregister_spark_listener()
            logger.debug("Spark listener unregistered.")
        except Exception as e:
            logger.warning("Failed to unregister Spark listener: %s", str(e))

        self.reset_listener_state()