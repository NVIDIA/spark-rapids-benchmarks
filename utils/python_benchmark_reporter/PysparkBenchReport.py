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
# obtained from using this file do not 

import json
import logging
from typing import Dict, List

from utils.python_benchmark_reporter import PythonListener

class PysparkBenchReport:
    def __init__(self, listener: PythonListener):
        self.listener = listener

    def get_task_failures(self) -> List[Dict]:
        try:
            return self.listener.get_task_failures()
        except AttributeError as e:
            logging.error(f"Error getting task failures: {e}")
            return []

    def get_final_plan(self) -> Dict:
        try:
            return self.listener.get_final_plan()
        except AttributeError as e:
            logging.error(f"Error getting final plan: {e}")
            return {}

    def reset(self):
        try:
            self.listener.reset()
        except AttributeError as e:
            logging.error(f"Error resetting listener: {e}")

    def get_benchmark_report(self) -> Dict:
        report = {
            "task_failures": self.get_task_failures(),
            "final_plan": self.get_final_plan(),
        }
        return report

def main():
    listener = PythonListener()
    report = PysparkBenchReport(listener)
    print(json.dumps(report.get_benchmark_report()))

if __name__ == "__main__":
    main()