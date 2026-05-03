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
# obtained from using this file do not comply with the TPC-H Benchmark.
#

import json
import os
import logging

from utils.python_benchmark_reporter import PythonListener

class PysparkBenchReport:
    def __init__(self, listener):
        self.listener = listener
        self.task_failures = []
        self.final_plan = None

    def get_task_failures(self):
        return self.task_failures

    def get_final_plan(self):
        return self.final_plan

    def reset(self):
        self.task_failures = []
        self.final_plan = None

    def process_task(self, task):
        try:
            self.listener.notify(task)
            self.final_plan = self.listener.get_final_plan()
        except Exception as e:
            logging.error(f"Error processing task: {e}")
            self.task_failures.append(task)

    def process_tasks(self, tasks):
        for task in tasks:
            self.process_task(task)

def main():
    listener = PythonListener()
    report = PysparkBenchReport(listener)
    tasks = [...]  # Replace with actual task data
    report.process_tasks(tasks)
    print(json.dumps(report.get_task_failures()))
    print(json.dumps(report.get_final_plan()))

if __name__ == "__main__":
    main()