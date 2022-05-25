# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
# Certain portions of the contents of this file are derived from TPC-DS version 3.2.0
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
# and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-DS Benchmark and as such any results
# obtained using this file are not comparable to published TPC-DS Benchmark results, as the results
# obtained from using this file do not comply with the TPC-DS Benchmark.
#

from pyspark import SparkContext
from pyspark.java_gateway import ensure_callback_server_started

from pyspark_spy.interface import SparkListenerInterface


def register_listener(sc: SparkContext, *listeners: SparkListenerInterface):
    """register SparkListener instance to SparkContext. Start call back server for py4j by pyspark
    public method.

    Args:
        sc (SparkContext): Spark Context
    """
    ensure_callback_server_started(gw = sc._gateway)

    for listener in listeners:
        sc._jsc.sc().addSparkListener(listener)

class TaskFailureListener(SparkListenerInterface):
    """Listener to track task failures. Each failed task will provide its failure reason.
    """
    def __init__(self):
        self.reason = ''
        self.failures = []

    def onTaskEnd(self, taskEnd):
        self.reason = taskEnd.reason().toString()
        if self.reason != 'Success':
            self.failures.append(self.reason)
        super().onTaskEnd(taskEnd)
