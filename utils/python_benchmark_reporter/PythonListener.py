#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from pyspark import SparkContext
from pyspark.java_gateway import ensure_callback_server_started

class PythonListener(object):
    package = "com.nvidia.spark.rapids.listener"

    @staticmethod
    def get_manager():
        jvm = SparkContext.getOrCreate()._jvm
        manager = getattr(jvm, "{}.{}".format(PythonListener.package, "Manager"))
        return manager

    def __init__(self):
        self.uuid = None
        self.failures = []

    def notify(self, obj):
        """This method is required by Scala Listener interface
        we defined above.
        """
        self.failures.append(obj)

    def register(self):
        ensure_callback_server_started(gw = SparkContext.getOrCreate()._gateway)
        manager = PythonListener.get_manager()
        self.uuid = manager.register(self)
        return self.uuid

    def unregister(self):
        manager =  PythonListener.get_manager()
        manager.unregister(self.uuid)
        self.uuid = None

    # should call after register
    def register_spark_listener(self):
        manager = PythonListener.get_manager()
        manager.registerSparkListener()

    def unregister_spark_listener(self):
        manager = PythonListener.get_manager()
        manager.unregisterSparkListener()

    class Java:
        implements = ["com.nvidia.spark.rapids.listener.Listener"]
