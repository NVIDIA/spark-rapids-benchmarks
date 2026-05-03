// File: utils/spark_utils.py
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
import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates or retrieves a Spark session with standard configurations for benchmarking.

    :param app_name: Name of the Spark application.
    :return: Configured SparkSession.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "4")
        .config("spark.driver.memory", "8g")
    )
    return builder.getOrCreate()

def get_python_benchmark_reporter(listener: object) -> object:
    """
    Creates a Python benchmark reporter instance.

    :param listener: The listener instance.
    :return: The Python benchmark reporter instance.
    """
    return PythonBenchmarkReporter(listener)

class PythonBenchmarkReporter:
    def __init__(self, listener: object):
        self.listener = listener

    def get_task_failures(self) -> list:
        """
        Retrieves task failures from the listener.

        :return: A list of task failures.
        """
        return self.listener.get_task_failures()

    def get_final_plan(self) -> dict:
        """
        Retrieves the final plan from the listener.

        :return: The final plan.
        """
        return self.listener.get_final_plan()

    def reset(self) -> None:
        """
        Resets the listener.
        """
        self.listener.reset()

def get_spark_benchmark_reporter(listener: object) -> object:
    """
    Creates a Spark benchmark reporter instance.

    :param listener: The listener instance.
    :return: The Spark benchmark reporter instance.
    """
    return SparkBenchmarkReporter(listener)

class SparkBenchmarkReporter:
    def __init__(self, listener: object):
        self.listener = listener

    def get_task_failures(self) -> list:
        """
        Retrieves task failures from the listener.

        :return: A list of task failures.
        """
        return self.listener.get_task_failures()

    def get_final_plan(self) -> dict:
        """
        Retrieves the final plan from the listener.

        :return: The final plan.
        """
        return self.listener.get_final_plan()

    def reset(self) -> None:
        """
        Resets the listener.
        """
        self.listener.reset()