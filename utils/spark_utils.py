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

import logging
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession
from pyspark import SparkContext


def get_spark_session(app_name: str, conf: Optional[Dict[str, Any]] = None) -> SparkSession:
    """
    Get or create a Spark session with the given application name and configuration.

    :param app_name: Name of the Spark application
    :param conf: Optional dictionary of Spark configuration properties
    :return: SparkSession instance
    """
    if not isinstance(app_name, str):
        raise TypeError("app_name must be a string")
    if conf is not None and not isinstance(conf, dict):
        raise TypeError("conf must be a dictionary or None")

    builder = SparkSession.builder.appName(app_name)
    if conf:
        for key, value in conf.items():
            if not isinstance(key, str):
                raise TypeError("Spark configuration keys must be strings")
            if not isinstance(value, str):
                raise TypeError("Spark configuration values must be strings")
            builder.config(key, value)

    return builder.getOrCreate()


def get_python_listener() -> object:
    """
    Get a Python listener instance.

    :return: Python listener instance
    """
    from utils.python_benchmark_reporter import PythonListener
    return PythonListener()


def get_task_failures(listener: object) -> Dict[str, Any]:
    """
    Get task failures from the given listener.

    :param listener: Python listener instance
    :return: Dictionary of task failures
    """
    if not hasattr(listener, 'notify'):
        raise TypeError("Listener must have a notify method")
    return listener.notify()


def get_final_plan(listener: object) -> Dict[str, Any]:
    """
    Get the final plan from the given listener.

    :param listener: Python listener instance
    :return: Dictionary of the final plan
    """
    if not hasattr(listener, 'notify'):
        raise TypeError("Listener must have a notify method")
    return listener.notify()


def reset_listener(listener: object) -> None:
    """
    Reset the given listener.

    :param listener: Python listener instance
    :return: None
    """
    if not hasattr(listener, 'reset'):
        raise TypeError("Listener must have a reset method")
    listener.reset()