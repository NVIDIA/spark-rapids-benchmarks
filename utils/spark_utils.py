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
                raise TypeError(f"Configuration key must be a string; got {type(key).__name__}")
            if not isinstance(value, (str, int, float, bool)):
                raise TypeError(
                    f"Configuration value for key '{key}' must be a primitive type (str, int, float, bool); "
                    f"got {type(value).__name__}"
                )
            builder = builder.config(key, str(value))

    session = builder.getOrCreate()
    logging.info(f"Spark session created with app name: {app_name}")
    return session


def get_spark_context() -> SparkContext:
    """
    Get the active SparkContext, creating it through a default SparkSession if necessary.

    :return: SparkContext instance
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = get_spark_session("default_app")
    return spark.sparkContext