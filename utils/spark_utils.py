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
                raise TypeError(f"Configuration key must be a string, got {type(key)}")
            builder = builder.config(key, str(value))
    return builder.getOrCreate()


def get_spark_context(spark: SparkSession) -> SparkContext:
    """
    Safely extract SparkContext from SparkSession.

    :param spark: Active SparkSession
    :return: SparkContext instance
    """
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession instance")
    return spark.sparkContext


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stop the given Spark session gracefully.

    :param spark: SparkSession to stop
    """
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession instance")
    try:
        spark.stop()
        logging.info("Spark session stopped successfully.")
    except Exception as e:
        logging.error("Failed to stop Spark session: %s", str(e))
        raise


def is_spark_active() -> bool:
    """
    Check if there is an active Spark context.

    :return: True if Spark context is active, False otherwise
    """
    try:
        sc = SparkContext.getOrCreate()
        return sc._jsc.sc() is not None  # pylint: disable=protected-access
    except Exception:  # pylint: disable=broad-except
        return False


def set_spark_log_level(spark: SparkSession, log_level: str = "WARN") -> None:
    """
    Set the log level for Spark drivers and executors.

    :param spark: Active SparkSession
    :param log_level: Log level to set (e.g., INFO, WARN, ERROR)
    """
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a SparkSession instance")
    if not isinstance(log_level, str):
        raise TypeError("log_level must be a string")
    valid_levels = ["ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"]
    if log_level not in valid_levels:
        raise ValueError(f"log_level must be one of {valid_levels}")

    sc = get_spark_context(spark)
    sc.setLogLevel(log_level)
    logging.info("Spark log level set to %s", log_level)