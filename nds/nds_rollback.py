#!/usr/bin/env python3
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

import argparse

from pyspark.sql import SparkSession

tables_to_rollback = [
    'catalog_sales',
    'inventory',
    'store_returns',
    'store_sales',
    'web_returns',
    'web_sales']


def rollback(spark, timestamp, tables_to_rollback):
    """roll back the tables to the timestamp"""
    for table in tables_to_rollback:
        print(f"Rolling back {table} to {timestamp}")
        rollback_sql = f"CALL spark_catalog.system.rollback_to_timestamp('{table}', TIMESTAMP '{timestamp}')"
        spark.sql(rollback_sql)


if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('timestamp', help='timestamp to rollback to')
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Rollback").getOrCreate()
    rollback(spark, args.timestamp, tables_to_rollback)
    spark.stop()