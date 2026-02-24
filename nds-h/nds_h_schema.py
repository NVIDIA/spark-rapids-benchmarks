#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
# -----
#
# Certain portions of the contents of this file are derived from TPC-H version 3.0.1
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

from pyspark.sql.types import *


def get_schemas():
    """get the schemas of all tables.

    Returns:
        dict: {table_name: schema}
    """
    SCHEMAS = {}

    # The specification states that "Identifier means that the column shall be able to hold any
    # key value generated for that column". Some tables have more rows than others so we can
    # choose to use different types per table.
    identifier_int = IntegerType()
    identifier_long = LongType()

    SCHEMAS["part"] = StructType([
        StructField("p_partkey", LongType(), False),
        StructField("p_name", StringType(), False),
        StructField("p_mfgr", StringType(), False),
        StructField("p_brand", StringType(), False),
        StructField("p_type", StringType(), False),
        StructField("p_size", IntegerType(), False),
        StructField("p_container", StringType(), False),
        StructField("p_retailprice", DecimalType(11, 2), False),
        StructField("p_comment", StringType(), False),
        StructField("ignore", StringType(), True)
    ])

    SCHEMAS['supplier'] = StructType([
            StructField("s_suppkey", LongType(), False),
            StructField("s_name", StringType(), False),
            StructField("s_address", StringType(), False),
            StructField("s_nationkey", LongType(), False),
            StructField("s_phone", StringType(), False),
            StructField("s_acctbal", DecimalType(11, 2), False),
            StructField("s_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    SCHEMAS['partsupp'] = StructType([
            StructField("ps_partkey", LongType(), False),
            StructField("ps_suppkey", LongType(), False),
            StructField("ps_availqty", IntegerType(), False),
            StructField("ps_supplycost", DecimalType(11, 2), False),
            StructField("ps_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    SCHEMAS['customer'] = StructType([
            StructField("c_custkey", LongType(), False),
            StructField("c_name", StringType(), False),
            StructField("c_address", StringType(), False),
            StructField("c_nationkey", LongType(), False),
            StructField("c_phone", StringType(), False),
            StructField("c_acctbal", DecimalType(11, 2), False),
            StructField("c_mktsegment", StringType(), False),
            StructField("c_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    SCHEMAS['orders'] = StructType([
            StructField("o_orderkey", LongType(), False),
            StructField("o_custkey", LongType(), False),
            StructField("o_orderstatus", StringType(), False),
            StructField("o_totalprice", DecimalType(11, 2), False),
            StructField("o_orderdate", DateType(), False),
            StructField("o_orderpriority", StringType(), False),
            StructField("o_clerk", StringType(), False),
            StructField("o_shippriority", IntegerType(), False),
            StructField("o_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    SCHEMAS['lineitem'] = StructType([
            StructField("l_orderkey", LongType(), False),
            StructField("l_partkey", LongType(), False),
            StructField("l_suppkey", LongType(), False),
            StructField("l_linenumber", IntegerType(), False),
            StructField("l_quantity", DecimalType(11, 2), False),
            StructField("l_extendedprice", DecimalType(11, 2), False),
            StructField("l_discount", DecimalType(11, 2), False),
            StructField("l_tax", DecimalType(11, 2), False),
            StructField("l_returnflag", StringType(), False),
            StructField("l_linestatus", StringType(), False),
            StructField("l_shipdate", DateType(), False),
            StructField("l_commitdate", DateType(), False),
            StructField("l_receiptdate", DateType(), False),
            StructField("l_shipinstruct", StringType(), False),
            StructField("l_shipmode", StringType(), False),
            StructField("l_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    SCHEMAS['nation'] = StructType([
            StructField("n_nationkey", LongType(), False),
            StructField("n_name", StringType(), False),
            StructField("n_regionkey", LongType(), False),
            StructField("n_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    SCHEMAS['region'] = StructType([
            StructField("r_regionkey", LongType(), False),
            StructField("r_name", StringType(), False),
            StructField("r_comment", StringType(), False),
            StructField("ignore", StringType(), True)
    ])

    return SCHEMAS

if __name__ == "__main__":
    # Test code
    print(get_schemas())