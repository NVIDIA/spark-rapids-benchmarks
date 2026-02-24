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

import argparse
import timeit
import pyspark

from datetime import datetime

from pyspark.sql.types import *
from pyspark.sql.functions import col
from nds_h_schema import *

# Note the specific partitioning is applied when save the parquet data files.
TABLE_PARTITIONING = {
    'part': 'p_partkey',
    'supplier': 's_suppkey',
    'partsupp': 'ps_partkey',
    'customer': 'c_custkey',
    'orders': 'o_orderkey',
    'nation': 'n_nationkey',
    'region':'r_regionkey'
}


def load(session, filename, schema, input_format, delimiter="|", header="false", prefix=""):
    data_path = prefix + '/' + filename
    if input_format == 'csv':
        print("Schema is {}",schema)
        df = session.read.option("delimiter", delimiter).option("header", header)\
                .option("encoding", "ISO-8859-1").csv(data_path, schema=schema)
        print("Head is {}",df.head())
        return df
    elif input_format in ['parquet', 'orc', 'avro', 'json']:
        return session.read.format(input_format).load(data_path)
    # TODO: all of the output formats should be also supported as input format possibilities
    # remains 'iceberg', 'delta'
    else:
        raise ValueError("Unsupported input format: {}".format(input_format))

def store(session,
          df,
          filename,
          output_format,
          output_mode,
          prefix=""):
    """Create Iceberg tables by CTAS

    Args:
        session (SparkSession): a working SparkSession instance
        df (DataFrame): DataFrame to be serialized into Iceberg table
        filename (str): name of the table(file)
        output_format (str): parquet, orc or avro
        output_mode (str): save modes as defined by "https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes.
        iceberg_write_format (bool): write data into Iceberg tables with specified format
        compression (str): compression codec for converted data when saving to disk
        prefix (str): output data path when not using Iceberg.
    """
    data_path = prefix + '/' + filename
    df = df.repartition(200)
    writer = df.write
    writer = writer.format(output_format).mode(output_mode)
    writer.saveAsTable(filename, path=data_path)

def transcode(args):
    """
    Default function that is triggered post argument parsing

    Parameters: 
    args ( argparse.Namespace ): returns the parsed arguments in the namespace

    Returns:
    Nothing

    """
    session_builder = pyspark.sql.SparkSession.builder
    session = session_builder.appName(f"NDS-H - transcode - {args.output_format}").getOrCreate()
    session.sparkContext.setLogLevel(args.log_level)
    results = {}

    schemas = get_schemas()
    
    trans_tables = schemas

    if args.tables:
        for t in args.tables:
            if t not in trans_tables.keys() :
                raise Exception(f"invalid table name: {t}. Valid tables are: {schemas.keys()}")
        trans_tables = {t: trans_tables[t] for t in args.tables if t in trans_tables}


    start_time = datetime.now()
    print(f"Load Test Start Time: {start_time}")
    for fn, schema in trans_tables.items():
        results[fn] = timeit.timeit(
            lambda: store(session,
                          load(session,
                               f"{fn}",
                               schema,
                               input_format=args.input_format,
                               prefix=args.input_prefix),
                          f"{fn}",
                          args.output_format,
                          args.output_mode,
                          args.output_prefix),
            number=1)

    end_time = datetime.now()
    delta = (end_time - start_time).total_seconds()
    print(f"Load Test Finished at: {end_time}")
    print(f"Load Test Time: {delta} seconds")
    # format required at TPC-DS Spec 4.3.1
    end_time_formatted = end_time.strftime("%m%d%H%M%S%f")[:-5]
    print(f"RNGSEED used :{end_time_formatted}")

    report_text = ""
    report_text += f"Load Test Time: {delta} seconds\n"
    report_text += f"Load Test Finished at: {end_time}\n"
    report_text += f"RNGSEED used: {end_time_formatted}\n"

    for table, duration in results.items():
        report_text += "Time to convert '%s' was %.04fs\n" % (table, duration)

    report_text += "\n\n\nSpark configuration follows:\n\n"

    with open(args.report_file, "w") as report:
        report.write(report_text)
        print(report_text)

        for conf in session.sparkContext.getConf().getAll():
            report.write(str(conf) + "\n")
            print(conf)


if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_prefix',
        help='input folder')
    parser.add_argument(
        'output_prefix',
        help='output folder')
    parser.add_argument(
        'report_file',
        help='location to store a performance report(local)')
    parser.add_argument(
        '--output_mode',
        choices=['overwrite', 'append', 'ignore', 'error', 'errorifexists'],
        help="save modes as defined by " +
        "https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes." +
        "default value is errorifexists, which is the Spark default behavior.",
        default="errorifexists")
    parser.add_argument(
        '--input_format',
        choices=['csv', 'parquet', 'orc', 'avro', 'json'],
        default='csv',
        help='input data format to be converted. default value is csv.'
    )
    parser.add_argument(
        '--output_format',
        choices=['parquet', 'orc', 'avro', 'json', 'iceberg', 'delta'],
        default='parquet',
        help="output data format when converting CSV data sources."
    )
    parser.add_argument(
        '--tables',
        type=lambda s: s.split(','),
        help="specify table names by a comma separated string. e.g. 'catalog_page,catalog_sales'.")
    parser.add_argument(
        '--log_level',
        help='set log level for Spark driver log. Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN(default: INFO)',
        default="INFO")
    args = parser.parse_args()
    transcode(args)
