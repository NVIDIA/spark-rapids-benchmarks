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
import timeit
import pyspark
import os

from datetime import datetime

from pyspark.sql.types import *
from pyspark.sql.functions import col
from nds_schema import *

# Note the specific partitioning is applied when save the parquet data files.
TABLE_PARTITIONING = {
    'catalog_sales': 'cs_sold_date_sk',
    'catalog_returns': 'cr_returned_date_sk',
    'inventory': 'inv_date_sk',
    'store_sales': 'ss_sold_date_sk',
    'store_returns': 'sr_returned_date_sk',
    'web_sales': 'ws_sold_date_sk',
    'web_returns': 'wr_returned_date_sk'
}


def load(session, filename, schema, delimiter="|", header="false", prefix=""):
    data_path = prefix + '/' + filename
    return session.read.option("delimiter", delimiter).option("header", header).csv(data_path, schema=schema)


def store(session,
          df,
          filename,
          output_format,
          output_mode,
          iceberg_write_format,
          compression,
          prefix="",
          delta_unmanaged=False):
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
    if output_format == "iceberg":
        if output_mode == 'overwrite':
            session.sql(f"drop table if exists {filename}")
        CTAS = f"create table {filename} using iceberg "
        if filename in TABLE_PARTITIONING.keys():
           df.repartition(
               col(TABLE_PARTITIONING[filename])).sortWithinPartitions(
                   TABLE_PARTITIONING[filename]).createOrReplaceTempView("temptbl")
           CTAS += f"partitioned by ({TABLE_PARTITIONING[filename]})"
        else:
            df.coalesce(1).createOrReplaceTempView("temptbl")
        CTAS += f" tblproperties('write.format.default' = '{iceberg_write_format}'"
        # Iceberg now only support compression codec option for Parquet and Avro write.
        if compression:
            if iceberg_write_format == "parquet":
                CTAS += f", 'write.parquet.compression-codec' = '{compression}'"
            elif iceberg_write_format == "avro":
                CTAS += f", 'write.avro.compression-codec' = '{compression}'"
        CTAS += ")"
        CTAS += " as select * from temptbl"
        session.sql(CTAS)
    elif output_format == "delta" and not delta_unmanaged:
        if output_mode == 'overwrite':
            session.sql(f"drop table if exists {filename}")
        CTAS = f"create table {filename} using delta "
        if filename in TABLE_PARTITIONING.keys():
           df.repartition(
               col(TABLE_PARTITIONING[filename])).sortWithinPartitions(
                   TABLE_PARTITIONING[filename]).createOrReplaceTempView("temptbl")
           CTAS += f"partitioned by ({TABLE_PARTITIONING[filename]})"
        else:
            df.coalesce(1).createOrReplaceTempView("temptbl")
        # Delta Lake doesn't have specific compression properties, set it by `spark.sql.parquet.compression.codec`
        # Note Delta Lake only support Parquet.
        if compression:
            session.conf.set("spark.sql.parquet.compression.codec", compression)
        CTAS += " as select * from temptbl"
        session.sql(CTAS)
    else:
        data_path = prefix + '/' + filename
        if filename in TABLE_PARTITIONING.keys():
            df = df.repartition(
                col(TABLE_PARTITIONING[filename])).sortWithinPartitions(
                    TABLE_PARTITIONING[filename])
            writer = df.write
            if compression:
                writer = writer.option('compression', compression)
            writer.format(output_format).mode(
                output_mode).partitionBy(TABLE_PARTITIONING[filename]).save(data_path)
        else:
            writer = df.coalesce(1).write
            if compression:
                writer = writer.option('compression', compression)
            writer.format(output_format).mode(output_mode).save(data_path)

def transcode(args):
    session_builder = pyspark.sql.SparkSession.builder
    if args.output_format == "iceberg":
        session_builder.config("spark.sql.catalog.spark_catalog.warehouse", args.output_prefix)
    if args.output_format == "delta" and not args.delta_unmanaged:
        session_builder.config("spark.sql.warehouse.dir", args.output_prefix)
        session_builder.config("spark.sql.catalogImplementation", "hive")
    session = session_builder.appName(f"NDS - transcode - {args.output_format}").getOrCreate()
    session.sparkContext.setLogLevel(args.log_level)
    results = {}

    schemas = get_schemas(use_decimal=not args.floats)
    maintenance_schemas = get_maintenance_schemas(use_decimal=not args.floats)

    if args.update:
        trans_tables = maintenance_schemas
    else:
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
                               prefix=args.input_prefix),
                          f"{fn}",
                          args.output_format,
                          args.output_mode,
                          args.iceberg_write_format,
                          args.compression,
                          args.output_prefix,
                          args.delta_unmanaged),
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
        help='text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"; the default is empty)')
    parser.add_argument(
        'output_prefix',
        help='text to prepend to every output file (e.g., "hdfs:///ds-parquet"; the default is empty)' +
        '. If output_format is "iceberg", this argument will be regarded as the value of property ' +
        '"spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog ' +
        'session name "spark_catalog" is supported now, customized catalog is not ' +
        'yet supported.')
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
    parser.add_argument(
        '--floats',
        action='store_true',
        help='replace DecimalType with DoubleType when saving parquet files. If not specified, decimal data will be saved.')
    parser.add_argument(
        '--update',
        action='store_true',
        help='transcode the source data or update data'
    )
    parser.add_argument(
        '--iceberg_write_format',
        choices=['parquet', 'orc', 'avro'],
        default='parquet',
        help='File format for the Iceberg table; parquet, avro, or orc'
    )
    parser.add_argument(
        '--compression',
        help='Compression codec to use when saving data.' +
        ' See https://iceberg.apache.org/docs/latest/configuration/#write-properties ' +
        ' for supported codecs in Iceberg.' +
        ' See https://spark.apache.org/docs/latest/sql-data-sources.html' +
        ' for supported codecs for Spark built-in formats.' +
        ' When not specified, the default for the requested output format will be used.'
    )
    parser.add_argument(
        '--delta_unmanaged',
        action='store_true',
        help='Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without ' +
        'leveraging a Metastore service.')
    args = parser.parse_args()
    transcode(args)
