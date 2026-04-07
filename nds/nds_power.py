#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2022-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
import csv
import os
import re
import sys
import time
from collections import OrderedDict
from pyspark.sql import SparkSession
from PysparkBenchReport import PysparkBenchReport
from pyspark.sql import DataFrame

from check import check_json_summary_folder, check_query_subset_exists, check_version
from nds_schema import get_schemas

# Python doesn't automatically include sibling directories in the import path.
# We need to explicitly add the utils directory to sys.path to import shared utilities.
# Note: __file__ is not defined when Databricks runs scripts via exec(), so fall back to sys.argv[0].
try:
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
utils_dir = os.path.join(parent_dir, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)
from spark_utils import setQueryName, clearQueryName
from profiler import Profiler

check_version()


def split_and_strip(str, delimiter):
    return [s.strip() for s in str.split(delimiter) if s.strip()]


def parse_query_content(query_content):
    """
    Parse query content to identify setup, benchmark, and cleanup sections.
    
    Args:
        query_content (str): The full query content potentially containing timing tags.
        
    Returns:
        dict: A dictionary with keys:
            - 'query_tpl': Query name template. This will be updated based on the query type and index.
            - 'setup': SQL string before '-- start benchmark'
            - 'benchmark': SQL string between '-- start benchmark' and '-- end benchmark'
            - 'cleanup': SQL string after '-- end benchmark'
    """
    lines = split_and_strip(query_content, '\n')
    head = lines[0]
    lines = lines[1:]  # Exclude the head line
    
    setup_lines = []
    benchmark_lines = []
    cleanup_lines = []
    
    current_section = 'init'
    
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue
        if line_stripped.startswith('-- end query'):
            break

        # Transitions allowed:
        # init -> (setup) -> benchmark -> (cleanup -> done)
        # All other transitions are invalid.

        if line_stripped == '-- start setup':
            if current_section != 'init':
                raise RuntimeError(f"Init expected, actual section {current_section}. "
                                   f"The setup section must be the first section if it exists.")
            current_section = 'setup'
            continue
        elif line_stripped == '-- end setup':
            if current_section != 'setup':
                raise RuntimeError("Mismatched end setup tag.")
            current_section = 'benchmark'
            continue
        elif line_stripped == '-- start cleanup':
            if current_section != 'benchmark':
                raise RuntimeError(f"benchmark expected, actual section {current_section}. "
                                   f"The cleanup section must come after the benchmark section.")
            current_section = 'cleanup'
            continue
        elif line_stripped == '-- end cleanup':
            if current_section != 'cleanup':
                raise RuntimeError("Mismatched end cleanup tag.")
            current_section = 'done'
            continue

        if current_section == 'init':
            # No tag has been found yet, so assume this is the benchmark section
            current_section = 'benchmark'

        if current_section == 'setup':
            setup_lines.append(line)
        elif current_section == 'benchmark':
            benchmark_lines.append(line)
        elif current_section == 'cleanup':
            cleanup_lines.append(line)
    
    if current_section != 'benchmark' and current_section != 'done':
        raise RuntimeError("Unclosed section detected in query content. Current section: " + current_section)

    # Convert lists to strings
    setup_sql = '\n'.join(setup_lines).strip()
    benchmark_sql = '\n'.join(benchmark_lines).strip()
    cleanup_sql = '\n'.join(cleanup_lines).strip()
    
    return {
        'query_tpl': head,
        'setup': split_and_strip(setup_sql, ';'),
        'benchmark': split_and_strip(benchmark_sql, ';'),
        'cleanup': split_and_strip(cleanup_sql, ';')
    }


def get_query_type(query_name):
    """
    Determine the query type based on its name suffix.
    
    Args:
        query_name (str): The name of the query.
        
    Returns:
        str: One of 'setup', 'cleanup', or 'benchmark'
    """
    if '_setup' in query_name:
        return 'setup'
    elif '_cleanup' in query_name:
        return 'cleanup'
    else:
        return 'benchmark'


def gen_sql_from_stream(query_stream_file_path):
    """Read Spark compatible query stream and split them one by one

    Args:
        query_stream_file_path (str): path of query stream generated by TPC-DS tool

    Returns:
        ordered dict: an ordered dict of {query_name: query content} query pairs
    """
    with open(query_stream_file_path, 'r') as f:
        stream = f.read()
    all_queries = stream.split('-- start query')[1:]
    # split query in query14, query23, query24, query39
    extended_queries = OrderedDict()
    for q in all_queries:
        # e.g. "-- start query 32 in stream 0 using template query98.tpl"
        query_name = q[q.find('template')+9: q.find('.tpl')]

        parsed = parse_query_content(q)

        def add_to_extended_queries(query_type, i, parsed):
            idx = i + 1
            subquery_cnt = len(parsed[query_type])
            if query_type == 'benchmark':
                dict_key = f"{query_name}_part{idx}" if subquery_cnt > 1 else query_name
            else:
                dict_key = f"{query_name}_{query_type}{idx}" if subquery_cnt > 1 else f"{query_name}_{query_type}"
            query_part = parsed['query_tpl'].replace('.tpl', f'_{query_type}{idx}.tpl') + '\n'
            query_part += parsed[query_type][i] + ';'
            extended_queries[dict_key] = query_part

        for i in range(len(parsed['setup'])):
            add_to_extended_queries('setup', i, parsed)
        for i in range(len(parsed['benchmark'])):
            add_to_extended_queries('benchmark', i, parsed)
        for i in range(len(parsed['cleanup'])):
            add_to_extended_queries('cleanup', i, parsed)

    # add "-- start" string back to each query
    for q_name, q_content in extended_queries.items():
        extended_queries[q_name] = '-- start query' + q_content

    return extended_queries

def setup_tables(spark_session, input_prefix, input_format, use_decimal, execution_time_list):
    """set up data tables in Spark before running the Power Run queries.

    Args:
        spark_session (SparkSession): a SparkSession instance to run queries.
        input_prefix (str): path of input data.
        input_format (str): type of input data source, e.g. parquet, orc, csv, json.
        use_decimal (bool): use decimal type for certain columns when loading data of text type.
        execution_time_list ([(str, str, int)]): a list to record query and its execution time.

    Returns:
        execution_time_list: a list recording query execution time.
    """
    spark_app_id = spark_session.conf.get("spark.app.id")
    # Create TempView for tables
    for table_name in get_schemas(False).keys():
        start = int(time.time() * 1000)
        table_path = input_prefix + '/' + table_name
        reader =  spark_session.read.format(input_format)
        if input_format in ['csv', 'json']:
            reader = reader.schema(get_schemas(use_decimal)[table_name])
        reader.load(table_path).createOrReplaceTempView(table_name)
        end = int(time.time() * 1000)
        print("====== Creating TempView for table {} ======".format(table_name))
        print("Time taken: {} millis for table {}".format(end - start, table_name))
        execution_time_list.append(
            (spark_app_id, "CreateTempView {}".format(table_name), end - start))
    return execution_time_list

def register_delta_tables(spark_session, input_prefix, execution_time_list):
    spark_app_id = spark_session.sparkContext.applicationId
    # Register tables for Delta Lake
    for table_name in get_schemas(False).keys():
        start = int(time.time() * 1000)
        # input_prefix must be absolute path: https://github.com/delta-io/delta/issues/555
        register_sql = f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{input_prefix}/{table_name}'"
        print(register_sql)
        spark_session.sql(register_sql)
        end = int(time.time() * 1000)
        print("====== Registering for table {} ======".format(table_name))
        print("Time taken: {} millis for table {}".format(end - start, table_name))
        execution_time_list.append(
            (spark_app_id, "Register {}".format(table_name), end - start))
    return execution_time_list


def parse_explain_str(explain_str):
    plan_strs = explain_str.split('\n\n')
    plan_dict = {}
    for plan_str in plan_strs:
        if plan_str.startswith('== Optimized Logical Plan =='):
            plan_dict['logical'] = plan_str
        elif plan_str.startswith('== Physical Plan =='):
            plan_dict['physical'] = plan_str
    return plan_dict


def run_one_query(spark_session,
                  profiler,
                  query,
                  query_name,
                  output_path,
                  output_format,
                  save_plan_path,
                  plan_types,
                  skip_execution):
    with profiler(query_name=query_name):
        print(f"Running query {query_name}")
        df = spark_session.sql(query)
        if not skip_execution:
            if not output_path:
                df.collect()
            else:
                ensure_valid_column_names(df).write.format(output_format).mode('overwrite').save(
                        output_path + '/' + query_name)
        if save_plan_path:
            os.makedirs(save_plan_path, exist_ok=True)
            explain_str = spark_session._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), 'extended')
            plans = parse_explain_str(explain_str)
            for plan_type in plan_types:
                with open(save_plan_path + '/' + query_name + "." + plan_type, 'w') as f:
                    f.write(plans[plan_type])


def ensure_valid_column_names(df: DataFrame):
    def is_column_start(char):
        return char.isalpha() or char == '_'

    def is_column_part(char):
        return char.isalpha() or char.isdigit() or char == '_'

    def is_valid(column_name):
        return len(column_name) > 0 and is_column_start(column_name[0]) and all(
            [is_column_part(char) for char in column_name[1:]])

    def make_valid(column_name):
        # To simplify: replace all invalid char with '_'
        valid_name = ''
        if is_column_start(column_name[0]):
            valid_name += column_name[0]
        else:
            valid_name += '_'
        for char in column_name[1:]:
            if not is_column_part(char):
                valid_name += '_'
            else:
                valid_name += char
        return valid_name

    def deduplicate(column_names):
        # In some queries like q35, it's possible to get columns with the same name. Append a number
        # suffix to resolve this problem.
        dedup_col_names = []
        for i,v in enumerate(column_names):
            count = column_names.count(v)
            index = column_names[:i].count(v)
            dedup_col_names.append(v+str(index) if count > 1 else v)
        return dedup_col_names

    valid_col_names = [c if is_valid(c) else make_valid(c) for c in df.columns]
    dedup_col_names = deduplicate(valid_col_names)
    return df.toDF(*dedup_col_names)


def get_query_subset(query_dict, subset):
    """Get a subset of queries from query_dict.
    The subset is specified by a list of query names.
    """
    check_query_subset_exists(query_dict, subset)
    return dict((k, query_dict[k]) for k in subset)


def get_query_subset_by_pattern(query_dict, patterns):
    """Get a subset of queries from query_dict.
    The subset is specified by a list of regex patterns for the query name.
    """
    selected_queries = OrderedDict()
    for pattern in patterns:
        for query_name in query_dict.keys():
            if re.match(pattern, query_name):
                selected_queries[query_name] = query_dict[query_name]
    if not selected_queries:
        msg = f"No query matched the specified subset patterns: {patterns}"
        raise Exception(msg)
    return selected_queries

def run_query_stream(input_prefix,
                     property_file,
                     query_dict,
                     time_log_output_path,
                     extra_time_log_output_path,
                     sub_queries,
                     sub_query_patterns,
                     warmup_iterations,
                     iterations,
                     plan_types,
                     input_format="parquet",
                     use_decimal=True,
                     output_path=None,
                     output_format="parquet",
                     json_summary_folder=None,
                     delta_unmanaged=False,
                     keep_sc=False,
                     hive_external=False,
                     allow_failure=False,
                     profiling_hook=None,
                     save_plan_path=None,
                     skip_execution=False,
                     app_name=None):
    """run SQL in Spark and record execution time log. The execution time log is saved as a CSV file
    for easy accesibility. TempView Creation time is also recorded.

    Args:
        input_prefix (str): path of input data or warehouse if input_format is "iceberg" or hive_external=True.
        query_dict (OrderedDict): ordered dict {query_name: query_content} of all TPC-DS queries runnable in Spark
        time_log_output_path (str): path of the log that contains query execution time, both local
                                    and HDFS path are supported.
        input_format (str, optional): type of input data source.
        use_deciaml(bool, optional): use decimal type for certain columns when loading data of text type.
        output_path (str, optional): path of query output, optinal. If not specified, collect()
                                     action will be applied to each query. Defaults to None.
        output_format (str, optional): query output format, choices are csv, orc, parquet. Defaults
        to "parquet".
    """
    queries_reports = []
    execution_time_list = []
    total_time_start = time.time()
    # check if it's running specific query or Power Run
    if app_name is None:
        if len(query_dict) == 1:
            app_name = "NDS - " + list(query_dict.keys())[0]
        else:
            app_name = "NDS - Power Run"

    # Execute Power Run or Specific query in Spark
    # build Spark Session
    session_builder = SparkSession.builder
    if property_file:
        spark_properties = load_properties(property_file)
        for k,v in spark_properties.items():
            session_builder = session_builder.config(k,v)
    if input_format == 'iceberg':
        session_builder.config("spark.sql.catalog.spark_catalog.warehouse", input_prefix)
    if input_format == 'delta' and not delta_unmanaged:
        session_builder.config("spark.sql.warehouse.dir", input_prefix)
        session_builder.enableHiveSupport()
    if hive_external:
        session_builder.enableHiveSupport()

    spark_session = session_builder.appName(
        app_name).getOrCreate()
    if hive_external:
        spark_session.catalog.setCurrentDatabase(input_prefix)

    if input_format == 'delta' and delta_unmanaged:
        # Register tables for Delta Lake. This is only needed for unmanaged tables.
        execution_time_list = register_delta_tables(spark_session, input_prefix, execution_time_list)
    spark_app_id = spark_session.conf.get("spark.app.id")
    if input_format != 'iceberg' and input_format != 'delta' and not hive_external:
        execution_time_list = setup_tables(spark_session, input_prefix, input_format, use_decimal,
                                           execution_time_list)

    check_json_summary_folder(json_summary_folder)
    if sub_queries:
        query_dict = get_query_subset(query_dict, sub_queries)
    if sub_query_patterns:
        query_dict = get_query_subset_by_pattern(query_dict, sub_query_patterns)

    # Setup profiler
    profiler = Profiler(profiling_hook=profiling_hook, output_root=json_summary_folder)

    # Run query
    power_start = int(time.time())
    setup_time = 0
    cleanup_time = 0
    
    for query_name, q_content in query_dict.items():
        # show query name in Spark web UI
        setQueryName(spark_session, query_name)
        print("====== Run {} ======".format(query_name))
        q_report = PysparkBenchReport(spark_session, query_name)
        
        # Determine query type
        query_type = get_query_type(query_name)
        
        summary = q_report.report_on(run_one_query,warmup_iterations,
                                                   iterations,
                                                   spark_session,
                                                   profiler,
                                                   q_content,
                                                   query_name,
                                                   output_path,
                                                   output_format,
                                                   save_plan_path,
                                                   plan_types,
                                                   skip_execution)
        print(f"Time taken: {summary['queryTimes']} millis for {query_name}")
        query_times = summary['queryTimes']
        for query_time in query_times:
            execution_time_list.append((spark_app_id, query_name, query_time))
            
            # Accumulate setup and cleanup times
            if query_type == 'setup':
                setup_time += query_time
            elif query_type == 'cleanup':
                cleanup_time += query_time
        
        queries_reports.append(q_report)
        if json_summary_folder:
            # Add query type and includeInTotal to the summary
            q_report.summary['queryType'] = query_type
            q_report.summary['includeInTotal'] = (query_type == 'benchmark')
            
            # property_file e.g.: "property/aqe-on.properties" or just "aqe-off.properties"
            if property_file:
                summary_prefix = os.path.join(
                    json_summary_folder, os.path.basename(property_file).split('.')[0])
            else:
                summary_prefix =  os.path.join(json_summary_folder, '')
            q_report.write_summary(prefix=summary_prefix)
    clearQueryName(spark_session)
    power_end = int(time.time())
    power_elapse = int((power_end - power_start)*1000)
    
    # Calculate Power Test Time (excluding setup and cleanup)
    power_test_time = power_elapse - setup_time - cleanup_time
    
    if not keep_sc:
        spark_session.stop()
    total_time_end = time.time()
    total_elapse = int((total_time_end - total_time_start)*1000)
    print("====== Power Test Time: {} milliseconds ======".format(power_test_time))
    if setup_time > 0:
        print("====== Power Setup Time: {} milliseconds ======".format(setup_time))
    if cleanup_time > 0:
        print("====== Power Cleanup Time: {} milliseconds ======".format(cleanup_time))
    print("====== Total Time: {} milliseconds ======".format(total_elapse))
    execution_time_list.append(
        (spark_app_id, "Power Start Time", power_start))
    execution_time_list.append(
        (spark_app_id, "Power End Time", power_end))
    execution_time_list.append(
        (spark_app_id, "Power Test Time", power_test_time))
    if setup_time > 0:
        execution_time_list.append(
            (spark_app_id, "Power Setup Time", setup_time))
    if cleanup_time > 0:
        execution_time_list.append(
            (spark_app_id, "Power Cleanup Time", cleanup_time))
    execution_time_list.append(
        (spark_app_id, "Total Time", total_elapse))

    header = ["application_id", "query", "time/milliseconds"]
    # print to driver stdout for quick view
    print(header)
    for row in execution_time_list:
        print(row)
    # write to local file at driver node
    with open(time_log_output_path, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(execution_time_list)
    # write to csv in cloud environment
    if extra_time_log_output_path:
        spark_session = SparkSession.builder.getOrCreate()
        time_df = spark_session.createDataFrame(data=execution_time_list, schema = header)
        time_df.coalesce(1).write.csv(extra_time_log_output_path)

    # check queries_reports, if there's any task or query failed, exit a non-zero to represent the script failure
    exit_code = 0
    for q in queries_reports:
        if not q.is_success():
            if exit_code == 0:
                print("====== Queries with failure ======")
            print("{} status: {}".format(q.summary['query'], q.summary['queryStatus']))
            exit_code = 1
    if exit_code:
        print("Above queries failed or completed with failed tasks. Please check the logs for the detailed reason.")

    if not allow_failure and exit_code:
        sys.exit(exit_code)

def load_properties(filename):
    myvars = {}
    with open(filename) as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.strip()
    return myvars

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # argument group for query filtering
    query_filter_group = parser.add_mutually_exclusive_group(required=False)
    parser.add_argument('input_prefix',
                        help='text to prepend to every input file path (e.g., "hdfs:///ds-generated-data"). ' +
                        'If --hive or if input_format is "iceberg", this argument will be regarded as the value of property ' +
                        '"spark.sql.catalog.spark_catalog.warehouse". Only default Spark catalog ' +
                        'session name "spark_catalog" is supported now, customized catalog is not ' +
                        'yet supported. Note if this points to a Delta Lake table, the path must be ' +
                        'absolute. Issue: https://github.com/delta-io/delta/issues/555')
    parser.add_argument('query_stream_file',
                        help='query stream file that contains NDS queries in specific order')
    parser.add_argument('time_log',
                        help='path to execution time log, only support local path.',
                        default="")
    parser.add_argument('--input_format',
                        help='type for input data source, e.g. parquet, orc, json, csv or iceberg, delta. ' +
                        'Certain types are not fully supported by GPU reading, please refer to ' +
                        'https://github.com/NVIDIA/spark-rapids/blob/branch-22.08/docs/compatibility.md ' +
                        'for more details.',
                        choices=['parquet', 'orc', 'avro', 'csv', 'json', 'iceberg', 'delta'],
                        default='parquet')
    parser.add_argument('--output_prefix',
                        help='text to prepend to every output file (e.g., "hdfs:///ds-parquet")')
    parser.add_argument('--output_format',
                        help='type of query output',
                        default='parquet')
    parser.add_argument('--property_file',
                        help='property file for Spark configuration.')
    parser.add_argument('--floats',
                        action='store_true',
                        help='When loading Text files like json and csv, schemas are required to ' +
                        'determine if certain parts of the data are read as decimal type or not. '+
                        'If specified, float data will be used.')
    parser.add_argument('--json_summary_folder',
                        help='Empty folder/path (will create if not exist) to save JSON summary file for each query.')
    parser.add_argument('--delta_unmanaged',
                        action='store_true',
                        help='Use unmanaged tables for DeltaLake. This is useful for testing DeltaLake without ' +
        '               leveraging a Metastore service.')
    parser.add_argument('--keep_sc',
                        action='store_true',
                        help='Keep SparkContext alive after running all queries. This is a ' +
                        'limitation on Databricks runtime environment. User should always attach ' +
                        'this flag when running on Databricks.')
    parser.add_argument('--hive',
                        action='store_true',
                        help='use table meta information in Hive metastore directly without ' +
                        'registering temp views.')
    parser.add_argument('--extra_time_log',
                        help='extra path to save time log when running in cloud environment where '+
                        'driver node/pod cannot be accessed easily. User needs to add essential extra ' +
                        'jars and configurations to access different cloud storage systems. ' +
                        'e.g. s3, gs etc.')
    parser.add_argument('--allow_failure',
                        action='store_true',
                        help='Do not exit with non zero when any query failed or any task failed')
    parser.add_argument('--profiling_hook',
                        help='Executable that is called just before/after a query executes.' +
                        'The executable is called like this ' +
                        './hook {start|stop} output_root query_name.')
    parser.add_argument('--warmup_iterations',
                        type=int,
                        help='Number of warmup iterations for each query.',
                        default=0)
    parser.add_argument('--iterations',
                        type=int,
                        help='Number of iterations for each query.',
                        default=1)
    parser.add_argument('--save_plan_path',
                        help='Save the execution plan of each query to the specified file. If --skip_execution is ' +
                        'specified, the execution plan will be saved without executing the query.')
    parser.add_argument('--plan_types',
                        type=lambda s: [x.strip() for x in s.split(',')],
                        help='Comma separated list of plan types to save. ' +
                        'e.g. "physical, logical". Default is "logical".',
                        default='logical')
    parser.add_argument('--skip_execution',
                        action='store_true',
                        help='Skip the execution of the queries. This can be used in conjunction with ' +
                        '--save_plan_path to only save the execution plans without running the queries.' +
                        'Note that "spark.sql.adaptive.enabled" should be set to false to get GPU physical plans.')
    parser.add_argument('--app_name',
                        help='The name of the application. If not specified, the default name will be "NDS - Power Run", '
                             'or "NDS - <query_name>" when running a single query.',
                        default=None)
    query_filter_group.add_argument('--sub_queries',
                                    type=lambda s: [x.strip() for x in s.split(',')],
                                    help='comma separated list of queries to run. If this is specified, sub_query_patterns should be empty. ' +
                                    'If both sub_queries and sub_query_patterns are not specified, all queries ' +
                                    'in the stream file will be run. Note, use "_part1" and "_part2" suffix for the following query names: ' +
                                    'query14, query23, query24, and query39. Ex) "query1,query2,query14_part1,query39_part2"')
    query_filter_group.add_argument('--sub_query_patterns',
                                    type=lambda s: [x.strip() for x in s.split(',')],
                                    help='comma separated list of query patterns to run in regex. If this is specified, sub_queries should be empty. ' +
                                    'If both sub_queries and sub_query_patterns are not specified, all queries ' +
                                    'in the stream file will be run. ' +
                                    'For example, query1 will run all queries starting with "query1", ' +
                                    'and "^query1$,query(2|3)_part1" will run query1, query2_part1, and query3_part1.')
    args = parser.parse_args()
    query_dict = gen_sql_from_stream(args.query_stream_file)
    run_query_stream(args.input_prefix,
                     args.property_file,
                     query_dict,
                     args.time_log,
                     args.extra_time_log,
                     args.sub_queries,
                     args.sub_query_patterns,
                     args.warmup_iterations,
                     args.iterations,
                     args.plan_types,
                     args.input_format,
                     not args.floats,
                     args.output_prefix,
                     args.output_format,
                     args.json_summary_folder,
                     args.delta_unmanaged,
                     args.keep_sc,
                     args.hive,
                     args.allow_failure,
                     args.profiling_hook,
                     args.save_plan_path,
                     args.skip_execution,
                     args.app_name)
