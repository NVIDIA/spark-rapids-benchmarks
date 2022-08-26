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
import csv
from datetime import datetime
import os
import time

from pyspark.sql import SparkSession
from PysparkBenchReport import PysparkBenchReport

from check import check_json_summary_folder, get_abs_path
from nds_schema import get_maintenance_schemas

INSERT_FUNCS = [
    'LF_CR',
    'LF_CS',
    'LF_I',
    'LF_SR',
    'LF_SS',
    'LF_WR',
    'LF_WS']
DELETE_FUNCS = [
    'DF_CS',
    'DF_SS',
    'DF_WS']
INVENTORY_DELETE_FUNC = ['DF_I']
DM_FUNCS = INSERT_FUNCS + DELETE_FUNCS + INVENTORY_DELETE_FUNC

def get_delete_date(spark_session):
    """get delete dates for Data Maintenance. Each delete functions requires 3 tuples: (date1, date2)

    Args:
        spark_session (SparkSession): Spark session
    Returns:
        delete_dates_dict ({str: list[(date1, date2)]}): a dict contains date tuples for each delete functions
    """
    delete_dates = spark_session.sql("select * from delete").collect()
    inventory_delete_dates = spark_session.sql("select * from inventory_delete").collect()
    date_dict = {}
    date_dict['delete'] = [(row['date1'], row['date2']) for row in delete_dates]
    date_dict['inventory_delete'] = [(row['date1'], row['date2']) for row in inventory_delete_dates]
    return date_dict

def replace_date(query_list, date_tuple_list):
    """Replace the date keywords in DELETE queries. 3 date tuples will be applied to the delete query.

    Args:
        query_list ([str]): delete query list
        date_tuple_list ([(str, str)]): actual delete date
    """
    q_updated = []
    for date_tuple in date_tuple_list:
        for c in query_list:
            c = c.replace("DATE1", date_tuple[0])
            c = c.replace("DATE2", date_tuple[1])
            q_updated.append(c)
    return q_updated

def get_valid_query_names(spec_queries):
    global DM_FUNCS
    if spec_queries:
        for q in spec_queries:
            if q not in DM_FUNCS:
                raise Exception(f"invalid Data Maintenance query: {q}. Valid  are: {DM_FUNCS}")
        DM_FUNCS = spec_queries
    return DM_FUNCS

def create_spark_session(valid_queries):
    if len(valid_queries) == 1:
        app_name = "NDS - Data Maintenance - " + valid_queries[0]
    else:
        app_name = "NDS - Data Maintenance"
    spark_session = SparkSession.builder.appName(
        app_name).getOrCreate()
    return spark_session

def get_maintenance_queries(spark_session, folder, valid_queries):
    """get query content from DM query files

    Args:
        folder (str): folder to Data Maintenance query files
        spec_queries (list[str]): specific target Data Maintenance queries
    Returns:
        dict{str: list[str]}: a dict contains Data Maintenance query name and its content.
    """
    delete_date_dict = get_delete_date(spark_session)
    folder_abs_path = get_abs_path(folder)
    q_dict = {}
    for q in valid_queries:
        with open(folder_abs_path + '/' + q + '.sql', 'r') as f:
            # file content e.g.
            # " LICENSE CONTENT ... ;"
            # " CREATE view ..... ; INSERT into .... ;"
            # " DELETE from ..... ; DELETE FROM .... ;"
            q_content = [ c + ';' for c in f.read().split(';')[1:-1]]
            if q in DELETE_FUNCS:
                # There're 3 date tuples to be replace for one DELETE function
                # according to TPC-DS Spec 5.3.11
                q_content = replace_date(q_content, delete_date_dict['delete'])
            if q in INVENTORY_DELETE_FUNC:
                q_content = replace_date(q_content, delete_date_dict['inventory_delete'])
            q_dict[q] = q_content
    return q_dict

def run_dm_query(spark, query_list):
    """Run data maintenance query.
    For delete queries, they can run on Spark 3.2.2 but not Spark 3.2.1
    See: https://issues.apache.org/jira/browse/SPARK-39454
    See: data_maintenance/DF_*.sql for insert query details.
    See data_maintenance/LF_*.sql for delete query details.

    Args:
        spark (SparkSession):  SparkSession instance.
        query_list ([str]): INSERT query list.
    """
    for q in query_list:
        spark.sql(q)

def run_query(spark_session, query_dict, time_log_output_path, json_summary_folder, property_file):
    # TODO: Duplicate code in nds_power.py. Refactor this part, make it general.
    execution_time_list = []
    check_json_summary_folder(json_summary_folder)
    # Run query
    total_time_start = datetime.now()
    spark_app_id = spark_session.sparkContext.applicationId
    DM_start = datetime.now()
    for query_name, q_content in query_dict.items():
        # show query name in Spark web UI
        spark_session.sparkContext.setJobGroup(query_name, query_name)
        print(f"====== Run {query_name} ======")
        q_report = PysparkBenchReport(spark_session)
        summary = q_report.report_on(run_dm_query, spark_session,
                                                       q_content)
        print(f"Time taken: {summary['queryTimes']} millis for {query_name}")
        execution_time_list.append((spark_app_id, query_name, summary['queryTimes']))
        if json_summary_folder:
            # property_file e.g.: "property/aqe-on.properties" or just "aqe-off.properties"
            if property_file:
                summary_prefix = os.path.join(
                    json_summary_folder, os.path.basename(property_file).split('.')[0])
            else:
                summary_prefix =  os.path.join(json_summary_folder, '')
            q_report.write_summary(query_name, prefix=summary_prefix)
    spark_session.sparkContext.stop()
    DM_end = datetime.now()
    DM_elapse = (DM_end - DM_start).total_seconds()
    total_elapse = (DM_end - total_time_start).total_seconds()
    print(f"====== Data Maintenance Start Time: {DM_start}")
    print(f"====== Data Maintenance Time: {DM_elapse} s ======")
    print(f"====== Total Time: {total_elapse} s ======")
    execution_time_list.append(
        (spark_app_id, "Data Maintenance Start Time", DM_start)
    )
    execution_time_list.append(
        (spark_app_id, "Data Maintenance End Time", DM_end)
    )
    execution_time_list.append(
        (spark_app_id, "Data Maintenance Time", DM_elapse))
    execution_time_list.append(
        (spark_app_id, "Total Time", total_elapse))

    # write to local csv file
    header = ["application_id", "query", "time/s"]
    with open(time_log_output_path, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(execution_time_list)
    
def register_temp_views(spark_session, refresh_data_path):
    refresh_tables = get_maintenance_schemas(True)
    for table, schema in refresh_tables.items():
        spark_session.read.option("delimiter", '|').option(
            "header", "false").csv(refresh_data_path + '/' + table, schema=schema).createOrReplaceTempView(table)

if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('refresh_data_path',
                        help='path to refresh data')
    parser.add_argument('maintenance_queries_folder',
                        help='folder contains all NDS Data Maintenance queries. If ' +
                        '"--maintenance_queries" is not set, all queries under the folder will be' +
                        'executed.')
    parser.add_argument('time_log',
                        help='path to execution time log, only support local path.',
                        default="")
    parser.add_argument('--maintenance_queries',
                        type=lambda s: s.split(','),
                        help='specify Data Maintenance query names by a comma seprated string.' +
                        ' e.g. "LF_CR,LF_CS"')
    parser.add_argument('--property_file',
                        help='property file for Spark configuration.')
    parser.add_argument('--json_summary_folder',
                        help='Empty folder/path (will create if not exist) to save JSON summary file for each query.')

    args = parser.parse_args()
    valid_queries = get_valid_query_names(args.maintenance_queries)
    spark_session = create_spark_session(valid_queries)
    register_temp_views(spark_session, args.refresh_data_path)
    query_dict = get_maintenance_queries(spark_session,
                                         args.maintenance_queries_folder,
                                         valid_queries)
    run_query(spark_session, query_dict, args.time_log, args.json_summary_folder, args.property_file)
