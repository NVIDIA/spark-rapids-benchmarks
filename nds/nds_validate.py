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
import glob
import json
import math
import os
import time
from decimal import *

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

from nds_power import gen_sql_from_stream

def compare_results(spark_session: SparkSession,
                    input1: str,
                    input2: str,
                    input_format: str,
                    ignore_ordering: bool,
                    is_q78: bool,
                    use_iterator=False,
                    max_errors=10,
                    epsilon=0.00001) -> bool :
    """Giving 2 paths of input query output data, compare them row by row, value by value to see if
    the results match or not.

    Args:
        spark_session (SparkSession): Spark Session to hold the comparison
        input1 (str): path for the first input data
        input2 (str): path for the second input data
        input_format (str): data source format, e.g. parquet, orc
        ignore_ordering (bool): whether ignoring the order of input data.
            If true, we will order by ourselves.
        is_q78 (bool): whether the query is query78.
        use_iterator (bool, optional): When set to true, use `toLocalIterator` to load one partition
            at a time into driver memory, reducing memory usage at the cost of performance because
            processing will be single-threaded. Defaults to False.
        max_errors (int, optional): Maximum number of differences to report. Defaults to 10.
        epsilon (float, optional): Allow for differences in precision when comparing floating point
            values. Defaults to 0.00001.

    Returns:
        bool: True if result matches otherwise False
    """
    df1 = spark_session.read.format(input_format).load(input1)
    df2 = spark_session.read.format(input_format).load(input2)
    count1 = df1.count()
    count2 = df2.count()

    if(count1 == count2):
        #TODO: need partitioned collect for NDS? there's no partitioned output currently
        result1 = collect_results(df1, ignore_ordering, use_iterator)
        result2 = collect_results(df2, ignore_ordering, use_iterator)

        errors = 0
        i = 0
        while i < count1 and errors < max_errors:
            lhs = next(result1)
            rhs = next(result2)
            if not rowEqual(list(lhs), list(rhs), epsilon, is_q78):
                print(f"Row {i}: \n{list(lhs)}\n{list(rhs)}\n")
                errors += 1
            i += 1
        print(f"Processed {i} rows")
        
        if errors == max_errors:
            print(f"Aborting comparison after reaching maximum of {max_errors} errors")
            return False
        elif errors == 0:
            print("Results match")
            return True
        else:
            print(f"There were {errors} errors")
            return False
    else:
        print(f"DataFrame row counts do not match: {count1} != {count2}")
        return False

def collect_results(df: DataFrame,
                   ignore_ordering: bool,
                   use_iterator: bool):
    # apply sorting if specified
    non_float_cols = [col(field.name) for \
        field in df.schema.fields \
            if (field.dataType.typeName() != FloatType.typeName()) \
                and \
                (field.dataType.typeName() != DoubleType.typeName())]
    float_cols = [col(field.name) for \
        field in df.schema.fields \
            if (field.dataType.typeName() == FloatType.typeName()) \
                or \
                (field.dataType.typeName() == DoubleType.typeName())]
    if ignore_ordering:
        df = df.sort(non_float_cols + float_cols)

    # TODO: do we still need this for NDS? Query outputs are usually 1 - 100 rows,
    #       there should'nt be memory pressure.
    if use_iterator:
        it = df.toLocalIterator()
    else:
        print("Collecting rows from DataFrame")
        t1 = time.time()
        rows = df.collect()
        t2 = time.time()
        print(f"Collected {len(rows)} rows in {t2-t1} seconds")
        it = iter(rows)
    return it

def rowEqual(row1, row2, epsilon, is_q78):
    # only simple types in a row for NDS results
    if is_q78:
        # TODO: make the special compare for q78 more common and make it apply to other queries that contain round function
        # TODO: remove this special case after we resolve https://github.com/NVIDIA/spark-rapids/issues/1573
        # see example error case: https://github.com/NVIDIA/spark-rapids-benchmarks/pull/7#issue-1247422850
        # Pop the 4th column value in q78, compare it alone.
        fourth_val_row1 = row1.pop(3)
        fourth_val_row2 = row2.pop(3)
        fourth_val_eq = False
        # this value could be none in some rows
        if all([fourth_val_row1, fourth_val_row2]):
            # this value is rounded to its pencentile: round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2)
            # so we allow the diff <= 0.01
            fourth_val_eq = abs(fourth_val_row1 - fourth_val_row2) <= 0.01
        elif fourth_val_row1 == None and fourth_val_row2 == None:
            fourth_val_eq = True
        else:
            fourth_val_eq = False
        return fourth_val_eq and all([compare(lhs, rhs, epsilon) for lhs, rhs in zip(row1, row2)])
    else:
        return all([compare(lhs, rhs, epsilon) for lhs, rhs in zip(row1, row2)])

def compare(expected, actual, epsilon=0.00001):
    #TODO 1: we can optimize this with case-match after Python 3.10
    #TODO 2: we can support complex data types like nested type if needed in the future.
    #        now NDS only contains simple data types.
    if isinstance(expected, float) and isinstance(actual, float):
        # Double is converted to float in pyspark...
        if math.isnan(expected) and math.isnan(actual):
            return True
        else:
            return math.isclose(expected, actual, rel_tol=epsilon)
    elif isinstance(expected, str) and isinstance(actual, str):
        return expected == actual
    elif expected == None and actual == None:
        return True
    elif expected != None and actual == None:
        return False
    elif expected == None and actual != None:
        return False
    elif isinstance(expected, Decimal) and isinstance(actual, Decimal):
        return math.isclose(expected, actual, rel_tol=epsilon)
    else:
        return expected == actual

def iterate_queries(spark_session: SparkSession,
                    input1: str,
                    input2: str,
                    input_format: str,
                    ignore_ordering: bool,
                    queries: list,
                    use_iterator=False,
                    max_errors=10,
                    epsilon=0.00001,
                    is_float=False):
    # Iterate each query folder for a Power Run output
    # Providing a list instead of hard-coding all NDS queires is to satisfy the arbitary queries run.
    unmatch_queries = []
    for query in queries:
        if query == 'query65':
            # query65 is skipped due to: https://github.com/NVIDIA/spark-rapids-benchmarks/pull/7#issuecomment-1147077894
            continue
        if query == 'query67' and is_float:
            # query67 is skipped due to: https://github.com/NVIDIA/spark-rapids-benchmarks/pull/7#issuecomment-1156214630
            continue
        sub_input1 = input1 + '/' + query
        sub_input2 = input2 + '/' + query
        print(f"=== Comparing Query: {query} ===")
        result_equal = compare_results(spark_session,
                                         sub_input1,
                                         sub_input2,
                                         input_format,
                                         ignore_ordering,
                                         query == 'query78',
                                         use_iterator=use_iterator,
                                         max_errors=max_errors,
                                         epsilon=epsilon)
        if result_equal == False:
            unmatch_queries.append(query)
    if len(unmatch_queries) != 0:
        print(f"=== Unmatch Queries: {unmatch_queries} ===")
    return unmatch_queries

def update_summary(prefix, unmatch_queries):
    """set the queryValidationStatus field in json summary file.
    If the queryStatus is 'Completed' or 'CompletedWithTaskFailures' but validation failed,
    set to 'Fail'.
    If the queryStatus is 'Completed' or 'CompletedWithTaskFailures' and validation passed,
    set to 'Pass'.
    If the queryStatus is 'Failed',
    set to 'NotAttempted'.

    Args:
        prefix (str): folder of the json summary files
        unmatch_queries ([str]): list of queries that failed validation
    """
    if not os.path.exists(args.json_summary_folder):
        raise Exception("The json summary folder doesn't exist.")
    print("Updating queryValidationStatus.")
    for query_name in query_dict.keys():
        summary_wildcard = prefix + f'/*{query_name}*.json'
        file_glob = glob.glob(summary_wildcard)
        if len(file_glob) > 1:
            raise Exception(f"More than one summary file found for query {query_name}")
        for filename in file_glob:
            with open(filename, 'r') as f:
                summary = json.load(f)
                if query_name in unmatch_queries:
                    if 'Completed' in summary['queryStatus'] or 'CompletedWithTaskFailures' in summary['queryStatus']:
                        summary['queryValidationStatus'] = ['Fail']
                    else:
                        summary['queryValidationStatus'] = ['NotAttempted']
                else:
                    summary['queryValidationStatus'] = ['Pass']
            with open(filename, 'w') as f:
                json.dump(summary, f, indent=2)

if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('input1',
                        help='path of the first input data.')
    parser.add_argument('input2',
                        help='path of the second input data.')
    parser.add_argument('query_stream_file',
                        help='query stream file that contains NDS queries in specific order.')
    parser.add_argument('--input_format',
                        default='parquet',
                        help='data source type. e.g. parquet, orc. Default is: parquet.')
    parser.add_argument('--max_errors',
                        help='Maximum number of differences to report.',
                        type=int,
                        default=10)
    parser.add_argument('--epsilon',
                        type=float,
                        default=0.00001,
                        help='Allow for differences in precision when comparing floating point values.' +
                        ' Given 2 float numbers: 0.000001 and 0.000000, the diff of them is 0.000001' +
                        ' which is less than 0.00001, so we regard this as acceptable and will not' +
                        ' report a mismatch.')
    parser.add_argument('--ignore_ordering',
                        action='store_true',
                        help='Sort the data collected from the DataFrames before comparing them.')
    parser.add_argument('--use_iterator',
                        action='store_true',
                        help='When set, use `toLocalIterator` to load one partition at a' +
                        ' time into driver memory, reducing memory usage at the cost of performance' +
                        ' because processing will be single-threaded.')
    parser.add_argument('--floats',
                        action='store_true',
                        help='whether the input data contains float data or decimal data. There\'re' +
                        ' some known mismatch issues due to float point, we will do some special' +
                        ' checks when the input data is float for some queries.')
    parser.add_argument('--json_summary_folder',
                        required=True,
                        help='path of a folder that contains json summary file for each query.')
    args = parser.parse_args()
    query_dict = gen_sql_from_stream(args.query_stream_file)
    session_builder = SparkSession.builder.appName("Validate Query Output").getOrCreate()
    unmatch_queries = iterate_queries(session_builder,
                                      args.input1,
                                      args.input2,
                                      args.input_format,
                                      args.ignore_ordering,
                                      query_dict.keys(),
                                      use_iterator=args.use_iterator,
                                      max_errors=args.max_errors,
                                      epsilon=args.epsilon,
                                      is_float=args.floats)
    update_summary(args.json_summary_folder, unmatch_queries)
