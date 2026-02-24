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
import os
import sys
import subprocess
import shutil

#For adding utils to path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
utils_dir = os.path.join(parent_dir, 'utils')
sys.path.insert(0, utils_dir)

from check import check_build_nds_h, check_version, get_abs_path, get_dir_size, parallel_value_type, valid_range

check_version()

# Source tables contained in the schema for TPC-H. For more information, check -
# https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf

source_table_names = [
    'customer',
    'lineitem',
    'nation',
    'orders',
    'part',
    'partsupp',
    'region',
    'supplier'
]


def generate_data_local(args, range_start, range_end, tool_path):
    """Generate data to local file system. TPC-DS tool will generate all table data under target
    folder without creating sub-folders for each table. So we add extra code to create sub folder
    for each table and move data there respectively.

    Args:
        args (Namepace): Namespace from argparser
        tool_path (str): path to the dsdgen tool

    Raises:
        Exception: if data already exists and overwrite_output is not honored
        Exception: dsdgen failed
    """
    data_dir = get_abs_path(args.data_dir)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
    else:
        # Verify if there's already data in this path
        if get_dir_size(data_dir) > 0 and not args.overwrite_output:
            raise Exception(
                "There's already been data exists in directory {}.".format(data_dir) +
                " Use '--overwrite_output' to overwrite.")

    # working directory for dsdgen
    work_dir = tool_path.parent
    print(work_dir)
    procs = []
    for i in range(range_start, range_end + 1):
        dbgen = ["-s", args.scale,
                 "-C", args.parallel,
                 "-S", str(i),
                 "-v", "Y",
                 "-f", "Y"]
        procs.append(subprocess.Popen(
            ["./dbgen"] + dbgen, cwd=str(work_dir)))
    # wait for data generation to complete
    for p in procs:
        p.wait()
        if p.returncode != 0:
            print("dbgen failed with return code {}".format(p.returncode))
            raise Exception("dbgen failed")
    # move multi-partition files into table folders
    table_names = source_table_names
    for table in table_names:
        print('mkdir -p {}/{}'.format(data_dir, table))
        subprocess.run(['mkdir', '-p', data_dir + '/' + table])
        if (table != 'region' and table != 'nation'):
            for i in range(range_start, range_end + 1):
                subprocess.run(['mv', f'{work_dir}/{table}.tbl.{i}',
                                f'{data_dir}/{table}/'], stderr=subprocess.DEVNULL)
        else:
            subprocess.run(['mv', f'{work_dir}/{table}.tbl',
                            f'{data_dir}/{table}/'], stderr=subprocess.DEVNULL)
        # delete date file has no parallel number suffix in the file name, move separately
    # show summary
    subprocess.run(['du', '-h', '-d1', data_dir])


def clean_temp_data(temp_data_path):
    cmd = ['hadoop', 'fs', '-rm', '-r', '-skipTrash', temp_data_path]
    print(" ".join(cmd))
    subprocess.run(cmd)


def merge_temp_tables(temp_data_path, parent_data_path):
    """Helper functions for incremental data generation. Move data in temporary child range path to
    parent directory.

    Args:
        temp_data_path (str): temorary child range data path
        parent_data_path (str): parent data path
    """
    table_names = source_table_names
    for table_name in table_names:
        # manually create table sub-folders
        # redundant step if it's not the first range part.
        cmd = ['hadoop', 'fs', '-mkdir', parent_data_path + '/' + table_name]
        print(" ".join(cmd))
        subprocess.run(cmd)
        # move temp content to upper folder
        # note not all tables are generated in different child range step
        # please ignore messages like "mv: `.../reason/*': No such file or directory"
        temp_table_data_path = temp_data_path + '/' + table_name + '/*'
        cmd = ['hadoop', 'fs', '-mv', temp_table_data_path,
               parent_data_path + '/' + table_name + '/']
        print(" ".join(cmd))
        subprocess.run(cmd)
    clean_temp_data(temp_data_path)


def generate_data_hdfs(args, jar_path):
    """generate data to hdfs using TPC-DS dsdgen tool. Support incremental generation: due to the
    limit of hdfs, each range data will be generated under a temporary folder then move to target
    folder.

    Args:
        args (Namespace): Namespace from argparser
        jar_path (str): path to the target jar

    Raises:
        Exception: if Hadoop binary is not installed.
    """
    # Check if hadoop is installed.
    if shutil.which('hadoop') is None:
        raise Exception('No Hadoop binary found in current environment, ' +
                        'please install Hadoop for data generation in cluster.')
    # Submit hadoop MR job to generate data
    cmd = ['hadoop', 'jar', str(jar_path)]
    cmd += ['-p', args.parallel, '-s', args.scale]
    # get dsdgen.jar path, assume user won't change file structure
    tpcds_gen_path = jar_path.parent.parent.absolute()
    if args.overwrite_output:
        cmd += ['-o']
    if args.range:
        # use a temp folder to save the specific range data.
        # will move the content to parent folder afterwards.
        # it's a workaround for "Output directory ... already exists" in incremental generation
        temp_data_path = args.data_dir + '/_temp_'
        # before generation, we remove "_temp_" folders in case they contain garbage generated by
        # previous user runs.
        clean_temp_data(temp_data_path)
        cmd.extend(["-r", args.range])
        cmd.extend(["-d", temp_data_path])
        try:
            subprocess.run(cmd, check=True, cwd=str(tpcds_gen_path))
            # only move delete table for data maintenance
            merge_temp_tables(temp_data_path, args.data_dir)
        finally:
            clean_temp_data(temp_data_path)
    else:
        cmd.extend(["-d", args.data_dir])
        subprocess.run(cmd, check=True, cwd=str(tpcds_gen_path))
        # only move delete table for data maintenance


def generate_data(args):
    jar_path, tool_path = check_build_nds_h()
    range_start = 1
    range_end = int(args.parallel)
    if args.range:
        range_start, range_end = valid_range(args.range, args.parallel)
    if args.type == 'hdfs':
        generate_data_hdfs(args, jar_path)
    else:
        generate_data_local(args, range_start, range_end, tool_path)


if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument("type",
                        choices=["local", "hdfs"],
                        help="file system to save the generated data.")
    parser.add_argument("scale",
                        help="volume of data to generate in GB. Accepted SF - 1,10, 100, 300, 1000 \
                            ,3000, 10000, 30000,"
                        )
    parser.add_argument("parallel",
                        type=parallel_value_type,
                        help="build data in <parallel_value> separate chunks"
                        )
    parser.add_argument("data_dir",
                        help="generate data in directory.")
    parser.add_argument('--range',
                        help='Used for incremental data generation, meaning which part of child' +
                             'chunks are generated in one run. Format: "start,end", both are inclusive. ' +
                             'e.g. "1,100". Note: the child range must be within the "parallel", ' +
                             '"--parallel 100 --range 100,200" is illegal.')
    parser.add_argument("--overwrite_output",
                        action="store_true",
                        help="overwrite if there has already existing data in the path provided.")

    args = parser.parse_args()
    generate_data(args)
