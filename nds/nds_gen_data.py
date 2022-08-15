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
import os
import shutil
import subprocess

from check import check_build, check_version, get_abs_path, get_dir_size, parallel_value_type, valid_range

check_version()

source_table_names = [
    'call_center',
    'catalog_page',
    'catalog_returns',
    'catalog_sales',
    'customer',
    'customer_address',
    'customer_demographics',
    'date_dim',
    'dbgen_version',
    'household_demographics',
    'income_band',
    'inventory',
    'item',
    'promotion',
    'reason',
    'ship_mode',
    'store',
    'store_returns',
    'store_sales',
    'time_dim',
    'warehouse',
    'web_page',
    'web_returns',
    'web_sales',
    'web_site',
]

maintenance_table_names = [
    's_catalog_order',
    's_catalog_order_lineitem',
    's_catalog_returns',
    's_inventory',
    's_purchase',
    's_purchase_lineitem',
    's_store_returns',
    's_web_order',
    's_web_order_lineitem',
    's_web_returns',
    'delete',
    'inventory_delete'
]

def clean_temp_data(temp_data_path):
    cmd = ['hadoop', 'fs', '-rm', '-r', '-skipTrash', temp_data_path]
    print(" ".join(cmd))
    subprocess.run(cmd)


def merge_temp_tables(temp_data_path, parent_data_path, update):
    """Helper functions for incremental data generation. Move data in temporary child range path to
    parent directory.

    Args:
        temp_data_path (str): temorary child range data path
        parent_data_path (str): parent data path
    """
    if update:
        table_names = maintenance_table_names
    else:
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

def move_delete_date_tables(base_path, update):
    # delete date table are special, move them separately
    # with --update 2, it'll generate the files named like delete_2.dat-m-00000, delete_2.dat-m-00001...
    # the number of files is decided by the parallel value, and they all have same content
    # So we just copy the first one
    for delete_table in ['delete', 'inventory_delete']:
        mkdir = ['hadoop', 'fs', '-mkdir', '-p', base_path + '/' + delete_table]
        move = ['hadoop', 'fs', '-mv', base_path  + '/' + delete_table + f'_{update}.dat-m-00000', base_path + '/' + delete_table + '/']
        subprocess.run(mkdir, check=True)
        subprocess.run(move, check=True)

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
    cmd =  ['hadoop', 'jar', str(jar_path)]
    if args.replication:
        cmd += ["-D", f"dfs.replication={args.replication}"]
    cmd += ['-p', args.parallel, '-s', args.scale]
    # get dsdgen.jar path, assume user won't change file structure
    tpcds_gen_path = jar_path.parent.parent.absolute()
    if args.overwrite_output:
        cmd += ['-o']
    if args.update:
        cmd += ["-u", args.update]
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
            if args.update:
                move_delete_date_tables(temp_data_path, args.update)
            merge_temp_tables(temp_data_path, args.data_dir, args.update)
        finally:
            clean_temp_data(temp_data_path)
    else:
        cmd.extend(["-d", args.data_dir])
        subprocess.run(cmd, check=True, cwd=str(tpcds_gen_path))
        # only move delete table for data maintenance
        if args.update:
            move_delete_date_tables(args.data_dir, args.update)


def generate_data_local(args, range_start, range_end, tool_path):
    """Generate data to local file system. TPC-DS tool will generate all table data under target
    folder without creating sub-folders for each table. So we add extra code to create sub folder
    for each table and move data there respectively.

    Args:
        args (Namepace): Namespace from argparser
        range_start (int): start index of the data portion to be generated
        range_end (int): end index of the data portion tobe generated
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
    procs = []
    for i in range(range_start, range_end + 1):
        dsdgen_args = ["-scale", args.scale,
                       "-dir", data_dir,
                       "-parallel", args.parallel,
                       "-child", str(i),
                       "-verbose", "Y"]
        if args.overwrite_output:
            dsdgen_args += ["-force", "Y"]
        if args.update:
            dsdgen_args += ["-update", args.update]
        procs.append(subprocess.Popen(
            ["./dsdgen"] + dsdgen_args, cwd=str(work_dir)))
    # wait for data generation to complete
    for p in procs:
        p.wait()
        if p.returncode != 0:
            print("dsdgen failed with return code {}".format(p.returncode))
            raise Exception("dsdgen failed")
    # move multi-partition files into table folders
    if args.update:
        table_names = maintenance_table_names
    else:
        table_names = source_table_names
    for table in table_names:
        print('mkdir -p {}/{}'.format(data_dir, table))
        subprocess.run(['mkdir', '-p', data_dir + '/' + table])
        for i in range(range_start, range_end + 1):
            subprocess.run(['mv', f'{data_dir}/{table}_{i}_{args.parallel}.dat',
                            f'{data_dir}/{table}/'], stderr=subprocess.DEVNULL)
        # delete date file has no parallel number suffix in the file name, move separately
        subprocess.run(['mv', f'{data_dir}/{table}_1.dat',
                        f'{data_dir}/{table}/'], stderr=subprocess.DEVNULL)
    # show summary
    subprocess.run(['du', '-h', '-d1', data_dir])


def generate_data(args):
    jar_path, tool_path = check_build()
    range_start = 1
    range_end = int(args.parallel)
    if args.range:
        range_start, range_end = valid_range(args.range, args.parallel)
    if args.type == 'hdfs':
        generate_data_hdfs(args, jar_path)
    if args.type == 'local':
        generate_data_local(args, range_start, range_end, tool_path)


if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument("type",
                        choices=["local", "hdfs"],
                        help="file system to save the generated data.")
    parser.add_argument("scale",
                        help="volume of data to generate in GB."
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
    parser.add_argument("--replication",
                        help="the number of replication factor when generating data to HDFS. " +
                        "if not set, the Hadoop job will use the setting in the Hadoop cluster.")
    parser.add_argument("--update",
                        help="generate update dataset <n>. <n> is identical to the number of " +
                        "streams used in the Throughput Tests of the benchmark")


    args = parser.parse_args()
    generate_data(args)
