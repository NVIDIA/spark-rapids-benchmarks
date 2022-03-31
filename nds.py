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
import subprocess
import shutil
import sys
import os
import csv
from multiprocessing import Process

def check_version():
    req_ver = (3,6)
    cur_ver = sys.version_info
    if cur_ver < req_ver:
        raise Exception('Minimum required Python version is 3.6, but current python version is {}.'
                        .format(str(cur_ver.major) + '.' + str(cur_ver.minor)) +
                        ' Please use proper Python version')

def check_build():
    # Check if necessary executable or jars are built.
    if not (os.path.exists('tpcds-gen/target/tpcds-gen-1.0-SNAPSHOT.jar') and
            os.path.exists('tpcds-gen/target/tools/dsdgen')):
        raise Exception('Target jar file is not found in `target` folder, ' +
                        'please refer to README document and build this project first.')


def generate_data(args):
    check_build()
    if args.type == 'hdfs':
        # Check if hadoop is installed.
        if shutil.which('hadoop') is None:
            raise Exception('No Hadoop binary found in current environment, ' +
                            'please install Hadoop for data generation in cluster.')
        # Submit hadoop MR job to generate data
        os.chdir('tpcds-gen')
        subprocess.run(['hadoop', 'jar', 'target/tpcds-gen-1.0-SNAPSHOT.jar',
                        '-d', args.data_dir, '-p', args.parallel, '-s', args.scale], check=True)
    if args.type == 'local':
        if not os.path.isdir(args.data_dir):
            os.makedirs(args.data_dir)
        if args.data_dir[0] == '/':
            data_dir = args.data_dir
        else:
            # add this because the dsdgen will be executed in a sub-folder
            data_dir = '../../../{}'.format(args.data_dir)

        os.chdir('tpcds-gen/target/tools')
        proc = []
        for i in range(1, int(args.parallel) + 1):
            dsdgen_args = ["-scale", args.scale, "-dir", data_dir, "-parallel", args.parallel, "-child", str(i), "-force", "Y", "-verbose", "Y"]
            proc.append(subprocess.Popen(["./dsdgen"] + dsdgen_args))

        # wait for data generation to complete
        for i in range(int(args.parallel)):
            proc[i].wait()
            if proc[i].returncode != 0:
                print("dsdgen failed with return code {}".format(proc[i].returncode))
                raise Exception("dsdgen failed")

        os.chdir('../../..')
        from ds_convert import get_schemas
        # move multi-partition files into table folders
        for table in get_schemas(False).keys():
            print('mkdir -p {}/{}'.format(args.data_dir, table))
            os.system('mkdir -p {}/{}'.format(args.data_dir, table))
            for i in range(1, int(args.parallel)+1):
                os.system('mv {}/{}_{}_{}.dat {}/{}/ 2>/dev/null'.format(args.data_dir, table, i, args.parallel, args.data_dir, table))
        # show summary
        os.system('du -h -d1 {}'.format(args.data_dir))


def generate_query(args):
    check_build()
    # copy tpcds.idx to working dir, it's required by TPCDS tool
    subprocess.run(['cp', './tpcds-gen/target/tools/tpcds.idx',
                   './tpcds.idx'], check=True)

    if not os.path.isdir(args.query_output_dir):
        os.makedirs(args.query_output_dir)
    subprocess.run(['./tpcds-gen/target/tools/dsqgen', '-template', args.template, '-directory',
                    args.template_dir, '-dialect', 'spark', '-scale', args.scale, '-output_dir',
                    args.query_output_dir], check=True)
    # remove it after use.
    subprocess.run(['rm', './tpcds.idx'], check=True)


def generate_query_streams(args):
    check_build()
    # Copy tpcds.idx to working dir, it's required by TPCDS tool.
    subprocess.run(['cp', './tpcds-gen/target/tools/tpcds.idx',
                   './tpcds.idx'], check=True)

    if not os.path.isdir(args.query_output_dir):
        os.makedirs(args.query_output_dir)

    subprocess.run(['./tpcds-gen/target/tools/dsqgen', '-scale', args.scale, '-directory',
                    args.template_dir, '-output_dir', args.query_output_dir, '-input',
                    os.path.join(args.template_dir,'templates.lst'),
                    '-dialect', 'spark', '-streams', args.streams], check=True)
    # Remove it after use.
    subprocess.run(['rm', './tpcds.idx'], check=True)


def convert_csv_to_parquet(args):
    # This will submit a Spark job to read the TPCDS raw data (csv with "|" delimiter) then save as Parquet files.
    # The configuration for this will be read from an external template file. User should set Spark parameters there.
    with open(args.spark_submit_template, 'r') as f:
        template = f.read()

    cmd = []
    cmd.append("--input-prefix " + args.input_prefix)
    if args.input_suffix != "":
        cmd.append("--input-suffix " + args.input_suffix)
    cmd.append("--output-prefix " + args.output_prefix)
    cmd.append("--report-file " + args.report_file)
    cmd.append("--log-level " + args.log_level)
    if args.non_decimal:
        cmd.append("--non-decimal")

    # run spark-submit
    cmd = template.strip() + "\n  ds_convert.py " + " ".join(cmd).strip()
    print(cmd)
    os.system(cmd)

def gen_sql_from_stream(query_stream_file_path, data_path):
    with open(query_stream_file_path, 'r') as f:
        stream = f.read()
    # add \n to the head of stream for better split at next step
    stream = '\n' + stream
    all_queries = stream.split('\n-- start')
    # split query in q14, q23, q24, q39
    extended_queries = []
    matches = ['query14', 'query23', 'query24', 'query39']
    for q in all_queries:
        if any(s in q for s in matches):
            split_q = q.split(';')
            # now split_q has 3 items: 
            # 1. "query x in stream x using template query[xx].tpl query_part_1"
            # 2. "query_part_2"
            # 3. "-- end query [x] in stream [x] using template query[xx].tpl"
            part_1 = ''
            part_1 += split_q[0].replace('.tpl', '_part1.tpl')
            part_1 += ';'
            extended_queries.append(part_1)
            head = split_q[0].split('\n')[0]
            part_2 = head.replace('.tpl', '_part2.tpl') + '\n'
            part_2 += split_q[1]
            part_2 += ';'
            extended_queries.append(part_2)
        else:
            extended_queries.append(q)

    # add create data tables content
    spark_stream = ''
    from ds_convert import get_schemas
    for table_name in get_schemas(False).keys():
        spark_stream += 'println("====== Creating TempView for table {} ======")\n'.format(table_name)
        spark_stream += 'spark.time(spark.read.parquet("{}{}").createOrReplaceTempView("{}"))\n'.format(data_path, table_name, table_name)
    # add spark execution content, drop the first \n line
    for q in extended_queries[1:]:
        query_name = q[q.find('template')+9:q.find('.tpl')]
        spark_stream += 'println("====== Run {} ======")\n'.format(query_name)
        spark_stream += 'spark.time(spark.sql("""\n -- start'
        spark_stream += q
        spark_stream += '\n""").collect())\n\n'
    spark_stream += 'System.exit(0)'
    with open('{}.scala'.format(query_stream_file_path), 'w') as f:
        f.write(spark_stream)

def parse_run_log(run_log_path, csv_path):
    # Parse the log, show individual query time and total time
    cmd = "sed -i 's/Time taken/\\nTime taken/' {}".format(run_log_path)
    os.system(cmd)
    with open(run_log_path, 'r') as log:
        origin = log.read().splitlines()
        total_time = 0
        with open(csv_path, 'w') as csv_output:
            writer = csv.writer(csv_output)
            # writer header
            writer.writerow(['query', 'time'])
            row = []
            for line in origin:
                if 'TempView' in line:
                    # e.g. "====== Creating TempView for table store_sales ======"
                    print(line)
                    row.append(line.split()[-2])
                if 'Run query' in line:
                    # e.g. "====== Run query4 ======"
                    print(line)
                    row.append(line.split()[-2])
                if 'Time taken' in line:
                    # e.g. "Time taken: 25532 ms"
                    print(line)
                    query_time = line.split()[-2]
                    total_time += int(query_time)
                    row.append(query_time)
                # writer query and execution time
                if len(row) == 2:
                    writer.writerow(row)
                    row = []
        print("\n====== Total time : {} ms ======".format(total_time))

def run_query_stream(template_file_path, query_stream_file_path, run_log_path):
    if template_file_path is None:
        raise Exception('Please provide a Spark submit template file for the run.')
    # Execute Power Run
    with open(template_file_path, 'r') as f:
        template = f.read()
        cmd = template.strip() + "\n" + "-i {}.scala".format(query_stream_file_path)
        cmd += " 2>&1 | tee {}".format(run_log_path)
        print(cmd)
        os.system(cmd)

def power_run(args):
    gen_sql_from_stream(args.query_stream, args.input_prefix)
    run_query_stream(args.spark_submit_template, args.query_stream, args.run_log)
    parse_run_log(args.run_log, args.csv_output)


def throughput_run(args):
    streams = args.query_stream.split(',')
    # check if multiple streams are provided
    if len(streams) == 1:
        raise Exception('Throughput Run requires multiple query stream but only one is provided. ' +
            'Please use Power Run for one stream, or provide multiple query streams for Throughput Run.')
    # run queries together
    procs = []
    for stream in streams:
        gen_sql_from_stream(stream, args.input_prefix)
        # rename the log for each stream.
        # e.g. "./nds_query_streams/query_1.sql"
        log_path = args.run_log + '_{}'.format(stream.split('/')[-1][:-4])
        p = Process(target=run_query_stream, args=(args.spark_submit_template, stream, log_path))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()

    # parse logs for each stream
    for stream in streams:
        log_suffix = '_{}'.format(stream.split('/')[-1][:-4])
        log_path = args.run_log + log_suffix
        csv_path = args.csv_output + log_suffix
        parse_run_log(log_path, csv_path)


def main():
    check_version()
    parser = argparse.ArgumentParser(
        description='Argument parser for NDS benchmark options.')
    parser.add_argument('--generate', choices=['data', 'query', 'streams', 'convert'], 
                        help='generate tpc-ds data or queries.')
    parser.add_argument('--type', choices=['local', 'hdfs'], required='data' in sys.argv,
                        help='file system to save the generated data')
    parser.add_argument('--data-dir',
                        help='If generating data: target HDFS path for generated data.')
    parser.add_argument(
        '--template-dir', help='directory to find query templates.')
    parser.add_argument('--scale', help='volume of data to generate in GB.')
    parser.add_argument(
        '--parallel', help='generate data in n parallel MapReduce jobs.')
    parser.add_argument('--template', required='query' in sys.argv,
                        help='query template used to build queries.')
    parser.add_argument('--streams', help='generate how many query streams.')
    parser.add_argument('--query-output-dir',
                        help='directory to write query streams.')
    parser.add_argument('--spark-submit-template', required=('--run' in sys.argv) or ('convert' in sys.argv),
                        help='A Spark config template contains necessary Spark job configurations.')
    parser.add_argument('--output-mode',
                        help='Spark data source output mode for the result (default: overwrite)',
                        default="overwrite")
    parser.add_argument(
        '--input-prefix', help='text to prepend to every input file path (e.g., "hdfs:///ds-generated-data/"; the default is empty)', default="")
    parser.add_argument(
        '--input-suffix', help='text to append to every input filename (e.g., ".dat"; the default is empty)', default="")
    parser.add_argument(
        '--output-prefix', help='text to prepend to every output file (e.g., "hdfs:///ds-parquet/"; the default is empty)', default="")
    parser.add_argument(
        '--report-file', help='location in which to store a performance report', default='report.txt')
    parser.add_argument(
        '--log-level', help='set log level (default: OFF, same to log4j), possible values: OFF, ERROR, WARN, INFO, DEBUG, ALL', default="OFF")
    parser.add_argument('--run', choices=['power','throughput'], help='NDS run type')
    parser.add_argument(
        '--query-stream', help='query stream file that contains all NDS queries in specific order')
    parser.add_argument('--run-log', help='file to save  run logs')
    parser.add_argument('--csv-output', required='--run' in sys.argv, help='CSV file to save query and query execution time')
    parser.add_argument('--non-decimal', action='store_true',
                        help='replace DecimalType with DoubleType when saving parquet files. If not specified, decimal data will be saved.')
    args = parser.parse_args()

    if args.generate != None:
        if args.generate == 'data':
            generate_data(args)

        if args.generate == 'query':
            generate_query(args)

        if args.generate == 'streams':
            generate_query_streams(args)

        if args.generate == 'convert':
            convert_csv_to_parquet(args)
    else:
        if args.run == 'power':
            power_run(args)
        if args.run == 'throughput':
            throughput_run(args)


if __name__ == '__main__':
    main()
