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

# This script starts from Load Tests and will not generate raw data.
# We assume raw CSV data including "Maintenance data" is already generated.
#
# 1. run nds_transcode.py to load data to Iceberg. => get "TLoad" and "timestamp" for 2.
# 2. run nds_gen_query_stream.py to generate query streams with RNDSEED = "timestamp" from 1.
#    TPC-DS specification requires Sq >= 4, but this script allow Sq >= 1 for test purpose.
# 3. run nds_power.py to do Power Test => get "TPower"
# 4. run nds-throughput to do Throughput Test 1. => get "Ttt1"
# 5. run nds_maintenance.py to do Maintenance Test 1.  => get "Tdm1"
# 6. run nds-throughput to do Throughput Test 2. => get "Ttt2"
# 7. run nds_maintenance.py to do Maintenance Test 2. => get "Tdm2"

import argparse
import math
import subprocess
import yaml


def get_yaml_params(yaml_file):
    with open(yaml_file, 'r') as f:
        try:
            params = yaml.safe_load(f)
            return params
        except yaml.YAMLError as exc:
            print(exc)
            return None


def get_load_end_timestamp(load_report_file):
    """get the end timestamp in str format from the load report file"""
    rngseed = None
    with open(load_report_file, "r") as f:
        for line in f:
            if "RNGSEED used:" in line:
                # e.g. "RNGSEED used: 07291122510"
                rngseed = line.split(":")[1].strip()
    if rngseed:
        return rngseed
    else:
        raise Exception(
            f"RNGSEED not found in Load Test report file: {load_report_file}")


def get_load_time(load_report_file):
    """get the load test elapse time in str format from the load report file"""
    load_elapse = None
    with open(load_report_file, "r") as f:
        for line in f:
            if "Load Test Time" in line:
                # e.g. "Load Test Time: 1234 seconds"
                load_elapse = line.split(":")[1].split(" ")[1]
    if load_elapse:
        return load_elapse
    else:
        raise Exception(
            f"Load Test Time not found in Load Test report file: {load_report_file}.")


def get_power_time(power_report_file):
    """get the total elapse time for Power Test in str format from the power report file"""
    power_elapse = None
    with open(power_report_file, "r") as f:
        for line in f:
            if "Power Test Time" in line:
                # e.g. "app-20220715143743-0007,Power Test Time,11838"
                power_elapse = line.split(",")[2].strip()
    if power_elapse:
        return power_elapse
    else:
        raise Exception(
            f"Power Test Time not found in Power Test report file: {power_report_file}.")


def get_start_end_time(report_file):
    """get the start timestamp in str format from the Power Test report file"""
    start_time = None
    end_time = None
    with open(report_file, "r") as f:
        for line in f:
            if "Power Start Time" in line:
                # e.g. "app-20220715143743-0007,Power Start Time,1659067405.468058"
                start_time = line.split(",")[2].strip()
            if "Power End Time" in line:
                # e.g. "app-20220715143743-0007,Power End Time,1659067405.468058"
                end_time = line.split(",")[2].strip()
    if start_time and end_time:
        return start_time, end_time
    else:
        raise Exception(
            f"Start or End time not found in Power Test report file: {report_file}")


def get_throughput_time(throughput_report_file_base, num_streams, first_or_second):
    """get Throughput elapse time according to Spec 7.4.7.4. 
    Filter all Throughput reports and get the start timestamp and end timestamp to calculate the 
    elapse time of Throughput Test

    num_streams (int): number of streams in total including Power Stream
    first_or_second (int): 1 for first throughput test, 2 for second throughput test 
    """
    start_time = []
    end_time = []
    if first_or_second == 1:
        stream_range = [x for x in range(1, num_streams//2+1)]
    else:
        stream_range = [x for x in range(num_streams//2+1, num_streams+1)]

    for stream_num in stream_range:
        report_file = throughput_report_file_base+ f"_{stream_num}.csv"
        sub_start_time, sub_end_time = get_start_end_time(report_file)
        start_time.append(float(sub_start_time))
        end_time.append(float(sub_end_time))
    start_time = min(start_time)
    end_time = max(end_time)
    elapse = round_up_to_nearest_10_percent(end_time - start_time)
    return elapse


def get_maintenance_time(maintenance_report_file, maintenance_load_report_file):
    """get Maintenance elapse time from report"""
    maintenance_load_time = None
    maintenance_elapse = None
    with open(maintenance_report_file, "r") as f:
        for line in f:
            if "Data Maintenance Time" in line:
                # e.g. "app-20220715143743-0007,Data Maintenance Time,11838"
                maintenance_elapse = line.split(",")[2].strip()

    # refresh data load time is counted as a part in the maintenance time
    maintenance_load_time = get_load_time(maintenance_load_report_file)
    if maintenance_elapse:
        return float(maintenance_elapse) + float(maintenance_load_time)
    else:
        raise Exception("Data Maintenance Time not found in Data Maintenance report file: " +
                        f"{maintenance_report_file}.")


def get_throughput_stream_nums(num_streams, first_or_second):
    if first_or_second == 1:
        return ",".join([str(x) for x in range(1, num_streams//2+1)])
    else:
        return ",".join([str(x) for x in range(num_streams//2+1, num_streams+1)])


def round_up_to_nearest_10_percent(num):
    return math.ceil(num * 10) / 10


def run_data_gen(scale, parallel, data_path, local_or_hdfs, num_streams):
    gen_data_cmd = ["python3",
                    "nds_gen_data.py",
                    local_or_hdfs,
                    scale,
                    parallel,
                    data_path,
                    "--overwrite_output"]
    subprocess.run(gen_data_cmd, check=True)
    for i in range(1, num_streams):
        gen_refresh_data_cmd = ["python3",
                                "nds_gen_data.py",
                                local_or_hdfs,
                                scale,
                                parallel,
                                data_path + f"_{i}",
                                "--overwrite_output",
                                "--update", str(i)]
        subprocess.run(gen_refresh_data_cmd, check=True)


def run_load_test(template_path,
                  input_path,
                  output_path,
                  load_report_file):
    load_test_cmd = ["./spark-submit-template",
                     template_path,
                     "nds_transcode.py",
                     input_path,
                     output_path,
                     load_report_file,
                     "--output_format", "iceberg",
                     '--output_mode', "overwrite"]
    subprocess.run(load_test_cmd, check=True)


def gen_streams(num_streams,
                template_dir,
                scale_factor,
                stream_output_path,
                RNGSEED):
    gen_stream_cmd = ["python3",
                      "nds_gen_query_stream.py",
                      template_dir,
                      scale_factor,
                      stream_output_path,
                      "--rngseed", RNGSEED,
                      "--streams", str(num_streams)]
    subprocess.run(gen_stream_cmd, check=True)


def power_test(template_path,
               input_path,
               stream_path,
               report_path,
               property_path):
    power_test_cmd = ["./spark-submit-template",
                      template_path,
                      "nds_power.py",
                      input_path,
                      stream_path,
                      report_path,
                      "--input_format", "iceberg",
                      "--property_file", property_path]
    subprocess.run(power_test_cmd, check=True)


def throughput_test(num_streams,
                    first_or_second,
                    template_path,
                    input_path,
                    stream_base_path,
                    report_base_path,
                    property_path):
    throughput_cmd = ["./nds-throughput",
                      get_throughput_stream_nums(num_streams, first_or_second),
                      "./spark-submit-template",
                      template_path,
                      "nds_power.py",
                      input_path,
                      stream_base_path + "/query_{}.sql",
                      report_base_path + "_{}.csv",
                      "--input_format", "iceberg",
                      "--property_file", property_path]
    
    print(throughput_cmd)
    subprocess.run(throughput_cmd, check=True)


def maintenance_test(num_streams,
                     first_or_second,
                     template_path,
                     maintenance_raw_data_base_path,
                     maintenance_parquet_data_base_path,
                     maintenance_query_path,
                     maintenance_load_report_base_path,
                     maintenance_report_base_path,
                     property_path):
    if first_or_second == 1:
        refresh_nums = [i for i in range(1, num_streams//2+1)]
    else:
        refresh_nums = [i for i in range(num_streams//2+1, num_streams+1)]

    Tdm = 0
    # refresh run for each stream in Throughput Test 1.
    for i in refresh_nums:
        maintenance_raw_path = maintenance_raw_data_base_path + f"_{i}"
        maintenance_parquet_path = maintenance_parquet_data_base_path + f"_{i}"
        maintenance_load_report_path = maintenance_load_report_base_path + \
            f"_{i}" + ".txt"
        maintenance_report_path = maintenance_report_base_path + \
            f"_{i}" + ".csv"

        # the load time of refresh data is counted as a part in the maintenance time
        maintenance_load_cmd = ["./spark-submit-template",
                                template_path,
                                "nds_transcode.py",
                                maintenance_raw_path,
                                maintenance_parquet_path,
                                maintenance_load_report_path,
                                "--output_format", "parquet",
                                '--output_mode', "overwrite",
                                "--update"]
        subprocess.run(maintenance_load_cmd, check=True)
        Tdm += float(get_load_time(maintenance_load_report_path))
        maintenance_cmd = ["./spark-submit-template",
                           template_path,
                           "nds_maintenance.py",
                           maintenance_parquet_path,
                           maintenance_query_path,
                           maintenance_report_path,
                           "--property_file", property_path]
        subprocess.run(maintenance_cmd, check=True)
        Tdm += float(get_maintenance_time(maintenance_report_path, maintenance_load_report_path))

def run_full_bench(yaml_params):
    skip_data_gen = yaml_params['data_gen']['skip']
    scale_factor = str(yaml_params['data_gen']['scale_factor'])
    parallel = str(yaml_params['data_gen']['parallel'])
    raw_data_path = yaml_params['data_gen']['raw_data_path']
    local_or_hdfs = yaml_params['data_gen']['local_or_hdfs']
    template_path = yaml_params['load_test']['spark_template_path']
    iceberg_output_path = yaml_params['load_test']['output_path']
    load_report_path = yaml_params['load_test']['report_path']
    num_streams = yaml_params['generate_query_stream']['num_streams']
    query_template_dir = yaml_params['generate_query_stream']['query_template_dir']
    stream_output_path = yaml_params['generate_query_stream']['stream_output_path']
    power_stream_path = stream_output_path + "/query_0.sql"
    power_report_path = yaml_params['power_test']['report_path']
    power_property_path = yaml_params['power_test']['property_path']
    throughput_report_base = yaml_params['throughput_test']['report_base_path']
    maintenance_raw_data_base_path = yaml_params['maintenance_test']['raw_data_base_path']
    maintenance_parquet_data_base_path = yaml_params['maintenance_test']['output_data']
    maintenance_query_dir = yaml_params['maintenance_test']['query_dir']
    maintenance_load_report_base_path = yaml_params['maintenance_test']['load_report_base_path']
    maintenance_report_base_path = yaml_params['maintenance_test']['maintenance_report_base_path']
    
    
    # 0.
    # if not skip_data_gen:
    #     run_data_gen(scale_factor, parallel, raw_data_path, local_or_hdfs, num_streams)
    # # 1.
    # run_load_test(template_path,
    #               raw_data_path,
    #               iceberg_output_path,
    #               load_report_path)
    # Tld = float(get_load_time(load_report_path))
    # 2.
    # RNGSEED = get_load_end_timestamp(load_report_path)
    # gen_streams(num_streams, query_template_dir, scale_factor, stream_output_path, RNGSEED)
    # 3.
    # power_test(template_path,
    #            iceberg_output_path,
    #            power_stream_path,
    #            power_report_path,
    #            power_property_path)

    # TPower is in milliseconds
    # But Spec 7.1.16: Elapsed time is measured in seconds rounded up to the nearest 0.1 second.
    # Convert it to seconds.
    # TPower = round_up_to_nearest_10_percent(
    #     float(get_power_time(power_report_path)) / 1000)

    # 4.
    # throughput_test(num_streams,
    #                 1,
    #                 template_path,
    #                 iceberg_output_path,
    #                 stream_output_path,
    #                 throughput_report_base,
    #                 power_property_path)
    Ttt1 = get_throughput_time(throughput_report_base,
                               num_streams, 1)
    # 5
    Tdm1 = maintenance_test(num_streams,
                            1,
                            template_path,
                            maintenance_raw_data_base_path,
                            maintenance_parquet_data_base_path,
                            maintenance_query_dir,
                            maintenance_load_report_base_path,
                            maintenance_report_base_path,
                            power_property_path)
    # 6
    throughput_test(num_streams,
                    2,
                    template_path,
                    iceberg_output_path,
                    stream_output_path,
                    throughput_report_base,
                    power_property_path)
    Ttt2 = get_throughput_time(throughput_report_base,
                               num_streams, 2)
    # 7
    Tdm1 = maintenance_test(num_streams,
                            2,
                            template_path,
                            maintenance_raw_data_base_path,
                            maintenance_parquet_data_base_path,
                            maintenance_query_dir,
                            maintenance_report_base_path)


if __name__ == "__main__":
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('yaml_config',
                        help='yaml config file for the benchmark')
    args = parser.parse_args()
    params = get_yaml_params(args.yaml_config)    
    run_full_bench(params)
