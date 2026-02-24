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
import subprocess
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
utils_dir = os.path.join(parent_dir, 'utils')
sys.path.insert(0, utils_dir)

from check import check_build_nds_h, check_version, get_abs_path

check_version()

def generate_query_streams(args, tool_path):
    """call TPC-H qgen tool to generate a specific query or query stream(s) that contains all
    TPC-DS queries.

    Args:
        args (Namespace): Namespace from argparser
        tool_path (str): path to the tool
    """
    # move to the tools directory
    work_dir = tool_path.parent
    output_dir = get_abs_path(args.output_dir)

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    
    os.environ["DSS_QUERY"] = str(work_dir / "queries")

    base_cmd = ['./qgen',
                '-s', args.scale]
    
    if args.streams:
        procs = []
        for i in range(1,int(args.streams)+1):
            new_cmd = base_cmd + ['-p',str(i)]
            output_file = os.path.join(output_dir, f"stream_{i}.sql")
            with open(output_file,'w') as f:
                procs.append(subprocess.Popen(new_cmd, cwd=str(work_dir), stdout=f))
        for p in procs:
            p.wait()
            if p.returncode != 0:
                print("QGEN failed with return code {}".format(p.returncode))
                raise Exception("dbgen failed")
    else:
        output_file = os.path.join(output_dir, f"query_{args.template}.sql")
        base_cmd = base_cmd + ['-d',args.template]
        with open(output_file,"w") as f:
            subprocess.run(base_cmd, check=True, cwd=str(work_dir),stdout=f)

if __name__ == "__main__":
    jar_path, tool_path = check_build_nds_h()
    parser = parser = argparse.ArgumentParser()
    parser.add_argument("scale",
                        help="Assume a database of this scale factor.")
    parser.add_argument("output_dir",
                        help="Generate query stream in directory.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--template",
                        help="build queries from this template. Only used to generate one query " +
                        "from one tempalte. This argument is mutually exclusive with --streams. " +
                        "It is often used for test purpose.")
    group.add_argument('--streams',
                        help='generate how many query streams. ' +
                        'This argument is mutually exclusive with --template.')
    args = parser.parse_args()

    generate_query_streams(args, tool_path) 
