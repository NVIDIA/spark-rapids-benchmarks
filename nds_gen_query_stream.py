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
import subprocess
import sys

from check import check_build, check_version, get_abs_path

check_version()

def generate_query_streams(args, tool_path):
    """call TPC-DS dsqgen tool to generate a specific query or query stream(s) that contains all
    TPC-DS queries.

    Args:
        args (Namespace): Namespace from argparser
        tool_path (str): path to the tool
    """
    # move to the tools directory
    work_dir = tool_path.parent
    output_dir = get_abs_path(args.output_dir)
    template_dir = get_abs_path(args.template_dir)
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    
    base_cmd = ['./dsqgen',
                '-scale', args.scale,
                '-directory', template_dir,
                '-dialect', 'spark',
                '-output_dir', output_dir]
    
    if args.streams:
        cmd = base_cmd + ['-input', template_dir + '/' + 'templates.lst',
                          '-streams', args.streams]
    else:
        cmd = base_cmd + ['-template', args.template]
    subprocess.run(cmd, check=True, cwd=str(work_dir))


if __name__ == "__main__":
    _, tool_path = check_build()
    parser = parser = argparse.ArgumentParser()
    parser.add_argument('template_dir',
                        help='directory to find query templates and dialect file.')
    parser.add_argument("scale",
                        help="assume a database of this scale factor."
    )
    parser.add_argument("output_dir",
                        help="generate query in directory.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--template",
                        help="build queries from this template")
    group.add_argument('--streams',
                        help='generate how many query streams. ' +
                        'If not specified, only one query will be produced.')


    args = parser.parse_args()

    generate_query_streams(args, tool_path) 
