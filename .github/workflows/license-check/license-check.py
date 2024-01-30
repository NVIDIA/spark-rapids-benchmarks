#!/usr/bin/env python

# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A license header check
The tool checks if all files in the repo contain the license header.
NOTE: this script is for github actions only, you should not use it anywhere else.
"""
import os
import sys
import glob
from argparse import ArgumentParser

# Configs of the license header check
includes = ['**/*']
excludes = ['**/*.md']
LICENSE_PATTERN = "Licensed under the Apache License"

def filterFiles(repo_path):
    files = []
    files_excluded = []
    os.chdir(repo_path)
    for pattern in includes:
        files.extend([ f for f in glob.glob(repo_path + '/' + pattern, recursive=True) if os.path.isfile(f)])
    for pattern in excludes:
        files_excluded.extend([ f for f in glob.glob(repo_path + '/' + pattern, recursive=True) if os.path.isfile(f)])
    return list(set(files) - set(files_excluded)), list(set(files_excluded))

def checkLicenseHeader(files):
    no_license_files = []
    for file in files:
        with open(file, 'r') as f:
            print("Checking file: {}".format(file))
            content = f.read()
            if LICENSE_PATTERN not in content:
                no_license_files.append(file)
    return no_license_files

if __name__ == '__main__':
    try:
        repo_path = '.'
        files, files_excluded = filterFiles(repo_path)
        no_license_files = checkLicenseHeader(files)
        warning_message = ""
        for file in files_excluded:
            warning_message += "WARNING: {} is excluded from this check.\n".format(file)
        print(warning_message)
        if no_license_files:
            error_message = ""
            for file in no_license_files:
                error_message += "ERROR: {} does not contain license header.\n".format(file)
            raise Exception(error_message)
    except Exception as e:
        print(e)
        sys.exit(1)