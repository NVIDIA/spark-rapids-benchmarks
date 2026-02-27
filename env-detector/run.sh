#!/bin/bash
#
# SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

# Run script for Spark Rapids Environment Detector (local mode)

set -e

JAR_FILE="target/spark-rapids-env-detector-1.0.0.jar"

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "JAR file not found. Building first..."
    ./build.sh
fi

# Check if SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
    echo "Warning: SPARK_HOME is not set. Trying to find spark-submit..."
    SPARK_SUBMIT=$(which spark-submit 2>/dev/null || true)
    if [ -z "$SPARK_SUBMIT" ]; then
        echo "Error: spark-submit not found. Please set SPARK_HOME or add spark-submit to PATH."
        exit 1
    fi
else
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi

# Output path (optional)
OUTPUT_PATH="$1"

echo "Running Spark Rapids Environment Detector..."
echo "=============================================="
echo

# Run in local mode
$SPARK_SUBMIT \
    --master local[*] \
    --driver-memory 2g \
    --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    "$JAR_FILE" \
    $OUTPUT_PATH

