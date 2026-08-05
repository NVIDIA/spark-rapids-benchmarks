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

# Build script for the NVIDIA cuDF plugin for Apache Spark Environment Detector

set -e

echo "Building the NVIDIA cuDF plugin for Apache Spark Environment Detector..."
echo

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

# Use local settings if URM_URL is not set
if [ -z "$URM_URL" ] && [ -f "settings.xml" ]; then
    MVN_OPTS="--settings settings.xml"
else
    MVN_OPTS=""
fi

# Clean and build
mvn clean package -DskipTests $MVN_OPTS

echo
echo "Build completed successfully!"
echo "JAR file: target/spark-rapids-env-detector-1.0.0.jar"
echo
echo "To run the detector, use:"
echo "  ./run.sh                    # Run locally"
echo "  ./run-cluster.sh <master>   # Run on cluster"

