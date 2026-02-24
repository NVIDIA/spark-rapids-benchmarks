#!/bin/bash
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

