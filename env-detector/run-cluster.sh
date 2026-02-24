#!/bin/bash
# Run script for Spark Rapids Environment Detector on a cluster

set -e

JAR_FILE="target/spark-rapids-env-detector-1.0.0.jar"

# Usage
usage() {
    echo "Usage: $0 <master-url> [output-path] [--benchmark] [--no-gpu]"
    echo
    echo "Arguments:"
    echo "  master-url   Spark master URL (e.g., spark://host:7077, yarn, k8s://...)"
    echo "  output-path  Optional path to save the report (HDFS, S3, local, etc.)"
    echo "  --benchmark  Run performance benchmarks (disk IOPS, network bandwidth, CPU, memory, GPU)"
    echo "  --no-gpu     Skip GPU benchmarks"
    echo
    echo "Environment variables (optional, auto-detected if not set):"
    echo "  DRIVER_MEMORY     Driver memory (default: 4g)"
    echo "  EXECUTOR_MEMORY   Executor memory (default: auto-detect or 8g)"
    echo "  EXECUTOR_CORES    Executor cores (default: auto-detect or 4)"
    echo "  NUM_EXECUTORS     Number of executors (default: auto-detect all available)"
    echo
    echo "Examples:"
    echo "  $0 spark://master:7077"
    echo "  $0 yarn hdfs:///reports/env-report"
    echo "  $0 spark://master:7077 /tmp/report --benchmark"
    echo "  EXECUTOR_MEMORY=32g NUM_EXECUTORS=10 $0 spark://master:7077"
    exit 1
}

# Check arguments
if [ $# -lt 1 ]; then
    usage
fi

MASTER_URL="$1"
OUTPUT_PATH=""
APP_ARGS=""

# Parse arguments
shift  # Remove master-url from args
for arg in "$@"; do
    case $arg in
        --benchmark|--no-gpu|--help)
            APP_ARGS="$APP_ARGS $arg"
            ;;
        *)
            if [ -z "$OUTPUT_PATH" ]; then
                OUTPUT_PATH="$arg"
            fi
            ;;
    esac
done

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

# Default values (can be overridden by environment variables)
DRIVER_MEMORY=${DRIVER_MEMORY:-4g}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-16g}
EXECUTOR_CORES=${EXECUTOR_CORES:-16}

echo "Running Spark Rapids Environment Detector on cluster..."
echo "Master: $MASTER_URL"
echo "Driver Memory: $DRIVER_MEMORY"
echo "Executor Memory: $EXECUTOR_MEMORY"
echo "Executor Cores: $EXECUTOR_CORES"
echo "========================================================="
echo

# Determine deploy mode and additional options based on master type
EXTRA_OPTS=""
RESOURCE_OPTS=""

if [[ "$MASTER_URL" == yarn* ]]; then
    # YARN: Use dynamic allocation to utilize all available resources
    EXTRA_OPTS="--deploy-mode client"
    RESOURCE_OPTS="--conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.minExecutors=1 \
        --conf spark.dynamicAllocation.maxExecutors=500 \
        --conf spark.shuffle.service.enabled=true"
    echo "Using YARN dynamic allocation (will use all available cluster resources)"
elif [[ "$MASTER_URL" == k8s* ]]; then
    # Kubernetes: Use dynamic allocation
    EXTRA_OPTS="--deploy-mode cluster"
    RESOURCE_OPTS="--conf spark.dynamicAllocation.enabled=true \
        --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
        --conf spark.dynamicAllocation.minExecutors=1 \
        --conf spark.dynamicAllocation.maxExecutors=500"
    echo "Using K8s dynamic allocation"
elif [[ "$MASTER_URL" == spark://* ]]; then
    # Standalone: Request all available resources
    # If NUM_EXECUTORS is not set, don't specify it (use all available)
    if [ -n "$NUM_EXECUTORS" ]; then
        RESOURCE_OPTS="--num-executors $NUM_EXECUTORS"
    fi
    # Use all cores available per executor
    RESOURCE_OPTS="$RESOURCE_OPTS \
        --conf spark.executor.memory=$EXECUTOR_MEMORY \
        --conf spark.executor.cores=$EXECUTOR_CORES \
        --conf spark.cores.max=0"  # 0 means use all available
    echo "Using Standalone mode (will request maximum available resources)"
else
    # Local or other: minimal config
    echo "Using default configuration"
fi

# Run on cluster
$SPARK_SUBMIT \
    --master "$MASTER_URL" \
    $EXTRA_OPTS \
    --driver-memory $DRIVER_MEMORY \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    $RESOURCE_OPTS \
    --conf spark.eventLog.enabled=false \
    --conf spark.sql.shuffle.partitions=200 \
    --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    "$JAR_FILE" \
    $APP_ARGS $OUTPUT_PATH
