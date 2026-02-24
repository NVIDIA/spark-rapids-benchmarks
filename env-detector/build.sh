#!/bin/bash
# Build script for Spark Rapids Environment Detector

set -e

echo "Building Spark Rapids Environment Detector..."
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

