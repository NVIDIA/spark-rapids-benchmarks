# NVIDIA cuDF plugin for Apache Spark Environment Detector

A tool for automatically detecting Spark cluster environment configuration, helping to optimize
NVIDIA cuDF plugin for Apache Spark POC testing environments.

## Features

This tool can automatically detect the following environment information:

### 1. Cluster Deployment Mode
- Standalone
- YARN
- Kubernetes
- Mesos
- Local

### 2. Cluster Node Information
- Number of nodes
- Single/multi-node detection
- Driver and Executor node details
- Node IP and hostname

### 3. Hardware Configuration
- **CPU**: Model, cores, architecture, frequency, physical/logical cores, hyper-threading
- **Memory**: Total physical memory, available memory, JVM heap memory, DIMM speed, NUMA nodes
- **GPU**: Device count, model, memory, compute capability, temperature, utilization, NVLink topology

### 4. Network Configuration
- Network type (internal/external)
- Network interface information (type, speed, MTU)
- Driver to Executor latency
- Bandwidth estimation
- InfiniBand/RoCE detection

### 5. Storage Configuration
- Storage types (HDFS, S3, OSS, GCS, Azure Blob, Local)
- HDFS information (capacity, usage, block size, replication)
- Local disk information
- Storage type detection (SSD, HDD, NVMe)
- Spark local directories (shuffle paths)

### 6. Software Versions
- Spark version
- Scala version
- Java version
- Hadoop version
- OS information (kernel version, distribution)
- **GPU Software Stack**:
  - NVIDIA driver version
  - CUDA version
  - cuDNN version
  - NCCL version
  - cuDF plugin version
  - nvidia-peermem status
  - libcuda.so presence
  - GPUDirect Storage status

### 7. Spark Configuration Summary
- Executor count and configuration
- Memory configuration
- Shuffle partitions
- Dynamic allocation status
- Adaptive execution status
- RAPIDS status
- Important config items
- spark-defaults.conf content

### 8. GPU Acceleration Readiness Score
- Overall readiness score (Green/Yellow/Red)
- Hardware score
- Software score
- Network score
- Storage score
- Issues and recommendations

## Performance Benchmarks

With the `--benchmark` flag, the tool also runs performance stress tests:

### Disk I/O Benchmark
- Sequential read/write throughput (MB/s)
- Random read/write IOPS

### Network Benchmark
- Driver to executor bandwidth
- Shuffle bandwidth
- Executor to executor bandwidth
- Network latency

### CPU Benchmark
- Single-thread GFLOPS
- Multi-thread GFLOPS
- CPU utilization

### Memory Benchmark
- Read bandwidth (GB/s)
- Write bandwidth (GB/s)
- Copy bandwidth (GB/s)

### GPU Benchmark
- Compute performance (TFLOPS)
- Memory bandwidth (GB/s)

## Build

### Prerequisites
- JDK 8+
- Maven 3.6+
- Spark 3.x (runtime)

### Build Command

```bash
./build.sh
```

Or manually execute:

```bash
mvn clean package -DskipTests
```

## Usage

### Local Mode

```bash
./run.sh
```

### Cluster Mode

```bash
# Standalone
./run-cluster.sh spark://master:7077

# YARN
./run-cluster.sh yarn

# Kubernetes
./run-cluster.sh k8s://https://k8s-api:443
```

### With Performance Benchmarks

```bash
./run-cluster.sh spark://master:7077 /tmp/env-report --benchmark
```

### Save Report

You can specify an output path to save the JSON format report:

```bash
# Local file
./run.sh /tmp/env-report

# HDFS
./run-cluster.sh yarn hdfs:///reports/env-report

# S3
./run-cluster.sh yarn s3a://bucket/reports/env-report
```

### Using spark-submit Directly

```bash
spark-submit \
    --master <master-url> \
    --driver-memory 2g \
    --executor-memory 2g \
    --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    target/spark-rapids-env-detector-1.0.0.jar \
    [--benchmark] [--no-gpu] [output-path]
```

## Output Example

```
================================================================================
CUDF PLUGIN ENVIRONMENT REPORT
================================================================================
Generated: 2024-01-15 14:30:25

--------------------------------------------------------------------------------
1. CLUSTER DEPLOYMENT
--------------------------------------------------------------------------------
  Mode:         yarn
  Master URL:   yarn
  Deploy Mode:  client
  Description:  YARN (Hadoop) cluster manager

--------------------------------------------------------------------------------
2. CLUSTER NODES
--------------------------------------------------------------------------------
  Total Nodes:     4
  Multi-Node:      true

  Driver Node:
    Hostname:      driver-host
    IP Address:    xxx.xxx.xxx.xxx

  Executor Nodes:
    - Executor 1: worker-1 (xxx.xxx.xxx.xxx)
    - Executor 2: worker-2 (xxx.xxx.xxx.xxx)
    - Executor 3: worker-3 (xxx.xxx.xxx.xxx)

--------------------------------------------------------------------------------
3. HARDWARE CONFIGURATION
--------------------------------------------------------------------------------
  Driver Hardware:
    CPU:
      Model:        Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz
      Physical:     20 cores
      Logical:      40 threads
      Hyper-Threading: Enabled
      Architecture: amd64
      Frequency:    2.50 GHz
    Memory:
      Total:        256.00 GB
      Free:         128.45 GB
      JVM Max Heap: 16.00 GB
      NUMA Nodes:   2
    GPU (4 device(s)):
      [0] NVIDIA A100-SXM4-40GB
          Memory:      40.00 GB total, 38.50 GB free
          Compute Cap: 8.0
          Temperature: 35°C
          Utilization: 0%
      ...

--------------------------------------------------------------------------------
4. SOFTWARE CONFIGURATION
--------------------------------------------------------------------------------
  Spark Version:   3.4.1
  Scala Version:   2.12.15
  Java Version:    1.8.0_312
  Hadoop Version:  3.3.4

  Operating System:
    Name:          Linux
    Version:       5.4.0-150-generic
    Kernel:        5.4.0-150-generic
    Distribution:  Ubuntu 22.04.3 LTS
    Architecture:  amd64

  GPU Software Stack:
    NVIDIA Driver: 535.104.12
    CUDA:          12.2
    cuDNN:         8.9.5
    NCCL:          2.18.3
    spark-rapids:  24.02.0
    nvidia-peermem: Loaded
    libcuda.so:    /usr/lib/x86_64-linux-gnu/libcuda.so
    GDS:           Enabled

...
================================================================================
GPU ACCELERATION READINESS SCORE
================================================================================
  Overall:    Green - Excellent
  Hardware:   Green
  Software:   Green
  Network:    Green
  Storage:    Green

================================================================================
END OF REPORT
================================================================================
```

## License

Apache License 2.0
