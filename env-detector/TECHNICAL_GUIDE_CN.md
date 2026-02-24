# Spark Rapids 环境检测器 - 技术指南

## 1. 使用方式

### 1.1 基本用法

```bash
# Local 模式 - 简单检测
./run.sh

# 集群模式 - 环境检测
./run-cluster.sh spark://master:7077

# 带性能基准测试
./run-cluster.sh spark://master:7077 /output/path --benchmark

# YARN 模式
./run-cluster.sh yarn hdfs:///reports/env-report --benchmark

# 使用 spark-submit 直接运行
spark-submit \
    --master <master-url> \
    --driver-memory 4g \
    --executor-memory 16g \
    --executor-cores 16 \
    --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    spark-rapids-env-detector-1.0.0.jar \
    --benchmark /output/path
```

### 1.2 命令行参数

| 参数 | 说明 |
|------|------|
| `--benchmark` | 运行性能基准测试 (disk, network, CPU, memory, GPU) |
| `--no-gpu` | 跳过 GPU 相关测试 |
| `--help` | 显示帮助信息 |
| `[output-path]` | 可选，报告保存路径 |

---

### 1.3 带与不带 `--benchmark` 的区别

#### 代码流程对比

```scala
// EnvDetector.scala 核心逻辑

def main(args: Array[String]): Unit = {
  val config = parseArgs(args)  // 解析参数，检查是否有 --benchmark
  
  // ✅ 始终执行：环境检测
  val envReport = detectEnvironment(spark)
  
  // ⭐ 关键区别在这里
  val benchmarkReport = if (config.runBenchmark) {  // --benchmark 时才执行
    Some(BenchmarkRunner.run(spark.sparkContext, config.enableGpu))
  } else {
    None  // 不带 --benchmark 时跳过
  }
  
  // 输出报告
  ReportGenerator.printReport(envReport)
  benchmarkReport.foreach(ReportGenerator.printBenchmarkReport)  // 有 benchmark 才打印
}
```

#### 功能对比表

| 功能模块 | 不带 `--benchmark` | 带 `--benchmark` |
|----------|-------------------|------------------|
| **集群部署检测** | ✅ | ✅ |
| **节点信息检测** | ✅ | ✅ |
| **网络配置检测** (接口/类型/MTU/速度) | ✅ | ✅ |
| **存储配置检测** | ✅ | ✅ |
| **软件版本检测** | ✅ | ✅ |
| **硬件配置检测** | ✅ | ✅ |
| **Spark 配置收集** | ✅ | ✅ |
| **GPU 就绪度评分** | ✅ | ✅ |
| **磁盘 I/O 压测** | ❌ | ✅ |
| **网络带宽压测** | ❌ | ✅ |
| **CPU 性能压测** | ❌ | ✅ |
| **内存带宽压测** | ❌ | ✅ |
| **GPU 性能压测** | ❌ | ✅ |

#### 执行时间对比

| 模式 | 预计时间 | 说明 |
|------|----------|------|
| **不带 `--benchmark`** | ~10-30 秒 | 仅读取系统信息，无压力测试 |
| **带 `--benchmark`** | ~2-10 分钟 | 执行大量数据读写和计算 |

#### 输出文件对比

**不带 `--benchmark`：**
```
./env-report/part-00000              ← 只有环境报告
```

**带 `--benchmark`：**
```
./env-report/part-00000              ← 环境报告
./env-report_benchmark/part-00000    ← 性能测试报告 (额外)
```

#### Benchmark 执行的 5 个压力测试

```scala
// BenchmarkRunner.scala

def run(sc: SparkContext, enableGpu: Boolean): BenchmarkReport = {
  
  // [1/5] 磁盘 I/O - 写入/读取 256MB 文件，随机 I/O 测试
  val diskResult = DiskBenchmark.run(sc, params)
  
  // [2/5] 网络 - 4GB shuffle 数据，多轮测试
  val networkResult = NetworkBenchmark.run(sc, params)
  
  // [3/5] CPU - 200万次浮点运算，单线程/多线程
  val cpuResult = CpuBenchmark.run(sc, params)
  
  // [4/5] 内存 - 64MB 读写带宽测试
  val memoryResult = MemoryBenchmark.run(sc, params)
  
  // [5/5] GPU - 估算计算能力和显存带宽
  val gpuResult = GpuBenchmark.run(sc, params)
}
```

#### 使用建议

| 场景 | 推荐 |
|------|------|
| **快速了解环境配置** | 不带 `--benchmark` |
| **诊断性能问题** | 带 `--benchmark` |
| **POC 前环境评估** | 带 `--benchmark` |
| **自动化脚本定期检查** | 不带 `--benchmark`（节省时间） |

---

## 2. 环境检测指标及实现

### 2.1 CPU 信息检测

**源文件**: `detectors/HardwareDetector.scala`

| 指标 | 获取方式 |
|------|----------|
| **CPU 型号** | 解析 `/proc/cpuinfo` 的 `model name` 字段 |
| **物理核心数** | 解析 `physical id` 和 `cpu cores` 计算 `sockets × cores_per_socket` |
| **逻辑核心数** | 统计 `/proc/cpuinfo` 中 `processor` 条目数量 |
| **超线程状态** | 比较 `逻辑核心 > 物理核心` |
| **最大频率** | 读取 `/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq` |
| **macOS 兼容** | 使用 `sysctl -n machdep.cpu.brand_string` 等命令 |

**代码示例**:
```scala
private def getCpuDetailsFromProc: Option[CpuDetails] = {
  val cpuInfo = scala.io.Source.fromFile("/proc/cpuinfo").mkString
  val modelPattern = """model name\s*:\s*(.+)""".r
  val model = modelPattern.findFirstMatchIn(cpuInfo).map(_.group(1).trim)
  // 统计唯一的 physical ID (sockets)
  val physicalIds = physicalIdPattern.findAllMatchIn(cpuInfo).map(_.group(1).toInt).toSet
  val socketsCount = physicalIds.size
  // ...
}
```

---

### 2.2 内存信息检测

**源文件**: `detectors/HardwareDetector.scala`

| 指标 | 获取方式 |
|------|----------|
| **总物理内存** | `OperatingSystemMXBean.getTotalPhysicalMemorySize()` 或 `/proc/meminfo` |
| **可用内存** | `OperatingSystemMXBean.getFreePhysicalMemorySize()` 或 `/proc/meminfo` 的 `MemAvailable` |
| **JVM 堆内存** | `Runtime.getRuntime.maxMemory()` |
| **DIMM 速度/数量** | 执行 `sudo dmidecode -t memory` 解析 `Speed` 和 `Type` 字段 |
| **NUMA 节点数** | 执行 `numactl --hardware` 或统计 `/sys/devices/system/node/node*` 目录 |

**代码示例**:
```scala
private def getDimmInfo: (Option[String], Option[Int]) = {
  val output = "sudo dmidecode -t memory 2>/dev/null".!!
  val speedPattern = """Speed:\s+(\d+\s*MT/s|\d+\s*MHz)""".r
  val typePattern = """Type:\s+(DDR\d+)""".r
  // 解析速度和类型...
}

private def getNumaNodes: Option[Int] = {
  val output = "numactl --hardware 2>/dev/null".!!
  val pattern = """available:\s+(\d+)\s+nodes""".r
  pattern.findFirstMatchIn(output).map(_.group(1).toInt)
}
```

---

### 2.3 GPU 信息检测

**源文件**: `detectors/HardwareDetector.scala`

| 指标 | 获取方式 |
|------|----------|
| **GPU 型号/数量** | `nvidia-smi --query-gpu=name,index` |
| **显存总量/可用** | `nvidia-smi --query-gpu=memory.total,memory.free` |
| **计算能力** | `nvidia-smi --query-gpu=compute_cap` |
| **温度/利用率** | `nvidia-smi --query-gpu=temperature.gpu,utilization.gpu` |
| **NVLink 拓扑** | 解析 `nvidia-smi topo -m` 输出，提取 `NV1`、`NV2` 等连接信息 |
| **NVLink 带宽** | 根据 NVLink 版本估算 (v1: 20GB/s, v2: 25GB/s, v3: 50GB/s, v4: 100GB/s) |

**代码示例**:
```scala
private def getGpuInfo: Option[GpuInfo] = {
  val queryFields = "index,name,memory.total,memory.free,compute_cap,temperature.gpu,utilization.gpu"
  val output = s"nvidia-smi --query-gpu=$queryFields --format=csv,noheader,nounits".!!
  val gpus = output.trim.split("\n").map { line =>
    val fields = line.split(",").map(_.trim)
    GpuDevice(index = fields(0).toInt, name = fields(1), ...)
  }
}
```

---

### 2.4 网络信息检测

**源文件**: `detectors/NetworkDetector.scala`

| 指标 | 获取方式 |
|------|----------|
| **接口类型** | 根据接口名称前缀判断 (`ib*` → InfiniBand, `eth*/en*` → Ethernet, `veth*` → Virtual) |
| **链路速度** | 读取 `/sys/class/net/<iface>/speed` |
| **InfiniBand 速度** | 执行 `ibstat` 解析 `Rate` 字段 |
| **MTU** | Java `NetworkInterface.getMTU()` |
| **网络类型** | 检查 IP 是否为私有地址 (10.x, 172.16-31.x, 192.168.x) |
| **Driver↔Executor 延迟** | 测量 RDD 任务往返时间 |

**代码示例**:
```scala
private def getInterfaceSpeed(ifName: String): Option[String] = {
  val speedPath = s"/sys/class/net/$ifName/speed"
  val speedMbps = scala.io.Source.fromFile(speedPath).mkString.trim.toInt
  if (speedMbps >= 1000) {
    Some(f"${speedMbps / 1000}%d Gb/s")
  } else {
    Some(f"$speedMbps Mb/s")
  }
}
```

---

### 2.5 存储信息检测

**源文件**: `detectors/StorageDetector.scala`

| 指标 | 获取方式 |
|------|----------|
| **默认文件系统** | Hadoop `fs.defaultFS` 配置 |
| **存储类型** | 根据 URI 前缀 (`hdfs://`, `s3a://`, `file://` 等) |
| **HDFS 信息** | Hadoop `FileSystem.getStatus()` 获取容量/使用量 |
| **本地磁盘类型** | 读取 `/sys/block/<device>/queue/rotational` (0=SSD, 1=HDD) |
| **NVMe 检测** | 设备名以 `nvme` 开头 |
| **Spark 本地目录** | 读取 `spark.local.dir` 配置 |

**代码示例**:
```scala
private def detectStorageTypeForPath(path: String): String = {
  val rotational = new File(s"/sys/block/$dev/queue/rotational")
  val isRotational = scala.io.Source.fromFile(rotational).mkString.trim
  if (isRotational == "0") {
    if (dev.startsWith("nvme")) "NVMe SSD" else "SSD"
  } else {
    "HDD"
  }
}
```

---

### 2.6 软件版本检测

**源文件**: `detectors/SoftwareDetector.scala`

| 指标 | 获取方式 |
|------|----------|
| **Spark 版本** | `SparkSession.version` |
| **Scala 版本** | `util.Properties.versionNumberString` |
| **Java 版本** | `System.getProperty("java.version")` |
| **Hadoop 版本** | `org.apache.hadoop.util.VersionInfo.getVersion` |
| **Kernel 版本** | 执行 `uname -r` |
| **Linux 发行版** | 解析 `/etc/os-release` 的 `PRETTY_NAME` |
| **CUDA 版本** | 解析 `nvidia-smi` 输出或执行 `nvcc --version` |
| **cuDNN 版本** | 读取头文件 `/usr/include/cudnn_version.h` 的宏定义 |
| **NCCL 版本** | 读取头文件 `/usr/include/nccl.h` 的宏定义 |
| **NVIDIA 驱动版本** | `nvidia-smi --query-gpu=driver_version` |
| **nvidia-peermem** | 执行 `lsmod | grep nvidia_peermem` |
| **libcuda.so** | 检查常见路径或执行 `ldconfig -p | grep libcuda` |
| **GDS (GPUDirect Storage)** | 检查 `nvidia_fs` 模块和 `libcufile.so` |

**代码示例**:
```scala
private def getCudaVersion: Option[String] = {
  val output = "nvidia-smi".!!
  val cudaPattern = """CUDA Version: (\d+\.\d+)""".r
  cudaPattern.findFirstMatchIn(output).map(_.group(1))
}

private def checkNvidiaPeermem: Option[Boolean] = {
  val output = "lsmod".!!
  Some(output.contains("nvidia_peermem"))
}
```

---

## 3. 性能基准测试指标及实现

### 3.1 磁盘 I/O 测试

**源文件**: `benchmark/DiskBenchmark.scala`

| 指标 | 测试方法 |
|------|----------|
| **顺序写入 (MB/s)** | 使用 1MB buffer 循环写入 256MB 文件，调用 `sync()` 确保落盘 |
| **顺序读取 (MB/s)** | 使用 1MB buffer 顺序读取整个文件 |
| **随机读 IOPS** | 使用 4KB block 随机位置读取 2000 次 |
| **随机写 IOPS** | 使用 4KB block 随机位置写入 2000 次，调用 `sync()` |

**代码示例**:
```scala
private def testSequentialWrite(file: File, testFileSizeMB: Int): Double = {
  val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
  Random.nextBytes(buffer)
  val startTime = System.nanoTime()
  val fos = new FileOutputStream(file)
  for (_ <- 0 until testFileSizeMB) {
    fos.write(buffer)
  }
  fos.getFD.sync() // 确保数据写入磁盘
  val durationSec = (System.nanoTime() - startTime) / 1e9
  testFileSizeMB / durationSec  // MB/s
}
```

---

### 3.2 网络测试

**源文件**: `benchmark/NetworkBenchmark.scala`

| 指标 | 测试方法 |
|------|----------|
| **Broadcast 带宽** | Driver 广播数据 (64-128MB) 到所有 Executor，测量总时间 |
| **Shuffle 带宽** | 在 Executor 上生成数据，执行 `reduceByKey` 触发 all-to-all shuffle |
| **Executor↔Executor 带宽** | 每个 partition 发送数据到相邻 partition，通过 `groupByKey` 聚合 |
| **网络延迟** | 根据 shuffle 总时间 / (partitions × rounds) 估算 |

**代码示例**:
```scala
private def testShuffleBandwidth(sc: SparkContext, params: BenchmarkParams): (Double, Double) = {
  for (round <- 1 to params.shuffleRounds) {
    val rdd = sc.parallelize(1 to numPartitions, numPartitions)
      .flatMap { partId =>
        // 每个 partition 生成 4KB 的记录
        (0 until recordsPerPartition).iterator.map { i =>
          val key = random.nextInt(numPartitions)  // 随机 key 触发 shuffle
          val value = new Array[Byte](recordSize)
          (key, value)
        }
      }
    rdd.reduceByKey((a, b) => a).count()  // 执行 shuffle
  }
  // 计算: totalDataMB / durationSec
}
```

**动态参数配置**:
- `shuffleDataSizeGB`: 基于集群内存的 1/4，上限 100GB
- `numPartitions`: 等于集群总核心数
- `shuffleRounds`: 大集群 5 轮，小集群 2-3 轮

---

### 3.3 CPU 测试

**源文件**: `benchmark/CpuBenchmark.scala`

| 指标 | 测试方法 |
|------|----------|
| **单线程 GFLOPS** | 单线程执行 200 万次浮点运算 (乘加、除法、sqrt、sin/cos) |
| **多线程 GFLOPS** | 所有核心并行执行，计算总 GFLOPS |
| **CPU 利用率** | 启动压力线程，通过 `OperatingSystemMXBean.getSystemCpuLoad()` 采样 |
| **Spark 分布式测试** | 在所有 Executor 上并行执行浮点计算 |

**代码示例**:
```scala
private def runFloatPointTest(numThreads: Int): Double = {
  val threads = (0 until numThreads).map { _ =>
    new Thread(() => {
      for (_ <- 0 until iterationsPerThread) {
        // FMA 类似操作
        result += a * b + c
        result += a / b - c
        result += Math.sqrt(a * a + b * b)
        result += Math.sin(a) + Math.cos(b)
      }
    })
  }
  threads.foreach(_.start())
  threads.foreach(_.join())
  // 每次迭代约 8 次浮点运算
  val gflops = (totalOperations / durationSec) / 1e9
}
```

---

### 3.4 内存测试

**源文件**: `benchmark/MemoryBenchmark.scala`

| 指标 | 测试方法 |
|------|----------|
| **读取带宽 (GB/s)** | 64MB Long 数组顺序读取 3 次迭代 |
| **写入带宽 (GB/s)** | 64MB Long 数组顺序写入 3 次迭代 |
| **复制带宽 (GB/s)** | 使用 `System.arraycopy()` 复制 32MB 数组 |
| **内存利用率** | `(totalMemory - freeMemory) / totalMemory × 100%` |

**代码示例**:
```scala
private def testMemoryRead(): Double = {
  val buffer = new Array[Long](sizeBytes / 8)
  // 初始化和预热...
  val startTime = System.nanoTime()
  for (_ <- 0 until ITERATIONS) {
    var sum = 0L
    var i = 0
    while (i < buffer.length) {
      sum += buffer(i)
      i += 1
    }
  }
  val totalGB = (TEST_SIZE_MB.toLong * ITERATIONS) / 1024.0
  totalGB / durationSec
}
```

---

### 3.5 GPU 测试

**源文件**: `benchmark/GpuBenchmark.scala`

| 指标 | 测试方法 |
|------|----------|
| **计算性能 (TFLOPS)** | 根据 GPU 型号和计算能力估算理论峰值 |
| **显存带宽 (GB/s)** | 根据 GPU 规格估算 (如 A100: 2039 GB/s) |
| **GPU 利用率** | `nvidia-smi --query-gpu=utilization.gpu` |
| **显存使用** | `nvidia-smi --query-gpu=memory.used,memory.total` |

---

## 4. 架构图

```
EnvDetector.main()
     │
     ├── detectEnvironment(spark)
     │      ├── ClusterDeploymentDetector.detect()  → 部署模式
     │      ├── NodeInfoDetector.detect()           → 节点信息
     │      ├── NetworkDetector.detect()            → 网络配置
     │      ├── StorageDetector.detect()            → 存储配置
     │      ├── SoftwareDetector.detect()           → 软件版本
     │      ├── HardwareDetector.detect()           → 硬件信息
     │      ├── SparkConfigDetector.detect()        → Spark 配置
     │      └── ReadinessScoreCalculator.calculate() → GPU 加速就绪度评分
     │
     └── BenchmarkRunner.run() (if --benchmark)
            ├── DiskBenchmark.run()      → 磁盘 I/O
            ├── NetworkBenchmark.run()   → 网络带宽
            ├── CpuBenchmark.run()       → CPU GFLOPS
            ├── MemoryBenchmark.run()    → 内存带宽
            └── GpuBenchmark.run()       → GPU 性能
```

---

## 5. 输出格式

工具生成两个 JSON 报告：

1. **环境报告** (`env-report/part-00000`)
   - 集群部署信息、节点信息、硬件配置、软件版本、网络/存储配置、GPU 加速就绪度评分

2. **基准测试报告** (`env-report_benchmark/part-00000`)
   - 磁盘 IOPS、网络带宽、CPU GFLOPS、内存带宽、GPU 性能

所有数值保留 2 位小数，存储/内存大小自动转换为人类可读格式 (KB/MB/GB)。

---

## 6. 关键术语解释

### 6.1 DIMM 速度/数量

- **DIMM** (Dual In-line Memory Module): 物理内存条
- **速度**: 如 `DDR4 3200 MT/s` 表示每秒 32 亿次传输
- **数量**: 服务器上安装的内存条数量
- **重要性**: 影响内存带宽，多通道填充可提升性能

### 6.2 NUMA 节点数

- **NUMA** (Non-Uniform Memory Access): 非统一内存访问架构
- **含义**: 独立的 CPU + 本地内存组合的数量
- **重要性**: 
  - 本地内存访问: ~80ns
  - 远程内存访问: ~140ns (慢 70%+)
  - Executor 应绑定到单个 NUMA 节点以获得最佳性能

### 6.3 MTU (Maximum Transmission Unit)

- **含义**: 网络接口一次可发送的最大数据包大小
- **常见值**:
  - 1500 字节: 标准以太网
  - 9000 字节: Jumbo Frame (高性能数据中心)
  - 4096 字节: InfiniBand
- **重要性**: 更大 MTU = 更少数据包 = 更高吞吐量 (Shuffle 性能提升 15-25%)

### 6.4 isUp 和 isLoopback

- **isUp**: 网络接口是否启用/连接
- **isLoopback**: 是否为本地回环接口 (127.0.0.1)，用于本机进程间通信

