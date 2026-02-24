package com.nvidia.sparkrapids.envdetector.report

import com.nvidia.sparkrapids.envdetector.model._
import com.nvidia.sparkrapids.envdetector.benchmark._
import com.google.gson.{Gson, GsonBuilder}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._

/**
 * Generates and outputs environment reports in various formats.
 */
object ReportGenerator {

  private val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
  
  /**
   * Convert Scala collections to Java collections for proper JSON serialization.
   */
  private def toJavaMap[K, V](map: Map[K, V]): java.util.Map[K, V] = map.asJava
  private def toJavaList[T](seq: Seq[T]): java.util.List[T] = seq.asJava
  
  /**
   * Print the environment report to console in a human-readable format.
   */
  def printReport(report: EnvironmentReport): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestamp = dateFormat.format(new Date(report.timestamp))
    
    println("=" * 80)
    println("SPARK RAPIDS ENVIRONMENT REPORT")
    println("=" * 80)
    println(s"Generated: $timestamp")
    println()
    
    printDeploymentInfo(report.deployment)
    printNodeInfo(report.nodes)
    printHardwareInfo(report.hardware)
    printSoftwareInfo(report.software)
    printStorageInfo(report.storage)
    printNetworkInfo(report.network)
    printSparkConfigInfo(report.sparkConfig)
    
    // Print readiness score
    report.readinessScore.foreach(printReadinessScore)
    
    println("=" * 80)
    println("END OF REPORT")
    println("=" * 80)
  }

  /**
   * Print GPU acceleration readiness score.
   */
  private def printReadinessScore(score: ReadinessScore): Unit = {
    println("-" * 80)
    println("8. GPU ACCELERATION READINESS SCORE")
    println("-" * 80)
    
    val overallColor = score.overallScore match {
      case "Green" => Console.GREEN
      case "Yellow" => Console.YELLOW
      case "Red" => Console.RED
      case _ => Console.WHITE
    }
    
    println(s"  Overall Score:  $overallColor${score.overallScore}${Console.RESET}")
    println(s"  Description:    ${score.overallDescription}")
    println()
    println("  Component Scores:")
    println(s"    Hardware:     ${colorScore(score.hardwareScore)}")
    println(s"    Software:     ${colorScore(score.softwareScore)}")
    println(s"    Network:      ${colorScore(score.networkScore)}")
    println(s"    Storage:      ${colorScore(score.storageScore)}")
    println()
    
    if (score.issues.nonEmpty) {
      println("  Issues Found:")
      score.issues.foreach(issue => println(s"    [!] $issue"))
      println()
    }
    
    if (score.recommendations.nonEmpty) {
      println("  Recommendations:")
      score.recommendations.foreach(rec => println(s"    -> $rec"))
      println()
    }
  }

  /**
   * Add color to score display.
   */
  private def colorScore(score: String): String = {
    val color = score match {
      case "Green" => Console.GREEN
      case "Yellow" => Console.YELLOW
      case "Red" => Console.RED
      case _ => Console.WHITE
    }
    s"$color$score${Console.RESET}"
  }

  /**
   * Print deployment information.
   */
  private def printDeploymentInfo(info: DeploymentInfo): Unit = {
    println("-" * 80)
    println("1. CLUSTER DEPLOYMENT")
    println("-" * 80)
    println(s"  Mode:         ${info.mode}")
    println(s"  Master URL:   ${info.masterUrl}")
    println(s"  Deploy Mode:  ${info.deployMode}")
    println(s"  Description:  ${info.description}")
    println()
  }

  /**
   * Print node information.
   */
  private def printNodeInfo(info: NodeInfo): Unit = {
    println("-" * 80)
    println("2. CLUSTER NODES")
    println("-" * 80)
    println(s"  Total Nodes:     ${info.totalNodes}")
    println(s"  Multi-Node:      ${info.isMultiNode}")
    println()
    println("  Driver Node:")
    println(s"    Hostname:      ${info.driverNode.hostname}")
    println(s"    IP Address:    ${info.driverNode.ipAddress}")
    println()
    
    if (info.executorNodes.nonEmpty) {
      println("  Executor Nodes:")
      info.executorNodes.foreach { node =>
        println(s"    - Executor ${node.executorId.getOrElse("N/A")}: ${node.hostname} (${node.ipAddress})")
      }
    } else {
      println("  Executor Nodes:  No dedicated executors (local mode or not yet allocated)")
    }
    println()
  }

  /**
   * Print hardware information.
   */
  private def printHardwareInfo(info: HardwareInfo): Unit = {
    println("-" * 80)
    println("3. HARDWARE CONFIGURATION")
    println("-" * 80)
    
    println("  Driver Hardware:")
    printHardwareDetails(info.driverHardware, "    ")
    
    if (info.executorHardware.nonEmpty) {
      println()
      println("  Executor Hardware:")
      info.executorHardware.foreach { exec =>
        println(s"    Executor ${exec.executorId} (${exec.hostname}):")
        printHardwareDetails(exec.hardware, "      ")
      }
    }
    println()
  }

  /**
   * Print hardware details with indentation.
   */
  private def printHardwareDetails(details: HardwareDetails, indent: String): Unit = {
    println(s"${indent}CPU:")
    println(s"$indent  Model:          ${details.cpu.model}")
    println(s"$indent  Physical Cores: ${details.cpu.physicalCores}")
    println(s"$indent  Logical Cores:  ${details.cpu.logicalCores}")
    println(s"$indent  Hyper-Threading: ${if (details.cpu.hyperThreadingEnabled) "Enabled" else "Disabled"}")
    println(s"$indent  Sockets:        ${details.cpu.socketsCount}")
    println(s"$indent  Cores/Socket:   ${details.cpu.coresPerSocket}")
    println(s"$indent  Architecture:   ${details.cpu.architecture}")
    details.cpu.frequency.foreach(f => println(s"$indent  Frequency:      $f"))
    details.cpu.maxFrequency.foreach(f => println(s"$indent  Max Frequency:  $f"))
    
    println(s"${indent}Memory:")
    println(s"$indent  Total:          ${formatBytes(details.memory.totalPhysical)}")
    println(s"$indent  Free:           ${formatBytes(details.memory.freePhysical)}")
    println(s"$indent  JVM Max Heap:   ${formatBytes(details.memory.jvmMaxHeap)}")
    details.memory.dimmSpeed.foreach(s => println(s"$indent  DIMM Speed:     $s"))
    details.memory.dimmCount.foreach(c => println(s"$indent  DIMM Count:     $c"))
    details.memory.numaNodes.foreach(n => println(s"$indent  NUMA Nodes:     $n"))
    
    details.gpu match {
      case Some(gpuInfo) =>
        println(s"${indent}GPU (${gpuInfo.gpuCount} device(s)):")
        gpuInfo.gpus.foreach { gpu =>
          println(s"$indent  [${gpu.index}] ${gpu.name}")
          println(s"$indent      Memory:      ${formatBytes(gpu.memoryTotal)} total, ${formatBytes(gpu.memoryFree)} free")
          println(s"$indent      Compute Cap: ${gpu.computeCapability}")
          gpu.temperature.foreach(t => println(s"$indent      Temperature: ${t}°C"))
          gpu.utilization.foreach(u => println(s"$indent      Utilization: $u%"))
        }
        // Print NVLink topology if available
        gpuInfo.nvlinkTopology.foreach { nvlink =>
          println(s"${indent}NVLink Topology:")
          nvlink.nvlinkVersion.foreach(v => println(s"$indent  Version:     NVLink $v"))
          nvlink.nvlinkBandwidthGBps.foreach(b => println(f"$indent  Bandwidth:   $b%.1f GB/s (per link)"))
          if (nvlink.nvlinkConnections.nonEmpty) {
            println(s"$indent  Connections:")
            nvlink.nvlinkConnections.foreach { conn =>
              println(s"$indent    GPU${conn.gpu0} <-> GPU${conn.gpu1}: ${conn.linkCount} link(s)")
            }
          }
        }
      case None =>
        println(s"${indent}GPU: Not detected or not available")
    }
  }

  /**
   * Print software information.
   */
  private def printSoftwareInfo(info: SoftwareInfo): Unit = {
    println("-" * 80)
    println("4. SOFTWARE CONFIGURATION")
    println("-" * 80)
    println(s"  Spark Version:   ${info.sparkVersion}")
    println(s"  Scala Version:   ${info.scalaVersion}")
    println(s"  Java Version:    ${info.javaVersion}")
    info.hadoopVersion.foreach(v => println(s"  Hadoop Version:  $v"))
    println()
    println("  Operating System:")
    println(s"    Name:          ${info.osInfo.name}")
    println(s"    Version:       ${info.osInfo.version}")
    println(s"    Architecture:  ${info.osInfo.arch}")
    info.osInfo.kernelVersion.foreach(v => println(s"    Kernel:        $v"))
    info.osInfo.distribution.foreach(v => println(s"    Distribution:  $v"))
    println()
    println("  GPU Software Stack:")
    info.gpuSoftware.nvidiaDriverVersion.foreach(v => println(s"    NVIDIA Driver:   $v"))
    info.gpuSoftware.cudaVersion.foreach(v => println(s"    CUDA:            $v"))
    info.gpuSoftware.cudnnVersion.foreach(v => println(s"    cuDNN:           $v"))
    info.gpuSoftware.ncclVersion.foreach(v => println(s"    NCCL:            $v"))
    info.gpuSoftware.sparkRapidsVersion.foreach(v => println(s"    spark-rapids:    $v"))
    info.gpuSoftware.libcudaPresent.foreach { present =>
      val status = if (present) "Yes" else "No"
      println(s"    libcuda.so:      $status")
    }
    info.gpuSoftware.libcudaPath.foreach(v => println(s"    libcuda Path:    $v"))
    info.gpuSoftware.nvidiaPeermemLoaded.foreach { loaded =>
      val status = if (loaded) "Loaded" else "Not Loaded"
      println(s"    nvidia-peermem:  $status")
    }
    info.gpuSoftware.gdsEnabled.foreach { enabled =>
      val status = if (enabled) "Enabled" else "Not Enabled"
      println(s"    GPUDirect (GDS): $status")
    }
    if (info.gpuSoftware.cudaVersion.isEmpty && 
        info.gpuSoftware.nvidiaDriverVersion.isEmpty) {
      println("    (No GPU software detected)")
    }
    println()
  }

  /**
   * Print storage information.
   */
  private def printStorageInfo(info: StorageInfo): Unit = {
    println("-" * 80)
    println("5. STORAGE CONFIGURATION")
    println("-" * 80)
    println(s"  Default FS:     ${info.defaultFileSystem}")
    println(s"  Storage Types:  ${info.storageTypes.mkString(", ")}")
    println()
    
    info.hdfsInfo.foreach { hdfs =>
      println("  HDFS:")
      println(s"    NameNode:     ${hdfs.nameNodeUrl}")
      println(s"    Capacity:     ${formatBytes(hdfs.totalCapacity)}")
      println(s"    Used:         ${formatBytes(hdfs.usedCapacity)}")
      println(s"    Available:    ${formatBytes(hdfs.availableCapacity)}")
      println(s"    Block Size:   ${formatBytes(hdfs.blockSize)}")
      println(s"    Replication:  ${hdfs.replication}")
      println()
    }
    
    // Print Spark local directories (shuffle paths)
    if (info.sparkLocalDirs.nonEmpty) {
      println("  Spark Local Directories (Shuffle Paths):")
      info.sparkLocalDirs.groupBy(_.hostname).foreach { case (hostname, dirs) =>
        println(s"    $hostname:")
        dirs.foreach { dir =>
          val status = if (dir.isAccessible) "OK" else "INACCESSIBLE"
          println(s"      ${dir.path}: ${formatBytes(dir.freeSpace)} free (${dir.storageType}) [$status]")
        }
      }
      println()
    }
    
    if (info.localStorageInfo.nonEmpty) {
      println("  Local Storage:")
      info.localStorageInfo.foreach { nodeStorage =>
        println(s"    ${nodeStorage.hostname}:")
        nodeStorage.mountPoints.foreach { mount =>
          println(s"      ${mount.path}: ${formatBytes(mount.usableSpace)} available of ${formatBytes(mount.totalSpace)} (${mount.storageType})")
        }
      }
    }
    println()
  }

  /**
   * Print network information.
   */
  private def printNetworkInfo(info: NetworkInfo): Unit = {
    println("-" * 80)
    println("6. NETWORK CONFIGURATION")
    println("-" * 80)
    println(s"  Network Type:   ${info.networkType}")
    info.bandwidth.foreach(b => println(s"  Bandwidth:      $b"))
    info.latency.foreach(l => println(s"  Avg Latency:    $l"))
    println()
    
    if (info.driverToExecutorLatency.nonEmpty) {
      println("  Driver to Executor Latency:")
      info.driverToExecutorLatency.foreach { case (execId, latency) =>
        println(s"    Executor $execId: ${latency}ms")
      }
      println()
    }
    
    val nonLoopbackInterfaces = info.interfaces.filter(!_.isLoopback)
    if (nonLoopbackInterfaces.nonEmpty) {
      println("  Network Interfaces:")
      nonLoopbackInterfaces.foreach { ni =>
        val status = if (ni.isUp) "UP" else "DOWN"
        val typeStr = ni.interfaceType.map(t => s", Type: $t").getOrElse("")
        val speedStr = ni.speed.map(s => s", Speed: $s").getOrElse("")
        val mtuStr = ni.mtu.map(m => s", MTU: $m").getOrElse("")
        println(s"    ${ni.name}: ${ni.ipAddress} [$status$typeStr$speedStr$mtuStr]")
      }
    }
    println()
  }

  /**
   * Print Spark configuration information.
   */
  private def printSparkConfigInfo(info: SparkConfigInfo): Unit = {
    println("-" * 80)
    println("7. SPARK CONFIGURATION SUMMARY")
    println("-" * 80)
    println(s"  Executors:          ${info.executorCount}")
    println(s"  Executor Cores:     ${info.executorCores}")
    println(s"  Executor Memory:    ${info.executorMemory}")
    println(s"  Driver Memory:      ${info.driverMemory}")
    println(s"  Shuffle Partitions: ${info.shufflePartitions}")
    println()
    println(s"  Dynamic Allocation: ${if (info.dynamicAllocationEnabled) "Enabled" else "Disabled"}")
    println(s"  Adaptive Execution: ${if (info.adaptiveExecutionEnabled) "Enabled" else "Disabled"}")
    println(s"  RAPIDS Enabled:     ${if (info.rapidsEnabled) "Yes" else "No"}")
    println()
    
    // Print SPARK_HOME and SPARK_CONF_DIR
    info.sparkHome.foreach(v => println(s"  SPARK_HOME:         $v"))
    info.sparkConfDir.foreach(v => println(s"  SPARK_CONF_DIR:     $v"))
    println()
    
    // Print spark-defaults.conf if available
    if (info.sparkDefaultsConf.nonEmpty) {
      println("  spark-defaults.conf:")
      info.sparkDefaultsConf.toSeq.sortBy(_._1).foreach { case (key, value) =>
        println(s"    $key = $value")
      }
      println()
    }
    
    if (info.importantConfigs.nonEmpty) {
      println("  Runtime Configurations:")
      info.importantConfigs.toSeq.sortBy(_._1).foreach { case (key, value) =>
        println(s"    $key = $value")
      }
    }
    println()
  }

  /**
   * Save report to a file (JSON format).
   * If output path already exists, appends timestamp suffix to avoid overwriting.
   */
  def saveReport(report: EnvironmentReport, outputPath: String, spark: SparkSession): Unit = {
    val json = toJson(report)
    
    // Check if output path exists, if so, add timestamp suffix
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(outputPath), hadoopConf)
    val originalPath = new org.apache.hadoop.fs.Path(outputPath)
    
    val finalPath = if (fs.exists(originalPath)) {
      val timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date())
      val newPath = s"${outputPath}_$timestamp"
      println(s"    Output path exists, saving to: $newPath")
      newPath
    } else {
      outputPath
    }
    
    // Use Spark to write (works with HDFS, S3, local, etc.)
    import spark.implicits._
    spark.sparkContext.parallelize(Seq(json), 1)
      .saveAsTextFile(finalPath)
    
    println(s"    Report saved to: $finalPath")
  }

  /**
   * Convert report to JSON string.
   */
  def toJson(report: EnvironmentReport): String = {
    val javaReport = convertEnvironmentReportToJava(report)
    gson.toJson(javaReport)
  }

  /**
   * Convert EnvironmentReport to Java-friendly structure for proper JSON serialization.
   */
  private def convertEnvironmentReportToJava(report: EnvironmentReport): java.util.Map[String, Any] = {
    val result = new java.util.LinkedHashMap[String, Any]()
    result.put("timestamp", report.timestamp)
    
    // Deployment
    val deployment = new java.util.LinkedHashMap[String, Any]()
    deployment.put("mode", report.deployment.mode)
    deployment.put("masterUrl", report.deployment.masterUrl)
    deployment.put("deployMode", report.deployment.deployMode)
    deployment.put("description", report.deployment.description)
    result.put("deployment", deployment)
    
    // Nodes
    val nodes = new java.util.LinkedHashMap[String, Any]()
    nodes.put("totalNodes", report.nodes.totalNodes)
    nodes.put("isMultiNode", report.nodes.isMultiNode)
    val driverNode = new java.util.LinkedHashMap[String, Any]()
    driverNode.put("hostname", report.nodes.driverNode.hostname)
    driverNode.put("ipAddress", report.nodes.driverNode.ipAddress)
    driverNode.put("nodeType", report.nodes.driverNode.nodeType)
    report.nodes.driverNode.executorId.foreach(id => driverNode.put("executorId", id))
    nodes.put("driverNode", driverNode)
    nodes.put("executorNodes", report.nodes.executorNodes.map { n =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("hostname", n.hostname)
      m.put("ipAddress", n.ipAddress)
      m.put("nodeType", n.nodeType)
      n.executorId.foreach(id => m.put("executorId", id))
      m
    }.asJava)
    result.put("nodes", nodes)
    
    // Network
    val network = new java.util.LinkedHashMap[String, Any]()
    network.put("networkType", report.network.networkType)
    report.network.bandwidth.foreach(b => network.put("bandwidth", b))
    report.network.latency.foreach(l => network.put("latency", l))
    network.put("interfaces", report.network.interfaces.map { ni =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("name", ni.name)
      m.put("ipAddress", ni.ipAddress)
      m.put("isUp", ni.isUp)
      m.put("isLoopback", ni.isLoopback)
      ni.mtu.foreach(v => m.put("mtu", v))
      ni.interfaceType.foreach(v => m.put("interfaceType", v))
      ni.speed.foreach(v => m.put("speed", v))
      ni.macAddress.foreach(v => m.put("macAddress", v))
      m
    }.asJava)
    network.put("driverToExecutorLatency", report.network.driverToExecutorLatency.map { case (k, v) => k -> Long.box(v) }.asJava)
    result.put("network", network)
    
    // Storage
    val storage = new java.util.LinkedHashMap[String, Any]()
    storage.put("storageTypes", report.storage.storageTypes.asJava)
    storage.put("defaultFileSystem", report.storage.defaultFileSystem)
    report.storage.hdfsInfo.foreach { hdfs =>
      val h = new java.util.LinkedHashMap[String, Any]()
      h.put("nameNodeUrl", hdfs.nameNodeUrl)
      h.put("totalCapacity", formatBytes(hdfs.totalCapacity))
      h.put("usedCapacity", formatBytes(hdfs.usedCapacity))
      h.put("availableCapacity", formatBytes(hdfs.availableCapacity))
      h.put("blockSize", formatBytes(hdfs.blockSize))
      h.put("replication", hdfs.replication)
      storage.put("hdfsInfo", h)
    }
    storage.put("localStorageInfo", report.storage.localStorageInfo.map { ls =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("hostname", ls.hostname)
      m.put("mountPoints", ls.mountPoints.map { mp =>
        val p = new java.util.LinkedHashMap[String, Any]()
        p.put("path", mp.path)
        p.put("totalSpace", formatBytes(mp.totalSpace))
        p.put("freeSpace", formatBytes(mp.freeSpace))
        p.put("usableSpace", formatBytes(mp.usableSpace))
        p.put("storageType", mp.storageType)
        p
      }.asJava)
      m
    }.asJava)
    storage.put("sparkLocalDirs", report.storage.sparkLocalDirs.map { d =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("hostname", d.hostname)
      m.put("path", d.path)
      m.put("totalSpace", formatBytes(d.totalSpace))
      m.put("freeSpace", formatBytes(d.freeSpace))
      m.put("storageType", d.storageType)
      m.put("isAccessible", d.isAccessible)
      m
    }.asJava)
    result.put("storage", storage)
    
    // Software
    val software = new java.util.LinkedHashMap[String, Any]()
    software.put("sparkVersion", report.software.sparkVersion)
    software.put("scalaVersion", report.software.scalaVersion)
    software.put("javaVersion", report.software.javaVersion)
    report.software.hadoopVersion.foreach(v => software.put("hadoopVersion", v))
    val osInfo = new java.util.LinkedHashMap[String, Any]()
    osInfo.put("name", report.software.osInfo.name)
    osInfo.put("version", report.software.osInfo.version)
    osInfo.put("arch", report.software.osInfo.arch)
    report.software.osInfo.kernelVersion.foreach(v => osInfo.put("kernelVersion", v))
    report.software.osInfo.distribution.foreach(v => osInfo.put("distribution", v))
    software.put("osInfo", osInfo)
    val gpuSw = new java.util.LinkedHashMap[String, Any]()
    report.software.gpuSoftware.cudaVersion.foreach(v => gpuSw.put("cudaVersion", v))
    report.software.gpuSoftware.cudnnVersion.foreach(v => gpuSw.put("cudnnVersion", v))
    report.software.gpuSoftware.ncclVersion.foreach(v => gpuSw.put("ncclVersion", v))
    report.software.gpuSoftware.nvidiaDriverVersion.foreach(v => gpuSw.put("nvidiaDriverVersion", v))
    report.software.gpuSoftware.sparkRapidsVersion.foreach(v => gpuSw.put("sparkRapidsVersion", v))
    report.software.gpuSoftware.nvidiaPeermemLoaded.foreach(v => gpuSw.put("nvidiaPeermemLoaded", v))
    report.software.gpuSoftware.libcudaPresent.foreach(v => gpuSw.put("libcudaPresent", v))
    report.software.gpuSoftware.libcudaPath.foreach(v => gpuSw.put("libcudaPath", v))
    report.software.gpuSoftware.gdsEnabled.foreach(v => gpuSw.put("gdsEnabled", v))
    software.put("gpuSoftware", gpuSw)
    result.put("software", software)
    
    // Hardware
    val hardware = new java.util.LinkedHashMap[String, Any]()
    hardware.put("driverHardware", convertHardwareDetailsToJava(report.hardware.driverHardware))
    hardware.put("executorHardware", report.hardware.executorHardware.map { eh =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("executorId", eh.executorId)
      m.put("hostname", eh.hostname)
      m.put("hardware", convertHardwareDetailsToJava(eh.hardware))
      m
    }.asJava)
    result.put("hardware", hardware)
    
    // SparkConfig
    val sparkConfig = new java.util.LinkedHashMap[String, Any]()
    sparkConfig.put("executorCount", report.sparkConfig.executorCount)
    sparkConfig.put("executorCores", report.sparkConfig.executorCores)
    sparkConfig.put("executorMemory", report.sparkConfig.executorMemory)
    sparkConfig.put("driverMemory", report.sparkConfig.driverMemory)
    sparkConfig.put("shufflePartitions", report.sparkConfig.shufflePartitions)
    sparkConfig.put("dynamicAllocationEnabled", report.sparkConfig.dynamicAllocationEnabled)
    sparkConfig.put("adaptiveExecutionEnabled", report.sparkConfig.adaptiveExecutionEnabled)
    sparkConfig.put("rapidsEnabled", report.sparkConfig.rapidsEnabled)
    sparkConfig.put("importantConfigs", report.sparkConfig.importantConfigs.asJava)
    sparkConfig.put("sparkDefaultsConf", report.sparkConfig.sparkDefaultsConf.asJava)
    report.sparkConfig.sparkHome.foreach(v => sparkConfig.put("sparkHome", v))
    report.sparkConfig.sparkConfDir.foreach(v => sparkConfig.put("sparkConfDir", v))
    result.put("sparkConfig", sparkConfig)
    
    // Readiness Score
    report.readinessScore.foreach { rs =>
      val score = new java.util.LinkedHashMap[String, Any]()
      score.put("overallScore", rs.overallScore)
      score.put("overallDescription", rs.overallDescription)
      score.put("hardwareScore", rs.hardwareScore)
      score.put("softwareScore", rs.softwareScore)
      score.put("networkScore", rs.networkScore)
      score.put("storageScore", rs.storageScore)
      score.put("issues", rs.issues.asJava)
      score.put("recommendations", rs.recommendations.asJava)
      result.put("readinessScore", score)
    }
    
    result
  }

  /**
   * Convert HardwareDetails to Java-friendly structure.
   */
  private def convertHardwareDetailsToJava(details: HardwareDetails): java.util.Map[String, Any] = {
    val result = new java.util.LinkedHashMap[String, Any]()
    
    // CPU
    val cpu = new java.util.LinkedHashMap[String, Any]()
    cpu.put("model", details.cpu.model)
    cpu.put("physicalCores", details.cpu.physicalCores)
    cpu.put("logicalCores", details.cpu.logicalCores)
    cpu.put("hyperThreadingEnabled", details.cpu.hyperThreadingEnabled)
    cpu.put("socketsCount", details.cpu.socketsCount)
    cpu.put("coresPerSocket", details.cpu.coresPerSocket)
    cpu.put("architecture", details.cpu.architecture)
    details.cpu.frequency.foreach(v => cpu.put("frequency", v))
    details.cpu.maxFrequency.foreach(v => cpu.put("maxFrequency", v))
    result.put("cpu", cpu)
    
    // Memory
    val memory = new java.util.LinkedHashMap[String, Any]()
    memory.put("totalPhysical", formatBytes(details.memory.totalPhysical))
    memory.put("freePhysical", formatBytes(details.memory.freePhysical))
    memory.put("jvmMaxHeap", formatBytes(details.memory.jvmMaxHeap))
    memory.put("jvmTotalHeap", formatBytes(details.memory.jvmTotalHeap))
    memory.put("jvmFreeHeap", formatBytes(details.memory.jvmFreeHeap))
    details.memory.dimmSpeed.foreach(v => memory.put("dimmSpeed", v))
    details.memory.dimmCount.foreach(v => memory.put("dimmCount", v))
    details.memory.numaNodes.foreach(v => memory.put("numaNodes", v))
    result.put("memory", memory)
    
    // GPU
    details.gpu.foreach { gpuInfo =>
      val gpu = new java.util.LinkedHashMap[String, Any]()
      gpu.put("gpuCount", gpuInfo.gpuCount)
      gpu.put("gpus", gpuInfo.gpus.map { g =>
        val gm = new java.util.LinkedHashMap[String, Any]()
        gm.put("index", g.index)
        gm.put("name", g.name)
        gm.put("memoryTotal", formatBytes(g.memoryTotal))
        gm.put("memoryFree", formatBytes(g.memoryFree))
        gm.put("computeCapability", g.computeCapability)
        g.temperature.foreach(v => gm.put("temperature", s"${v}°C"))
        g.utilization.foreach(v => gm.put("utilization", s"${v}%"))
        gm
      }.asJava)
      gpuInfo.nvlinkTopology.foreach { nvlink =>
        val nv = new java.util.LinkedHashMap[String, Any]()
        nvlink.nvlinkVersion.foreach(v => nv.put("nvlinkVersion", v))
        nv.put("nvlinkConnections", nvlink.nvlinkConnections.map { c =>
          val cm = new java.util.LinkedHashMap[String, Any]()
          cm.put("gpu0", c.gpu0)
          cm.put("gpu1", c.gpu1)
          cm.put("linkCount", c.linkCount)
          c.linkBandwidthGBps.foreach(v => cm.put("linkBandwidthGBps", v))
          cm
        }.asJava)
        nvlink.nvlinkBandwidthGBps.foreach(v => nv.put("nvlinkBandwidthGBps", v))
        gpu.put("nvlinkTopology", nv)
      }
      result.put("gpu", gpu)
    }
    
    result
  }

  /**
   * Format bytes to human-readable format.
   */
  private def formatBytes(bytes: Long): String = {
    if (bytes <= 0) return "0 B"
    
    val units = Array("B", "KB", "MB", "GB", "TB", "PB")
    val digitGroups = (Math.log10(bytes.toDouble) / Math.log10(1024)).toInt
    val index = Math.min(digitGroups, units.length - 1)
    
    f"${bytes / Math.pow(1024, index)}%.2f ${units(index)}"
  }

  /**
   * Round a double to 2 decimal places.
   */
  private def round2(value: Double): Double = {
    math.round(value * 100.0) / 100.0
  }

  // ==================== BENCHMARK REPORT ====================

  /**
   * Print the benchmark report to console.
   */
  def printBenchmarkReport(report: BenchmarkReport): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestamp = dateFormat.format(new Date(report.timestamp))
    
    println("=" * 80)
    println("PERFORMANCE BENCHMARK REPORT")
    println("=" * 80)
    println(s"Generated: $timestamp")
    println()
    
    printDiskBenchmark(report.diskBenchmark)
    printNetworkBenchmark(report.networkBenchmark)
    printCpuBenchmark(report.cpuBenchmark)
    printMemoryBenchmark(report.memoryBenchmark)
    report.gpuBenchmark.foreach(printGpuBenchmark)
    
    // Print performance summary
    printPerformanceSummary(report)
    
    println("=" * 80)
    println("END OF BENCHMARK REPORT")
    println("=" * 80)
  }

  private def printDiskBenchmark(result: DiskBenchmarkResult): Unit = {
    println("-" * 80)
    println("DISK I/O PERFORMANCE (Local - spark.local.dir)")
    println("-" * 80)
    println(f"  Sequential Read:        ${result.sequentialReadMBps}%.2f MB/s")
    println(f"  Sequential Write:       ${result.sequentialWriteMBps}%.2f MB/s")
    println(f"  Random Read:            ${result.randomReadMBps}%.2f MB/s")
    println(f"  Random Write:           ${result.randomWriteMBps}%.2f MB/s")
    println(f"  Test Path:              ${result.testPath}")
    println(f"  Test Size:              ${formatBytes((result.testSizeMB.toLong * 1024 * 1024))}")
    println()
    
    // HDFS performance (if available)
    if (result.hdfsReadBandwidthMBps.isDefined || result.hdfsWriteBandwidthMBps.isDefined) {
      println("-" * 80)
      println("HDFS I/O PERFORMANCE (Spark Scan/Write)")
      println("-" * 80)
      result.hdfsReadBandwidthMBps.foreach(bw => println(f"  HDFS Read:              $bw%.2f MB/s"))
      result.hdfsWriteBandwidthMBps.foreach(bw => println(f"  HDFS Write:             $bw%.2f MB/s"))
      result.hdfsTestPath.foreach(path => println(f"  Test Path:              $path"))
      println()
    }
    
    if (result.executorResults.nonEmpty) {
      println("  Per-Executor Local Disk Results:")
      result.executorResults.foreach { exec =>
        println(f"    Executor ${exec.executorId} (${exec.hostname}):")
        println(f"      Seq Read: ${exec.sequentialReadMBps}%.2f MB/s, Seq Write: ${exec.sequentialWriteMBps}%.2f MB/s")
        println(f"      Rand Read: ${exec.randomReadMBps}%.2f MB/s, Rand Write: ${exec.randomWriteMBps}%.2f MB/s")
      }
    }
    println()
  }

  private def printNetworkBenchmark(result: NetworkBenchmarkResult): Unit = {
    println("-" * 80)
    println("NETWORK PERFORMANCE")
    println("-" * 80)
    println(f"  Shuffle Bandwidth:    ${result.shuffleBandwidthMBps}%.2f MB/s")
    println(f"  Average Latency:      ${result.avgLatencyMs}%.2f ms")
    println(f"  Test Data Size:       ${result.testDataSizeMB} MB")
    println()
  }

  private def printCpuBenchmark(result: CpuBenchmarkResult): Unit = {
    println("-" * 80)
    println("CPU PERFORMANCE")
    println("-" * 80)
    println(f"  Driver Single-Thread: ${result.driverResult.singleThreadGflops}%.2f GFLOPS")
    println(f"  Driver Multi-Thread:  ${result.driverResult.multiThreadGflops}%.2f GFLOPS (${result.driverResult.cores} cores)")
    println(f"  Spark Distributed:    ${result.sparkCpuTestGflops}%.2f GFLOPS")
    println(f"  Peak CPU Utilization: ${result.driverResult.utilizationPercent}%.1f%%")
    println()
    
    if (result.executorResults.nonEmpty) {
      println("  Per-Executor Results:")
      result.executorResults.foreach { exec =>
        println(f"    Executor ${exec.executorId} (${exec.hostname}):")
        println(f"      Single: ${exec.result.singleThreadGflops}%.2f GFLOPS, Multi: ${exec.result.multiThreadGflops}%.2f GFLOPS")
        println(f"      Cores: ${exec.result.cores}, Utilization: ${exec.result.utilizationPercent}%.1f%%")
      }
    }
    println()
  }

  private def printMemoryBenchmark(result: MemoryBenchmarkResult): Unit = {
    println("-" * 80)
    println("MEMORY PERFORMANCE")
    println("-" * 80)
    println(f"  Read Bandwidth:       ${result.driverResult.readBandwidthGBps}%.2f GB/s")
    println(f"  Write Bandwidth:      ${result.driverResult.writeBandwidthGBps}%.2f GB/s")
    println(f"  Copy Bandwidth:       ${result.driverResult.copyBandwidthGBps}%.2f GB/s")
    println(f"  Spark Distributed:    ${result.sparkMemoryTestGBps}%.2f GB/s")
    println(f"  Total Memory:         ${result.driverResult.totalMemoryGB}%.2f GB")
    println(f"  Memory Utilization:   ${result.driverResult.utilizationPercent}%.1f%%")
    println()
    
    if (result.executorResults.nonEmpty) {
      println("  Per-Executor Results:")
      result.executorResults.foreach { exec =>
        println(f"    Executor ${exec.executorId} (${exec.hostname}):")
        println(f"      Read: ${exec.result.readBandwidthGBps}%.2f GB/s, Write: ${exec.result.writeBandwidthGBps}%.2f GB/s")
        println(f"      Memory: ${exec.result.totalMemoryGB}%.2f GB, Utilization: ${exec.result.utilizationPercent}%.1f%%")
      }
    }
    println()
  }

  private def printGpuBenchmark(result: GpuBenchmarkResult): Unit = {
    println("-" * 80)
    println("GPU PERFORMANCE")
    println("-" * 80)
    
    result.driverResult.foreach { gpu =>
      println(s"  Driver GPU: ${gpu.gpuName}")
      println(f"    Compute:            ${gpu.computeTflops}%.2f TFLOPS (estimated)")
      println(f"    Memory Bandwidth:   ${gpu.memoryBandwidthGBps}%.2f GB/s (estimated)")
      println(f"    Memory Used:        ${gpu.memoryUsedGB}%.2f / ${gpu.memoryTotalGB}%.2f GB")
      println(f"    Utilization:        ${gpu.utilizationPercent}%.1f%%")
    }
    
    if (result.executorResults.nonEmpty) {
      println()
      println("  Executor GPUs:")
      result.executorResults.foreach { exec =>
        exec.results.foreach { gpu =>
          println(s"    Executor ${exec.executorId} (${exec.hostname}) - ${gpu.gpuName}:")
          println(f"      Compute: ${gpu.computeTflops}%.2f TFLOPS, Mem BW: ${gpu.memoryBandwidthGBps}%.2f GB/s")
        }
      }
    }
    println()
  }

  private def printPerformanceSummary(report: BenchmarkReport): Unit = {
    println("-" * 80)
    println("PERFORMANCE SUMMARY & RECOMMENDATIONS")
    println("-" * 80)
    
    val issues = scala.collection.mutable.ListBuffer[String]()
    val recommendations = scala.collection.mutable.ListBuffer[String]()
    
    // Check disk performance
    if (report.diskBenchmark.sequentialReadMBps < 100) {
      issues += "Disk read performance is low (< 100 MB/s)"
      recommendations += "Consider using faster storage (SSD/NVMe) or check for I/O bottlenecks"
    }
    if (report.diskBenchmark.randomReadMBps < 50) {
      issues += "Random I/O throughput is low (< 50 MB/s for 4MB blocks)"
      recommendations += "Use SSD/NVMe storage for better random I/O performance"
    }
    
    // Check network performance
    if (report.networkBenchmark.shuffleBandwidthMBps < 100) {
      issues += "Shuffle bandwidth is low (< 100 MB/s)"
      recommendations += "Check network configuration, consider using faster network (10GbE+)"
    }
    if (report.networkBenchmark.avgLatencyMs > 10) {
      issues += "Network latency is high (> 10ms)"
      recommendations += "Check for network congestion or consider co-locating nodes"
    }
    
    // Check CPU performance
    val expectedCpuPerf = report.cpuBenchmark.driverResult.cores * 2.0 // ~2 GFLOPS per core expected
    if (report.cpuBenchmark.driverResult.multiThreadGflops < expectedCpuPerf * 0.5) {
      issues += "CPU performance is below expected"
      recommendations += "Check for CPU throttling, power settings, or competing workloads"
    }
    
    // Check memory performance
    if (report.memoryBenchmark.driverResult.readBandwidthGBps < 10) {
      issues += "Memory bandwidth is low (< 10 GB/s)"
      recommendations += "Check memory configuration and NUMA settings"
    }
    
    // Check GPU if available
    report.gpuBenchmark match {
      case Some(gpu) =>
        gpu.driverResult match {
          case Some(g) if g.computeTflops >= 1 =>
            // GPU is working well
          case Some(g) =>
            issues += s"GPU compute performance is low (${g.computeTflops} TFLOPS)"
            recommendations += "Ensure CUDA is properly installed and GPU is accessible"
          case None =>
            issues += "GPU not detected on driver node"
            recommendations += "Check nvidia-smi availability and GPU drivers"
        }
      case None =>
        // GPU benchmark was skipped or not available
    }
    
    if (issues.isEmpty) {
      println("  [OK] All performance metrics are within expected ranges")
    } else {
      println("  Potential Issues Found:")
      issues.foreach(i => println(s"    [!] $i"))
      println()
      println("  Recommendations:")
      recommendations.distinct.foreach(r => println(s"    -> $r"))
    }
    println()
  }

  /**
   * Convert BenchmarkReport to Java-friendly structure for proper JSON serialization.
   */
  private def convertBenchmarkReportToJava(report: BenchmarkReport): java.util.Map[String, Any] = {
    val result = new java.util.LinkedHashMap[String, Any]()
    result.put("timestamp", report.timestamp)
    
    // Disk benchmark
    val disk = new java.util.LinkedHashMap[String, Any]()
    disk.put("sequentialReadMBps", round2(report.diskBenchmark.sequentialReadMBps))
    disk.put("sequentialWriteMBps", round2(report.diskBenchmark.sequentialWriteMBps))
    disk.put("randomReadMBps", round2(report.diskBenchmark.randomReadMBps))
    disk.put("randomWriteMBps", round2(report.diskBenchmark.randomWriteMBps))
    disk.put("testPath", report.diskBenchmark.testPath)
    disk.put("testSizeMB", report.diskBenchmark.testSizeMB)
    // HDFS performance metrics
    report.diskBenchmark.hdfsReadBandwidthMBps.foreach(bw => disk.put("hdfsReadBandwidthMBps", round2(bw)))
    report.diskBenchmark.hdfsWriteBandwidthMBps.foreach(bw => disk.put("hdfsWriteBandwidthMBps", round2(bw)))
    report.diskBenchmark.hdfsTestPath.foreach(path => disk.put("hdfsTestPath", path))
    disk.put("executorResults", report.diskBenchmark.executorResults.map { e =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("executorId", e.executorId)
      m.put("hostname", e.hostname)
      m.put("sequentialReadMBps", round2(e.sequentialReadMBps))
      m.put("sequentialWriteMBps", round2(e.sequentialWriteMBps))
      m.put("randomReadMBps", round2(e.randomReadMBps))
      m.put("randomWriteMBps", round2(e.randomWriteMBps))
      m
    }.asJava)
    result.put("diskBenchmark", disk)
    
    // Network benchmark
    val network = new java.util.LinkedHashMap[String, Any]()
    network.put("shuffleBandwidthMBps", round2(report.networkBenchmark.shuffleBandwidthMBps))
    network.put("avgLatencyMs", round2(report.networkBenchmark.avgLatencyMs))
    network.put("testDataSizeMB", report.networkBenchmark.testDataSizeMB)
    result.put("networkBenchmark", network)
    
    // CPU benchmark
    val cpu = new java.util.LinkedHashMap[String, Any]()
    val cpuDriver = new java.util.LinkedHashMap[String, Any]()
    cpuDriver.put("singleThreadGflops", round2(report.cpuBenchmark.driverResult.singleThreadGflops))
    cpuDriver.put("multiThreadGflops", round2(report.cpuBenchmark.driverResult.multiThreadGflops))
    cpuDriver.put("cores", report.cpuBenchmark.driverResult.cores)
    cpuDriver.put("utilizationPercent", round2(report.cpuBenchmark.driverResult.utilizationPercent))
    cpu.put("driverResult", cpuDriver)
    cpu.put("executorResults", report.cpuBenchmark.executorResults.map { e =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("executorId", e.executorId)
      m.put("hostname", e.hostname)
      m.put("singleThreadGflops", round2(e.result.singleThreadGflops))
      m.put("multiThreadGflops", round2(e.result.multiThreadGflops))
      m.put("cores", e.result.cores)
      m.put("utilizationPercent", round2(e.result.utilizationPercent))
      m
    }.asJava)
    cpu.put("sparkCpuTestGflops", round2(report.cpuBenchmark.sparkCpuTestGflops))
    result.put("cpuBenchmark", cpu)
    
    // Memory benchmark
    val memory = new java.util.LinkedHashMap[String, Any]()
    val memDriver = new java.util.LinkedHashMap[String, Any]()
    memDriver.put("readBandwidthGBps", round2(report.memoryBenchmark.driverResult.readBandwidthGBps))
    memDriver.put("writeBandwidthGBps", round2(report.memoryBenchmark.driverResult.writeBandwidthGBps))
    memDriver.put("copyBandwidthGBps", round2(report.memoryBenchmark.driverResult.copyBandwidthGBps))
    memDriver.put("totalMemoryGB", round2(report.memoryBenchmark.driverResult.totalMemoryGB))
    memDriver.put("usedMemoryGB", round2(report.memoryBenchmark.driverResult.usedMemoryGB))
    memDriver.put("utilizationPercent", round2(report.memoryBenchmark.driverResult.utilizationPercent))
    memory.put("driverResult", memDriver)
    memory.put("executorResults", report.memoryBenchmark.executorResults.map { e =>
      val m = new java.util.LinkedHashMap[String, Any]()
      m.put("executorId", e.executorId)
      m.put("hostname", e.hostname)
      m.put("readBandwidthGBps", round2(e.result.readBandwidthGBps))
      m.put("writeBandwidthGBps", round2(e.result.writeBandwidthGBps))
      m.put("copyBandwidthGBps", round2(e.result.copyBandwidthGBps))
      m.put("totalMemoryGB", round2(e.result.totalMemoryGB))
      m.put("usedMemoryGB", round2(e.result.usedMemoryGB))
      m.put("utilizationPercent", round2(e.result.utilizationPercent))
      m
    }.asJava)
    memory.put("sparkMemoryTestGBps", round2(report.memoryBenchmark.sparkMemoryTestGBps))
    result.put("memoryBenchmark", memory)
    
    // GPU benchmark
    report.gpuBenchmark.foreach { gpu =>
      val gpuMap = new java.util.LinkedHashMap[String, Any]()
      gpu.driverResult.foreach { g =>
        val driverGpu = new java.util.LinkedHashMap[String, Any]()
        driverGpu.put("gpuIndex", g.gpuIndex)
        driverGpu.put("gpuName", g.gpuName)
        driverGpu.put("computeTflops", round2(g.computeTflops))
        driverGpu.put("memoryBandwidthGBps", round2(g.memoryBandwidthGBps))
        driverGpu.put("utilizationPercent", round2(g.utilizationPercent))
        driverGpu.put("memoryUsedGB", round2(g.memoryUsedGB))
        driverGpu.put("memoryTotalGB", round2(g.memoryTotalGB))
        gpuMap.put("driverResult", driverGpu)
      }
      gpuMap.put("executorResults", gpu.executorResults.map { e =>
        val m = new java.util.LinkedHashMap[String, Any]()
        m.put("executorId", e.executorId)
        m.put("hostname", e.hostname)
        m.put("gpuResults", e.results.map { g =>
          val gm = new java.util.LinkedHashMap[String, Any]()
          gm.put("gpuIndex", g.gpuIndex)
          gm.put("gpuName", g.gpuName)
          gm.put("computeTflops", round2(g.computeTflops))
          gm.put("memoryBandwidthGBps", round2(g.memoryBandwidthGBps))
          gm.put("utilizationPercent", round2(g.utilizationPercent))
          gm.put("memoryUsedGB", round2(g.memoryUsedGB))
          gm.put("memoryTotalGB", round2(g.memoryTotalGB))
          gm
        }.asJava)
        m
      }.asJava)
      result.put("gpuBenchmark", gpuMap)
    }
    
    result
  }

  /**
   * Save benchmark report to a file (JSON format).
   * If output path already exists, appends timestamp suffix to avoid overwriting.
   */
  def saveBenchmarkReport(report: BenchmarkReport, outputPath: String, spark: SparkSession): Unit = {
    val javaReport = convertBenchmarkReportToJava(report)
    val json = gson.toJson(javaReport)
    
    // Check if output path exists, if so, add timestamp suffix
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(outputPath), hadoopConf)
    val originalPath = new org.apache.hadoop.fs.Path(outputPath)
    
    val finalPath = if (fs.exists(originalPath)) {
      val timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date())
      val newPath = s"${outputPath}_$timestamp"
      println(s"    Output path exists, saving benchmark to: $newPath")
      newPath
    } else {
      outputPath
    }
    
    import spark.implicits._
    spark.sparkContext.parallelize(Seq(json), 1)
      .saveAsTextFile(finalPath)
    
    println(s"    Benchmark report saved to: $finalPath")
  }
}

