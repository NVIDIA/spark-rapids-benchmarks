package com.nvidia.sparkrapids.envdetector.detectors

import com.nvidia.sparkrapids.envdetector.model._
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Detects hardware configuration across the cluster.
 */
object HardwareDetector {

  def detect(sc: SparkContext): HardwareInfo = {
    // Get driver hardware info
    val driverHardware = getLocalHardwareDetails

    // Get executor hardware info
    val executorHardware = getExecutorHardwareInfo(sc)

    HardwareInfo(
      driverHardware = driverHardware,
      executorHardware = executorHardware
    )
  }

  /**
   * Get hardware details of all executors.
   */
  private def getExecutorHardwareInfo(sc: SparkContext): Seq[ExecutorHardwareInfo] = {
    val numPartitions = math.max(sc.defaultParallelism * 2, 20)
    
    val executorInfo = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val sparkEnv = org.apache.spark.SparkEnv.get
        val executorId = if (sparkEnv != null) sparkEnv.executorId else "unknown"
        val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
        val hardware = getLocalHardwareDetails
        
        Iterator((executorId, hostname, hardware))
      }
      .collect()
      .toSeq
      .groupBy(_._1).map(_._2.head).toSeq
      .filter(_._1 != "driver")

    executorInfo.map { case (execId, hostname, hardware) =>
      ExecutorHardwareInfo(
        executorId = execId,
        hostname = hostname,
        hardware = hardware
      )
    }
  }

  /**
   * Get hardware details of the local node.
   */
  private def getLocalHardwareDetails: HardwareDetails = {
    HardwareDetails(
      cpu = getCpuInfo,
      memory = getMemoryInfo,
      gpu = getGpuInfo
    )
  }

  /**
   * Get CPU information with physical cores, logical cores, and hyper-threading status.
   */
  private def getCpuInfo: CpuInfo = {
    val logicalCores = Runtime.getRuntime.availableProcessors()

    // Try to get detailed CPU info from /proc/cpuinfo (Linux)
    val cpuDetails = getCpuDetailsFromProc.getOrElse {
      // Fallback for non-Linux systems
      getCpuDetailsFromSysctl.getOrElse {
        CpuDetails(
          model = "Unknown",
          physicalCores = logicalCores,
          logicalCores = logicalCores,
          socketsCount = 1,
          coresPerSocket = logicalCores,
          hyperThreadingEnabled = false,
          frequency = None,
          maxFrequency = None
        )
      }
    }

    CpuInfo(
      model = cpuDetails.model,
      physicalCores = cpuDetails.physicalCores,
      logicalCores = cpuDetails.logicalCores,
      hyperThreadingEnabled = cpuDetails.hyperThreadingEnabled,
      socketsCount = cpuDetails.socketsCount,
      coresPerSocket = cpuDetails.coresPerSocket,
      architecture = System.getProperty("os.arch"),
      frequency = cpuDetails.frequency,
      maxFrequency = cpuDetails.maxFrequency
    )
  }

  /**
   * Helper case class for CPU details.
   */
  private case class CpuDetails(
    model: String,
    physicalCores: Int,
    logicalCores: Int,
    socketsCount: Int,
    coresPerSocket: Int,
    hyperThreadingEnabled: Boolean,
    frequency: Option[String],
    maxFrequency: Option[String]
  )

  /**
   * Get CPU details from /proc/cpuinfo (Linux).
   */
  private def getCpuDetailsFromProc: Option[CpuDetails] = {
    Try {
      val cpuInfo = scala.io.Source.fromFile("/proc/cpuinfo").mkString
      
      val modelPattern = """model name\s*:\s*(.+)""".r
      val freqPattern = """cpu MHz\s*:\s*([\d.]+)""".r
      val physicalIdPattern = """physical id\s*:\s*(\d+)""".r
      val coresPattern = """cpu cores\s*:\s*(\d+)""".r
      val processorPattern = """processor\s*:\s*(\d+)""".r
      
      val model = modelPattern.findFirstMatchIn(cpuInfo)
        .map(_.group(1).trim)
        .getOrElse("Unknown")
      
      val frequency = freqPattern.findFirstMatchIn(cpuInfo)
        .map(m => f"${m.group(1).toDouble / 1000}%.2f GHz")
      
      // Count unique physical IDs (sockets)
      val physicalIds = physicalIdPattern.findAllMatchIn(cpuInfo)
        .map(_.group(1).toInt)
        .toSet
      val socketsCount = if (physicalIds.nonEmpty) physicalIds.size else 1
      
      // Get cores per socket
      val coresPerSocket = coresPattern.findFirstMatchIn(cpuInfo)
        .map(_.group(1).toInt)
        .getOrElse(1)
      
      // Count total logical processors
      val logicalCores = processorPattern.findAllMatchIn(cpuInfo).size
      
      // Calculate physical cores
      val physicalCores = socketsCount * coresPerSocket
      
      // Determine hyper-threading status
      val hyperThreadingEnabled = logicalCores > physicalCores
      
      // Try to get max frequency from /sys/devices/system/cpu
      val maxFrequency = getMaxCpuFrequency
      
      CpuDetails(
        model = model,
        physicalCores = physicalCores,
        logicalCores = logicalCores,
        socketsCount = socketsCount,
        coresPerSocket = coresPerSocket,
        hyperThreadingEnabled = hyperThreadingEnabled,
        frequency = frequency,
        maxFrequency = maxFrequency
      )
    }.toOption
  }

  /**
   * Get CPU details from sysctl (macOS).
   */
  private def getCpuDetailsFromSysctl: Option[CpuDetails] = {
    Try {
      import scala.sys.process._
      
      val model = Try("sysctl -n machdep.cpu.brand_string".!!.trim).getOrElse("Unknown")
      val physicalCores = Try("sysctl -n hw.physicalcpu".!!.trim.toInt).getOrElse(1)
      val logicalCores = Try("sysctl -n hw.logicalcpu".!!.trim.toInt).getOrElse(physicalCores)
      val hyperThreadingEnabled = logicalCores > physicalCores
      
      CpuDetails(
        model = model,
        physicalCores = physicalCores,
        logicalCores = logicalCores,
        socketsCount = 1,
        coresPerSocket = physicalCores,
        hyperThreadingEnabled = hyperThreadingEnabled,
        frequency = None,
        maxFrequency = None
      )
    }.toOption
  }

  /**
   * Get max CPU frequency from /sys/devices/system/cpu.
   */
  private def getMaxCpuFrequency: Option[String] = {
    Try {
      val maxFreqPath = "/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq"
      val maxFreqKhz = scala.io.Source.fromFile(maxFreqPath).mkString.trim.toLong
      Some(f"${maxFreqKhz / 1000000.0}%.2f GHz")
    }.toOption.flatten
  }

  /**
   * Get memory information including DIMM speed.
   */
  private def getMemoryInfo: MemoryInfo = {
    val runtime = Runtime.getRuntime

    // Try to get physical memory info
    val totalPhysical = getTotalPhysicalMemory
    val freePhysical = getFreePhysicalMemory
    
    // Get DIMM speed and count
    val (dimmSpeed, dimmCount) = getDimmInfo
    
    // Get NUMA info
    val numaNodes = getNumaNodes

    MemoryInfo(
      totalPhysical = totalPhysical,
      freePhysical = freePhysical,
      jvmMaxHeap = runtime.maxMemory(),
      jvmTotalHeap = runtime.totalMemory(),
      jvmFreeHeap = runtime.freeMemory(),
      dimmSpeed = dimmSpeed,
      dimmCount = dimmCount,
      numaNodes = numaNodes
    )
  }

  /**
   * Get DIMM speed and count using multiple methods.
   * Tries methods that don't require root first, then falls back to sudo commands.
   */
  private def getDimmInfo: (Option[String], Option[Int]) = {
    // Method 1: Try dmidecode without sudo first (some systems allow it)
    val dimmFromDmidecodeNoSudo = Try {
      import scala.sys.process._
      val output = "dmidecode -t memory 2>/dev/null".!!
      parseDmidecodeOutput(output)
    }.toOption.flatten
    
    if (dimmFromDmidecodeNoSudo.exists(_._1.isDefined)) {
      return dimmFromDmidecodeNoSudo.get
    }
    
    // Method 2: Try reading from /sys/devices/system/edac (memory controller info)
    val dimmFromEdac = getDimmInfoFromEdac
    if (dimmFromEdac._1.isDefined || dimmFromEdac._2.isDefined) {
      return dimmFromEdac
    }
    
    // Method 3: Try decode-dimms command (from i2c-tools, doesn't require root on some systems)
    val dimmFromDecodeDimms = Try {
      import scala.sys.process._
      val output = "decode-dimms 2>/dev/null".!!
      val speedPattern = """Maximum module speed.*?(\d+)\s*MT/s""".r
      val ddrPattern = """Fundamental Memory type.*?(DDR\d+)""".r
      
      val speeds = speedPattern.findAllMatchIn(output).map(_.group(1) + " MT/s").toSeq.distinct
      val ddrTypes = ddrPattern.findAllMatchIn(output).map(_.group(1)).toSeq.distinct
      val dimmCount = output.split("Decoding DIMM").length - 1
      
      val speed = if (ddrTypes.nonEmpty && speeds.nonEmpty) {
        Some(s"${ddrTypes.head} ${speeds.head}")
      } else if (speeds.nonEmpty) {
        Some(speeds.head)
      } else {
        None
      }
      
      (speed, if (dimmCount > 0) Some(dimmCount) else None)
    }.toOption.getOrElse((None, None))
    
    if (dimmFromDecodeDimms._1.isDefined) {
      return dimmFromDecodeDimms
    }
    
    // Method 4: Try dmidecode with sudo (requires root)
    val dimmFromDmidecode = Try {
      import scala.sys.process._
      val output = "sudo dmidecode -t memory 2>/dev/null".!!
      parseDmidecodeOutput(output)
    }.toOption.flatten
    
    if (dimmFromDmidecode.exists(_._1.isDefined)) {
      return dimmFromDmidecode.get
    }
    
    // Method 5: Try lshw as fallback (may require root)
    Try {
      import scala.sys.process._
      val output = "sudo lshw -class memory -short 2>/dev/null".!!
      val dimmCount = output.split("\n").count(_.contains("DIMM"))
      (None, if (dimmCount > 0) Some(dimmCount) else None)
    }.toOption.getOrElse((None, None))
  }
  
  /**
   * Parse dmidecode memory output.
   */
  private def parseDmidecodeOutput(output: String): Option[(Option[String], Option[Int])] = {
    if (output.isEmpty) return None
    
    val speedPattern = """Speed:\s+(\d+\s*MT/s|\d+\s*MHz)""".r
    val typePattern = """Type:\s+(DDR\d+)""".r
    
    val speeds = speedPattern.findAllMatchIn(output).map(_.group(1)).toSeq.distinct.filter(_ != "Unknown")
    val types = typePattern.findAllMatchIn(output).map(_.group(1)).toSeq.distinct.filter(_ != "Unknown")
    
    val speed = if (types.nonEmpty && speeds.nonEmpty) {
      Some(s"${types.head} ${speeds.head}")
    } else if (speeds.nonEmpty) {
      Some(speeds.head)
    } else {
      None
    }
    
    // Count DIMMs with actual memory (Size: not "No Module Installed")
    val sizePattern = """Size:\s+(\d+\s*[MG]B)""".r
    val dimmCount = sizePattern.findAllMatchIn(output).size
    
    Some((speed, if (dimmCount > 0) Some(dimmCount) else None))
  }
  
  /**
   * Try to get DIMM info from EDAC (Error Detection And Correction) sysfs.
   */
  private def getDimmInfoFromEdac: (Option[String], Option[Int]) = {
    Try {
      val edacDir = new java.io.File("/sys/devices/system/edac/mc")
      if (!edacDir.exists) return (None, None)
      
      // Count memory controllers and DIMMs
      val mcDirs = edacDir.listFiles.filter(_.getName.startsWith("mc"))
      var totalDimms = 0
      var dimmTypes = scala.collection.mutable.Set[String]()
      
      mcDirs.foreach { mcDir =>
        // Count DIMM directories (dimm0, dimm1, etc. or csrow0, csrow1, etc.)
        val dimmDirs = mcDir.listFiles.filter { f =>
          f.getName.startsWith("dimm") || f.getName.startsWith("csrow")
        }
        totalDimms += dimmDirs.length
        
        // Try to read DIMM type
        dimmDirs.foreach { dimmDir =>
          Try {
            val typeFile = new java.io.File(dimmDir, "dimm_mem_type")
            if (typeFile.exists) {
              val source = scala.io.Source.fromFile(typeFile)
              try {
                dimmTypes += source.mkString.trim
              } finally {
                source.close()
              }
            }
          }
        }
      }
      
      val dimmType = if (dimmTypes.nonEmpty) Some(dimmTypes.mkString(", ")) else None
      val dimmCount = if (totalDimms > 0) Some(totalDimms) else None
      
      (dimmType, dimmCount)
    }.getOrElse((None, None))
  }

  /**
   * Get number of NUMA nodes.
   */
  private def getNumaNodes: Option[Int] = {
    Try {
      import scala.sys.process._
      // Try numactl first
      val output = "numactl --hardware 2>/dev/null".!!
      val pattern = """available:\s+(\d+)\s+nodes""".r
      pattern.findFirstMatchIn(output).map(_.group(1).toInt)
    }.toOption.flatten.orElse {
      // Fallback: count directories in /sys/devices/system/node
      Try {
        val nodeDir = new java.io.File("/sys/devices/system/node")
        if (nodeDir.exists && nodeDir.isDirectory) {
          Some(nodeDir.listFiles.count(_.getName.startsWith("node")))
        } else {
          None
        }
      }.toOption.flatten
    }
  }

  /**
   * Get total physical memory.
   */
  private def getTotalPhysicalMemory: Long = {
    // Try using Java ManagementFactory
    Try {
      val osBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[com.sun.management.OperatingSystemMXBean]
      osBean.getTotalPhysicalMemorySize
    }.getOrElse {
      // Fallback: read from /proc/meminfo
      Try {
        val memInfo = scala.io.Source.fromFile("/proc/meminfo").mkString
        val pattern = """MemTotal:\s+(\d+)\s+kB""".r
        pattern.findFirstMatchIn(memInfo).map(_.group(1).toLong * 1024).getOrElse(0L)
      }.getOrElse(0L)
    }
  }

  /**
   * Get free physical memory.
   */
  private def getFreePhysicalMemory: Long = {
    Try {
      val osBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[com.sun.management.OperatingSystemMXBean]
      osBean.getFreePhysicalMemorySize
    }.getOrElse {
      // Fallback: read from /proc/meminfo
      Try {
        val memInfo = scala.io.Source.fromFile("/proc/meminfo").mkString
        val pattern = """MemAvailable:\s+(\d+)\s+kB""".r
        pattern.findFirstMatchIn(memInfo).map(_.group(1).toLong * 1024).getOrElse(0L)
      }.getOrElse(0L)
    }
  }

  /**
   * Get GPU information using nvidia-smi including NVLink topology.
   */
  private def getGpuInfo: Option[GpuInfo] = {
    Try {
      import scala.sys.process._
      
      // Check if nvidia-smi is available
      val checkResult = "which nvidia-smi".!
      if (checkResult != 0) {
        return None
      }

      // Query GPU information
      val queryFields = "index,name,memory.total,memory.free,compute_cap,temperature.gpu,utilization.gpu"
      val output = s"nvidia-smi --query-gpu=$queryFields --format=csv,noheader,nounits".!!
      
      val gpus = output.trim.split("\n").map { line =>
        val fields = line.split(",").map(_.trim)
        GpuDevice(
          index = fields(0).toInt,
          name = fields(1),
          memoryTotal = fields(2).toLong * 1024 * 1024, // Convert MiB to bytes
          memoryFree = fields(3).toLong * 1024 * 1024,
          computeCapability = fields(4),
          temperature = Try(fields(5).toInt).toOption,
          utilization = Try(fields(6).toInt).toOption
        )
      }.toSeq

      if (gpus.nonEmpty) {
        // Get NVLink topology
        val nvlinkTopology = if (gpus.size > 1) getNvlinkTopology(gpus.size) else None
        
        Some(GpuInfo(
          gpuCount = gpus.size,
          gpus = gpus,
          nvlinkTopology = nvlinkTopology
        ))
      } else {
        None
      }
    }.toOption.flatten
  }

  /**
   * Get NVLink topology using nvidia-smi.
   */
  private def getNvlinkTopology(gpuCount: Int): Option[NvlinkTopology] = {
    Try {
      import scala.sys.process._
      
      // Get NVLink status for each GPU pair
      val connections = scala.collection.mutable.ListBuffer[NvlinkConnection]()
      
      // Try to get NVLink version from nvidia-smi
      val nvlinkVersion = Try {
        val output = "nvidia-smi nvlink -s".!!
        val versionPattern = """NVLink Version:\s*(\d+)""".r
        versionPattern.findFirstMatchIn(output).map(_.group(1))
      }.toOption.flatten
      
      // Parse nvidia-smi topo output
      val topoOutput = "nvidia-smi topo -m".!!
      
      // Parse the matrix to find NVLink connections
      val lines = topoOutput.split("\n").filter(_.trim.nonEmpty)
      
      // Find NVLink connections between GPUs
      for (i <- 0 until gpuCount; j <- (i + 1) until gpuCount) {
        val nvlinkCount = countNvlinksBetweenGpus(topoOutput, i, j)
        if (nvlinkCount > 0) {
          connections += NvlinkConnection(
            gpu0 = i,
            gpu1 = j,
            linkCount = nvlinkCount,
            linkBandwidthGBps = estimateNvlinkBandwidth(nvlinkVersion)
          )
        }
      }
      
      if (connections.nonEmpty) {
        // Estimate total bandwidth based on link count and version
        val totalBandwidth = connections.map { c =>
          c.linkBandwidthGBps.getOrElse(0.0) * c.linkCount
        }.sum / connections.size
        
        Some(NvlinkTopology(
          nvlinkVersion = nvlinkVersion,
          nvlinkConnections = connections.toSeq,
          nvlinkBandwidthGBps = if (totalBandwidth > 0) Some(totalBandwidth) else None
        ))
      } else {
        None
      }
    }.toOption.flatten
  }

  /**
   * Count NVLink connections between two GPUs from nvidia-smi topo output.
   */
  private def countNvlinksBetweenGpus(topoOutput: String, gpu0: Int, gpu1: Int): Int = {
    Try {
      val lines = topoOutput.split("\n")
      // Find the row for gpu0, then find the cell for gpu1
      // NVLink connections are shown as NV1, NV2, NV3, etc. or NVX
      lines.find(_.startsWith(s"GPU$gpu0")) match {
        case Some(line) =>
          val cells = line.split("\\s+")
          // Find the cell corresponding to gpu1
          val headers = lines.head.split("\\s+")
          val gpu1Index = headers.indexWhere(h => h == s"GPU$gpu1")
          if (gpu1Index > 0 && gpu1Index < cells.length) {
            val cell = cells(gpu1Index)
            // Parse NVX where X is the number of links
            if (cell.startsWith("NV")) {
              val numPattern = """NV(\d+)""".r
              numPattern.findFirstMatchIn(cell).map(_.group(1).toInt).getOrElse(0)
            } else {
              0
            }
          } else {
            0
          }
        case None => 0
      }
    }.getOrElse(0)
  }

  /**
   * Estimate NVLink bandwidth per link based on version.
   */
  private def estimateNvlinkBandwidth(version: Option[String]): Option[Double] = {
    version.map { v =>
      v match {
        case "1" => 20.0   // NVLink 1.0: ~20 GB/s per link
        case "2" => 25.0   // NVLink 2.0: ~25 GB/s per link
        case "3" => 50.0   // NVLink 3.0: ~50 GB/s per link
        case "4" => 100.0  // NVLink 4.0: ~100 GB/s per link
        case _ => 25.0     // Default estimate
      }
    }
  }
}
