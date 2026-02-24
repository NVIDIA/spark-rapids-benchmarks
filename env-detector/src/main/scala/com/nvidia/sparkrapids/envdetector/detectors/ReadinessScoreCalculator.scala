package com.nvidia.sparkrapids.envdetector.detectors

import com.nvidia.sparkrapids.envdetector.model._

/**
 * Calculates GPU acceleration readiness score based on detected environment.
 */
object ReadinessScoreCalculator {

  /**
   * Calculate readiness score based on the environment report.
   */
  def calculate(report: EnvironmentReport): ReadinessScore = {
    val issues = scala.collection.mutable.ListBuffer[String]()
    val recommendations = scala.collection.mutable.ListBuffer[String]()
    
    // Calculate component scores
    val hardwareScore = calculateHardwareScore(report, issues, recommendations)
    val softwareScore = calculateSoftwareScore(report, issues, recommendations)
    val networkScore = calculateNetworkScore(report, issues, recommendations)
    val storageScore = calculateStorageScore(report, issues, recommendations)
    
    // Calculate overall score
    val overallScore = calculateOverallScore(hardwareScore, softwareScore, networkScore, storageScore)
    val overallDescription = getOverallDescription(overallScore, issues.size)
    
    ReadinessScore(
      overallScore = overallScore,
      overallDescription = overallDescription,
      hardwareScore = hardwareScore,
      softwareScore = softwareScore,
      networkScore = networkScore,
      storageScore = storageScore,
      issues = issues.toSeq,
      recommendations = recommendations.toSeq
    )
  }

  /**
   * Calculate hardware readiness score.
   */
  private def calculateHardwareScore(
    report: EnvironmentReport,
    issues: scala.collection.mutable.ListBuffer[String],
    recommendations: scala.collection.mutable.ListBuffer[String]
  ): String = {
    var score = 100
    
    // Check GPU availability
    val hasGpu = report.hardware.driverHardware.gpu.isDefined || 
                 report.hardware.executorHardware.exists(_.hardware.gpu.isDefined)
    
    if (!hasGpu) {
      score -= 50
      issues += "No GPU detected in the cluster"
      recommendations += "Install NVIDIA GPUs for GPU acceleration"
    } else {
      // Check GPU memory
      val gpuInfo = report.hardware.driverHardware.gpu.orElse(
        report.hardware.executorHardware.flatMap(_.hardware.gpu).headOption
      )
      
      gpuInfo.foreach { gpu =>
        val totalGpuMemoryGB = gpu.gpus.map(_.memoryTotal).sum / (1024.0 * 1024 * 1024)
        if (totalGpuMemoryGB < 16) {
          score -= 15
          issues += f"Low GPU memory: $totalGpuMemoryGB%.1f GB (recommend 16GB+)"
          recommendations += "Consider using GPUs with more memory (A100, L40, etc.)"
        }
        
        // Check for NVLink
        gpu.nvlinkTopology match {
          case Some(nvlink) if nvlink.nvlinkConnections.nonEmpty =>
            // NVLink is available - good
          case _ if gpu.gpuCount > 1 =>
            score -= 10
            issues += "Multi-GPU setup without NVLink interconnect"
            recommendations += "Consider NVLink-enabled GPUs for better multi-GPU performance"
          case _ =>
            // Single GPU, NVLink not applicable
        }
      }
    }
    
    // Check CPU cores
    val cpuCores = report.hardware.driverHardware.cpu.physicalCores
    if (cpuCores < 8) {
      score -= 10
      issues += s"Low CPU core count: $cpuCores cores"
      recommendations += "Consider using machines with more CPU cores"
    }
    
    // Check memory
    val totalMemoryGB = report.hardware.driverHardware.memory.totalPhysical / (1024.0 * 1024 * 1024)
    if (totalMemoryGB < 32) {
      score -= 10
      issues += f"Low system memory: $totalMemoryGB%.1f GB"
      recommendations += "Consider using machines with more RAM (64GB+)"
    }
    
    // Check NUMA
    report.hardware.driverHardware.memory.numaNodes match {
      case Some(nodes) if nodes > 1 =>
        // Multi-NUMA - recommend NUMA-aware configuration
        recommendations += "Multi-NUMA system detected - ensure NUMA-aware configuration"
      case _ =>
    }
    
    scoreToGrade(score)
  }

  /**
   * Calculate software readiness score.
   */
  private def calculateSoftwareScore(
    report: EnvironmentReport,
    issues: scala.collection.mutable.ListBuffer[String],
    recommendations: scala.collection.mutable.ListBuffer[String]
  ): String = {
    var score = 100
    val gpuSw = report.software.gpuSoftware
    
    // Check CUDA
    gpuSw.cudaVersion match {
      case Some(version) =>
        val majorVersion = version.split("\\.").headOption.map(_.toInt).getOrElse(0)
        if (majorVersion < 11) {
          score -= 20
          issues += s"CUDA version $version is outdated"
          recommendations += "Upgrade to CUDA 11.x or 12.x for better performance"
        }
      case None =>
        score -= 30
        issues += "CUDA not detected"
        recommendations += "Install CUDA toolkit for GPU acceleration"
    }
    
    // Check NVIDIA driver
    gpuSw.nvidiaDriverVersion match {
      case Some(version) =>
        val majorVersion = version.split("\\.").headOption.map(_.toInt).getOrElse(0)
        if (majorVersion < 450) {
          score -= 15
          issues += s"NVIDIA driver version $version may be outdated"
          recommendations += "Update NVIDIA driver to version 450+ for better compatibility"
        }
      case None if gpuSw.cudaVersion.isDefined =>
        score -= 15
        issues += "NVIDIA driver version could not be determined"
    }
    
    // Check libcuda.so
    gpuSw.libcudaPresent match {
      case Some(true) =>
        // libcuda.so is present - good
      case _ =>
        score -= 15
        issues += "libcuda.so not found in standard paths"
        recommendations += "Ensure NVIDIA driver is properly installed with libcuda.so"
    }
    
    // Check nvidia-peermem for GPUDirect
    gpuSw.nvidiaPeermemLoaded match {
      case Some(true) =>
        // nvidia-peermem loaded - good for GPUDirect
      case Some(false) =>
        recommendations += "Consider loading nvidia-peermem module for GPUDirect RDMA"
      case None =>
        // Could not check
    }
    
    // Check GDS
    gpuSw.gdsEnabled match {
      case Some(true) =>
        // GDS enabled - good
      case _ =>
        recommendations += "Consider enabling GPUDirect Storage (GDS) for faster I/O"
    }
    
    // Check spark-rapids plugin
    gpuSw.sparkRapidsVersion match {
      case Some(_) =>
        // spark-rapids is configured - good
      case None =>
        score -= 20
        issues += "spark-rapids plugin not configured"
        recommendations += "Install and configure spark-rapids plugin for GPU acceleration"
    }
    
    // Check Spark version compatibility
    val sparkVersion = report.software.sparkVersion
    val sparkMajorMinor = sparkVersion.split("\\.").take(2).mkString(".")
    if (sparkMajorMinor < "3.1") {
      score -= 10
      issues += s"Spark version $sparkVersion may have limited GPU support"
      recommendations += "Consider upgrading to Spark 3.1+ for better GPU support"
    }
    
    scoreToGrade(score)
  }

  /**
   * Calculate network readiness score.
   */
  private def calculateNetworkScore(
    report: EnvironmentReport,
    issues: scala.collection.mutable.ListBuffer[String],
    recommendations: scala.collection.mutable.ListBuffer[String]
  ): String = {
    var score = 100
    
    // Check for high-speed networking
    val hasHighSpeedNetwork = report.network.interfaces.exists { ni =>
      ni.interfaceType.exists(t => t == "InfiniBand" || t == "RoCE") ||
      ni.speed.exists(s => s.contains("100") || s.contains("200") || s.contains("400"))
    }
    
    if (!hasHighSpeedNetwork) {
      // Check for at least 10GbE
      val has10GbE = report.network.interfaces.exists { ni =>
        ni.speed.exists(s => {
          val speedNum = s.replaceAll("[^0-9]", "")
          val speedInt = scala.util.Try(speedNum.toInt).getOrElse(0)
          speedInt >= 10
        })
      }
      
      if (!has10GbE) {
        score -= 20
        issues += "No high-speed network (10GbE+) detected"
        recommendations += "Consider using 10GbE+ or InfiniBand for better shuffle performance"
      }
    }
    
    // Check MTU for jumbo frames
    val hasJumboFrames = report.network.interfaces.exists { ni =>
      ni.mtu.exists(_ >= 9000)
    }
    
    if (!hasJumboFrames && report.nodes.isMultiNode) {
      recommendations += "Consider enabling jumbo frames (MTU 9000) for better network throughput"
    }
    
    // Check for InfiniBand or RoCE
    val hasRdma = report.network.interfaces.exists { ni =>
      ni.interfaceType.exists(t => t == "InfiniBand" || t == "RoCE")
    }
    
    if (!hasRdma && report.nodes.totalNodes > 4) {
      recommendations += "Consider InfiniBand or RoCE for large cluster deployments"
    }
    
    scoreToGrade(score)
  }

  /**
   * Calculate storage readiness score.
   */
  private def calculateStorageScore(
    report: EnvironmentReport,
    issues: scala.collection.mutable.ListBuffer[String],
    recommendations: scala.collection.mutable.ListBuffer[String]
  ): String = {
    var score = 100
    
    // Check Spark local directories
    val sparkLocalDirs = report.storage.sparkLocalDirs
    if (sparkLocalDirs.nonEmpty) {
      // Check if all local dirs are accessible
      val inaccessibleDirs = sparkLocalDirs.filter(!_.isAccessible)
      if (inaccessibleDirs.nonEmpty) {
        score -= 15
        issues += s"${inaccessibleDirs.size} Spark local directories are not accessible"
        recommendations += "Ensure all spark.local.dir paths are accessible and writable"
      }
      
      // Check storage type for shuffle paths
      val hasNvme = sparkLocalDirs.exists(_.storageType.contains("NVMe"))
      val hasSsd = sparkLocalDirs.exists(_.storageType.contains("SSD"))
      val hasHdd = sparkLocalDirs.exists(_.storageType == "HDD")
      
      if (hasHdd && !hasSsd && !hasNvme) {
        score -= 20
        issues += "Spark local directories are on HDD storage"
        recommendations += "Use NVMe SSD for spark.local.dir for better shuffle performance"
      } else if (!hasNvme && hasSsd) {
        recommendations += "Consider using NVMe SSDs for optimal shuffle performance"
      }
      
      // Check available space
      val minFreeSpaceGB = sparkLocalDirs.map(_.freeSpace).min / (1024.0 * 1024 * 1024)
      if (minFreeSpaceGB < 50) {
        score -= 10
        issues += f"Low free space in Spark local directories: $minFreeSpaceGB%.1f GB"
        recommendations += "Ensure at least 100GB free space for shuffle data"
      }
    }
    
    // Check HDFS if used
    report.storage.hdfsInfo.foreach { hdfs =>
      val usagePercent = hdfs.usedCapacity.toDouble / hdfs.totalCapacity * 100
      if (usagePercent > 85) {
        score -= 10
        issues += f"HDFS usage is high: $usagePercent%.1f%%"
        recommendations += "Consider adding more HDFS storage or cleaning up old data"
      }
    }
    
    scoreToGrade(score)
  }

  /**
   * Calculate overall score from component scores.
   */
  private def calculateOverallScore(
    hardwareScore: String,
    softwareScore: String,
    networkScore: String,
    storageScore: String
  ): String = {
    val scores = Seq(hardwareScore, softwareScore, networkScore, storageScore)
    
    // If any component is Red, overall is Red
    if (scores.contains("Red")) {
      "Red"
    } else if (scores.contains("Yellow")) {
      "Yellow"
    } else {
      "Green"
    }
  }

  /**
   * Get overall description based on score and issues.
   */
  private def getOverallDescription(score: String, issueCount: Int): String = {
    score match {
      case "Green" =>
        if (issueCount == 0) {
          "Excellent - Environment is fully ready for GPU-accelerated Spark workloads"
        } else {
          s"Good - Environment is ready with $issueCount minor consideration(s)"
        }
      case "Yellow" =>
        s"Fair - Environment has $issueCount issue(s) that may impact performance"
      case "Red" =>
        s"Poor - Environment has $issueCount critical issue(s) that need to be addressed"
      case _ =>
        "Unknown"
    }
  }

  /**
   * Convert numeric score to grade (Green/Yellow/Red).
   */
  private def scoreToGrade(score: Int): String = {
    if (score >= 80) "Green"
    else if (score >= 50) "Yellow"
    else "Red"
  }
}

