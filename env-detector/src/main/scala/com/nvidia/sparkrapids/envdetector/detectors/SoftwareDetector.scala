/*
 * SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.sparkrapids.envdetector.detectors

import com.nvidia.sparkrapids.envdetector.model._
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Detects software versions across the cluster.
 */
object SoftwareDetector {

  def detect(spark: SparkSession): SoftwareInfo = {
    val sc = spark.sparkContext

    // Spark version
    val sparkVersion = spark.version

    // Scala version
    val scalaVersion = util.Properties.versionNumberString

    // Java version
    val javaVersion = System.getProperty("java.version")

    // Hadoop version
    val hadoopVersion = getHadoopVersion

    // OS information with kernel version
    val osInfo = getOsInfo

    // GPU software information (CUDA, cuDNN, NCCL, nvidia-peermem, libcuda.so, etc.)
    val gpuSoftwareInfo = getGpuSoftwareInfo(sc)

    SoftwareInfo(
      sparkVersion = sparkVersion,
      scalaVersion = scalaVersion,
      javaVersion = javaVersion,
      hadoopVersion = hadoopVersion,
      osInfo = osInfo,
      gpuSoftware = gpuSoftwareInfo
    )
  }

  /**
   * Get Hadoop version if available.
   */
  private def getHadoopVersion: Option[String] = {
    Try {
      org.apache.hadoop.util.VersionInfo.getVersion
    }.toOption
  }

  /**
   * Get operating system information including kernel version and distribution.
   */
  private def getOsInfo: OsInfo = {
    val kernelVersion = getKernelVersion
    val distribution = getLinuxDistribution
    
    OsInfo(
      name = System.getProperty("os.name"),
      version = System.getProperty("os.version"),
      arch = System.getProperty("os.arch"),
      kernelVersion = kernelVersion,
      distribution = distribution
    )
  }

  /**
   * Get kernel version using uname.
   */
  private def getKernelVersion: Option[String] = {
    Try {
      import scala.sys.process._
      val output = "uname -r".!!
      Some(output.trim)
    }.toOption.flatten.orElse {
      // Fallback to /proc/version
      Try {
        val content = scala.io.Source.fromFile("/proc/version").mkString
        val versionPattern = """Linux version (\S+)""".r
        versionPattern.findFirstMatchIn(content).map(_.group(1))
      }.toOption.flatten
    }
  }

  /**
   * Get Linux distribution name and version.
   */
  private def getLinuxDistribution: Option[String] = {
    // Try /etc/os-release first (standard)
    Try {
      val source = scala.io.Source.fromFile("/etc/os-release")
      try {
        val content = source.mkString
        val prettyNamePattern = """PRETTY_NAME="([^"]+)"""".r
        prettyNamePattern.findFirstMatchIn(content).map(_.group(1))
      } finally {
        source.close()
      }
    }.toOption.flatten.orElse {
      // Try /etc/lsb-release
      Try {
        val source = scala.io.Source.fromFile("/etc/lsb-release")
        try {
          val content = source.mkString
          val descPattern = """DISTRIB_DESCRIPTION="([^"]+)"""".r
          descPattern.findFirstMatchIn(content).map(_.group(1))
        } finally {
          source.close()
        }
      }.toOption.flatten
    }.orElse {
      // Try /etc/redhat-release
      Try {
        val source = scala.io.Source.fromFile("/etc/redhat-release")
        try {
          Some(source.mkString.trim)
        } finally {
          source.close()
        }
      }.toOption.flatten
    }.orElse {
      // Try lsb_release command
      Try {
        import scala.sys.process._
        val output = "lsb_release -d".!!
        val descPattern = """Description:\s+(.+)""".r
        descPattern.findFirstMatchIn(output).map(_.group(1).trim)
      }.toOption.flatten
    }
  }

  /**
   * Get GPU software information from executors.
   */
  private def getGpuSoftwareInfo(sc: org.apache.spark.SparkContext): GpuSoftwareInfo = {
    // Try to get GPU info from driver first
    val driverGpuInfo = detectLocalGpuSoftware

    // If driver doesn't have GPU, try to get from executors
    val gpuInfo = if (driverGpuInfo.cudaVersion.isDefined) {
      driverGpuInfo
    } else {
      Try {
        val numPartitions = math.max(sc.defaultParallelism, 4)
        val executorGpuInfo = sc.parallelize(1 to numPartitions, numPartitions)
          .mapPartitions { _ =>
            val info = detectLocalGpuSoftware
            if (info.cudaVersion.isDefined) Iterator(info) else Iterator.empty
          }
          .take(1)
        
        if (executorGpuInfo.nonEmpty) executorGpuInfo.head else driverGpuInfo
      }.getOrElse(driverGpuInfo)
    }

    // Check for spark-rapids plugin
    val sparkRapidsVersion = getSparkRapidsVersion(sc)

    gpuInfo.copy(sparkRapidsVersion = sparkRapidsVersion)
  }

  /**
   * Detect GPU software on the local node.
   */
  private def detectLocalGpuSoftware: GpuSoftwareInfo = {
    val (libcudaPresent, libcudaPath) = checkLibcuda
    val nvidiaPeermemLoaded = checkNvidiaPeermem
    val gdsEnabled = checkGdsEnabled
    
    GpuSoftwareInfo(
      cudaVersion = getCudaVersion,
      cudnnVersion = getCudnnVersion,
      ncclVersion = getNcclVersion,
      nvidiaDriverVersion = getNvidiaDriverVersion,
      sparkRapidsVersion = None,
      nvidiaPeermemLoaded = nvidiaPeermemLoaded,
      libcudaPresent = libcudaPresent,
      libcudaPath = libcudaPath,
      gdsEnabled = gdsEnabled
    )
  }

  /**
   * Check if nvidia-peermem kernel module is loaded.
   */
  private def checkNvidiaPeermem: Option[Boolean] = {
    Try {
      import scala.sys.process._
      // Check using lsmod
      val output = "lsmod".!!
      val isLoaded = output.contains("nvidia_peermem")
      Some(isLoaded)
    }.toOption.flatten.orElse {
      // Check in /sys/module
      Try {
        val peermemDir = new java.io.File("/sys/module/nvidia_peermem")
        Some(peermemDir.exists())
      }.toOption.flatten
    }
  }

  /**
   * Check if libcuda.so is present and find its path.
   */
  private def checkLibcuda: (Option[Boolean], Option[String]) = {
    // Common paths to check
    val commonPaths = Seq(
      "/usr/lib/x86_64-linux-gnu/libcuda.so",
      "/usr/lib64/libcuda.so",
      "/usr/local/cuda/lib64/libcuda.so",
      "/usr/lib/libcuda.so"
    )
    
    // Check common paths first
    val foundPath = commonPaths.find { path =>
      new java.io.File(path).exists() || new java.io.File(path + ".1").exists()
    }
    
    if (foundPath.isDefined) {
      return (Some(true), foundPath)
    }
    
    // Try ldconfig to find libcuda.so
    Try {
      import scala.sys.process._
      val output = "ldconfig -p 2>/dev/null".!!
      val lines = output.split("\n")
      val libcudaLine = lines.find(_.contains("libcuda.so"))
      
      libcudaLine.map { line =>
        // Extract path from ldconfig output
        val parts = line.split("=>")
        if (parts.length > 1) {
          parts(1).trim
        } else {
          line.split("\\s+").last
        }
      }
    }.toOption.flatten match {
      case Some(path) => (Some(true), Some(path))
      case None => 
        // Try to find using locate or find
        Try {
          import scala.sys.process._
          val output = "locate libcuda.so 2>/dev/null | head -1".!!
          if (output.trim.nonEmpty) {
            (Some(true), Some(output.trim))
          } else {
            (Some(false), None)
          }
        }.toOption.getOrElse((Some(false), None))
    }
  }

  /**
   * Check if GPUDirect Storage (GDS) is enabled.
   */
  private def checkGdsEnabled: Option[Boolean] = {
    Try {
      // Check for nvidia_fs module
      import scala.sys.process._
      val lsmodOutput = "lsmod".!!
      val hasnvidiaFs = lsmodOutput.contains("nvidia_fs")
      
      // Also check for cufile library
      val hasCufile = new java.io.File("/usr/local/cuda/lib64/libcufile.so").exists() ||
                      new java.io.File("/usr/lib64/libcufile.so").exists()
      
      Some(hasnvidiaFs && hasCufile)
    }.toOption.flatten.orElse {
      Some(false)
    }
  }

  /**
   * Get CUDA version from nvidia-smi or environment.
   */
  private def getCudaVersion: Option[String] = {
    // Try nvidia-smi first
    val nvidiaSmiVersion = Try {
      import scala.sys.process._
      val output = "nvidia-smi".!!
      val cudaPattern = """CUDA Version: (\d+\.\d+)""".r
      cudaPattern.findFirstMatchIn(output).map(_.group(1))
    }.toOption.flatten

    // Fallback to nvcc
    val nvccVersion = if (nvidiaSmiVersion.isEmpty) {
      Try {
        import scala.sys.process._
        val output = "nvcc --version".!!
        val versionPattern = """release (\d+\.\d+)""".r
        versionPattern.findFirstMatchIn(output).map(_.group(1))
      }.toOption.flatten
    } else {
      nvidiaSmiVersion
    }

    // Fallback to environment variable
    nvccVersion.orElse(sys.env.get("CUDA_VERSION"))
  }

  /**
   * Get cuDNN version.
   */
  private def getCudnnVersion: Option[String] = {
    // Try to read from cudnn header file
    val cudnnVersionFromHeader = Try {
      val headerPaths = Seq(
        "/usr/include/cudnn_version.h",
        "/usr/local/cuda/include/cudnn_version.h",
        "/usr/include/cudnn.h",
        "/usr/local/cuda/include/cudnn.h"
      )
      
      headerPaths.flatMap { path =>
        Try {
          val source = scala.io.Source.fromFile(path)
          try {
            val content = source.mkString
            val majorPattern = """#define CUDNN_MAJOR (\d+)""".r
            val minorPattern = """#define CUDNN_MINOR (\d+)""".r
            val patchPattern = """#define CUDNN_PATCHLEVEL (\d+)""".r
            
            for {
              major <- majorPattern.findFirstMatchIn(content).map(_.group(1))
              minor <- minorPattern.findFirstMatchIn(content).map(_.group(1))
              patch <- patchPattern.findFirstMatchIn(content).map(_.group(1))
            } yield s"$major.$minor.$patch"
          } finally {
            source.close()
          }
        }.toOption.flatten.toSeq
      }.headOption
    }.toOption.flatten

    cudnnVersionFromHeader.orElse(sys.env.get("CUDNN_VERSION"))
  }

  /**
   * Get NCCL version.
   */
  private def getNcclVersion: Option[String] = {
    // Try to read from nccl header file
    val ncclVersionFromHeader = Try {
      val headerPaths = Seq(
        "/usr/include/nccl.h",
        "/usr/local/nccl/include/nccl.h"
      )
      
      headerPaths.flatMap { path =>
        Try {
          val source = scala.io.Source.fromFile(path)
          try {
            val content = source.mkString
            val majorPattern = """#define NCCL_MAJOR (\d+)""".r
            val minorPattern = """#define NCCL_MINOR (\d+)""".r
            val patchPattern = """#define NCCL_PATCH (\d+)""".r
            
            for {
              major <- majorPattern.findFirstMatchIn(content).map(_.group(1))
              minor <- minorPattern.findFirstMatchIn(content).map(_.group(1))
              patch <- patchPattern.findFirstMatchIn(content).map(_.group(1))
            } yield s"$major.$minor.$patch"
          } finally {
            source.close()
          }
        }.toOption.flatten.toSeq
      }.headOption
    }.toOption.flatten

    ncclVersionFromHeader.orElse(sys.env.get("NCCL_VERSION"))
  }

  /**
   * Get NVIDIA driver version.
   */
  private def getNvidiaDriverVersion: Option[String] = {
    Try {
      import scala.sys.process._
      val output = "nvidia-smi --query-gpu=driver_version --format=csv,noheader".!!
      Some(output.trim.split("\n").head)
    }.toOption.flatten.orElse {
      // Fallback: read from /proc/driver/nvidia/version
      Try {
        val source = scala.io.Source.fromFile("/proc/driver/nvidia/version")
        try {
          val content = source.mkString
          val versionPattern = """(\d+\.\d+(?:\.\d+)?)""".r
          versionPattern.findFirstMatchIn(content).map(_.group(1))
        } finally {
          source.close()
        }
      }.toOption.flatten
    }
  }

  /**
   * Get spark-rapids plugin version if loaded.
   */
  private def getSparkRapidsVersion(sc: org.apache.spark.SparkContext): Option[String] = {
    // Check if spark-rapids plugin is configured
    val pluginClass = sc.getConf.get("spark.plugins", "")
    
    if (pluginClass.contains("com.nvidia.spark.SQLPlugin")) {
      // Try to get version from the plugin class
      Try {
        val clazz = Class.forName("com.nvidia.spark.rapids.RapidsPluginVersion")
        val versionMethod = clazz.getMethod("getVersion")
        versionMethod.invoke(null).toString
      }.toOption.orElse {
        // Check jar file in classpath
        Try {
          val url = Class.forName("com.nvidia.spark.SQLPlugin")
            .getProtectionDomain.getCodeSource.getLocation
          val jarName = url.getPath.split("/").last
          val versionPattern = """rapids-4-spark_[\d.]+-([\d.]+)""".r
          versionPattern.findFirstMatchIn(jarName).map(_.group(1))
        }.toOption.flatten
      }.orElse(Some("installed (version unknown)"))
    } else {
      None
    }
  }
}
