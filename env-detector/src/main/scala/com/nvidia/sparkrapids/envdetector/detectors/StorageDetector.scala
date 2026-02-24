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
import org.apache.hadoop.fs.FileSystem

import java.io.File
import scala.util.Try

/**
 * Detects storage configuration across the cluster.
 */
object StorageDetector {

  def detect(spark: SparkSession): StorageInfo = {
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration

    // Detect default file system
    val defaultFs = hadoopConf.get("fs.defaultFS", hadoopConf.get("fs.default.name", "file:///"))
    
    // Detect storage types based on configuration
    val storageTypes = detectStorageTypes(hadoopConf, defaultFs)
    
    // Get HDFS info if available
    val hdfsInfo = getHdfsInfo(spark, defaultFs)
    
    // Get local storage info from all nodes
    val localStorageInfo = getLocalStorageInfo(sc)
    
    // Get Spark local directories info (shuffle paths)
    val sparkLocalDirs = getSparkLocalDirsInfo(sc)

    StorageInfo(
      storageTypes = storageTypes,
      hdfsInfo = hdfsInfo,
      localStorageInfo = localStorageInfo,
      defaultFileSystem = defaultFs,
      sparkLocalDirs = sparkLocalDirs
    )
  }

  /**
   * Detect all configured storage types.
   */
  private def detectStorageTypes(hadoopConf: org.apache.hadoop.conf.Configuration, defaultFs: String): Seq[String] = {
    val types = scala.collection.mutable.ListBuffer[String]()
    
    // Check default file system
    if (defaultFs.startsWith("hdfs://")) types += "HDFS"
    else if (defaultFs.startsWith("s3://") || defaultFs.startsWith("s3a://") || defaultFs.startsWith("s3n://")) types += "S3"
    else if (defaultFs.startsWith("oss://")) types += "OSS"
    else if (defaultFs.startsWith("gs://")) types += "GCS"
    else if (defaultFs.startsWith("wasb://") || defaultFs.startsWith("abfs://")) types += "Azure Blob"
    else if (defaultFs.startsWith("file://") || defaultFs == "file:///") types += "Local"
    
    // Check for additional configured file systems
    val s3Configured = hadoopConf.get("fs.s3a.access.key") != null || 
                       hadoopConf.get("fs.s3.awsAccessKeyId") != null
    if (s3Configured && !types.contains("S3")) types += "S3"
    
    val ossConfigured = hadoopConf.get("fs.oss.accessKeyId") != null
    if (ossConfigured && !types.contains("OSS")) types += "OSS"
    
    // Local storage is always available
    if (!types.contains("Local")) types += "Local"
    
    types.toSeq
  }

  /**
   * Get HDFS information if the cluster is using HDFS.
   */
  private def getHdfsInfo(spark: SparkSession, defaultFs: String): Option[HdfsInfo] = {
    if (!defaultFs.startsWith("hdfs://")) {
      return None
    }
    
    Try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val status = fs.getStatus
      
      HdfsInfo(
        nameNodeUrl = defaultFs,
        totalCapacity = status.getCapacity,
        usedCapacity = status.getUsed,
        availableCapacity = status.getRemaining,
        blockSize = hadoopConf.getLong("dfs.blocksize", 128 * 1024 * 1024),
        replication = hadoopConf.getInt("dfs.replication", 3)
      )
    }.toOption
  }

  /**
   * Get Spark local directories (shuffle paths) info from all nodes.
   */
  private def getSparkLocalDirsInfo(sc: org.apache.spark.SparkContext): Seq[SparkLocalDirInfo] = {
    // Get configured spark.local.dir from Spark config
    val sparkLocalDir = sc.getConf.get("spark.local.dir", System.getProperty("java.io.tmpdir"))
    val sparkLocalDirs = sparkLocalDir.split(",").map(_.trim)
    
    val numPartitions = math.max(sc.defaultParallelism, 10)
    
    // Broadcast the directories to check
    val dirsBroadcast = sc.broadcast(sparkLocalDirs)
    
    // Collect Spark local dirs info from all executors
    val executorLocalDirs = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
        val dirs = dirsBroadcast.value
        val dirsInfo = dirs.flatMap { dir =>
          getSparkLocalDirInfo(hostname, dir)
        }
        Iterator((hostname, dirsInfo.toSeq))
      }
      .collect()
      .toSeq
      .groupBy(_._1).map(_._2.head).toSeq
      .flatMap(_._2)
    
    // Get driver local dirs info
    val driverHostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("driver")
    val driverLocalDirs = sparkLocalDirs.flatMap { dir =>
      getSparkLocalDirInfo(driverHostname, dir)
    }.toSeq
    
    // Combine and deduplicate
    (driverLocalDirs ++ executorLocalDirs).groupBy(d => (d.hostname, d.path)).map(_._2.head).toSeq
  }

  /**
   * Get info for a single Spark local directory.
   */
  private def getSparkLocalDirInfo(hostname: String, path: String): Option[SparkLocalDirInfo] = {
    Try {
      val dir = new File(path)
      val isAccessible = dir.exists() && dir.canWrite
      val (totalSpace, freeSpace) = if (isAccessible) {
        (dir.getTotalSpace, dir.getFreeSpace)
      } else {
        (0L, 0L)
      }
      val storageType = if (isAccessible) detectStorageTypeForPath(path) else "Unknown"
      
      SparkLocalDirInfo(
        hostname = hostname,
        path = path,
        totalSpace = totalSpace,
        freeSpace = freeSpace,
        storageType = storageType,
        isAccessible = isAccessible
      )
    }.toOption
  }

  /**
   * Detect storage type for a specific path.
   */
  private def detectStorageTypeForPath(path: String): String = {
    if (!new File("/sys/block").exists()) {
      return "Unknown"
    }
    
    Try {
      val deviceName = getDeviceForPath(path)
      deviceName match {
        case Some(dev) =>
          val rotational = new File(s"/sys/block/$dev/queue/rotational")
          if (rotational.exists()) {
            val source = scala.io.Source.fromFile(rotational)
            try {
              val isRotational = source.mkString.trim
              if (isRotational == "0") {
                if (dev.startsWith("nvme")) "NVMe SSD" else "SSD"
              } else {
                "HDD"
              }
            } finally {
              source.close()
            }
          } else {
            "Unknown"
          }
        case None => "Unknown"
      }
    }.getOrElse("Unknown")
  }

  /**
   * Get local storage information from all nodes in the cluster.
   */
  private def getLocalStorageInfo(sc: org.apache.spark.SparkContext): Seq[LocalStorageInfo] = {
    val numPartitions = math.max(sc.defaultParallelism, 10)
    
    // Collect local storage info from all executors
    val executorStorageInfo = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
        val mountPoints = getLocalMountPoints
        Iterator((hostname, mountPoints))
      }
      .collect()
      .toSeq
      .groupBy(_._1).map(_._2.head).toSeq
    
    // Add driver storage info
    val driverHostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("driver")
    val driverMountPoints = getLocalMountPoints
    
    val allStorageInfo = ((driverHostname, driverMountPoints) +: executorStorageInfo).groupBy(_._1).map(_._2.head).toSeq
    
    allStorageInfo.map { case (hostname, mountPoints) =>
      LocalStorageInfo(hostname, mountPoints)
    }
  }

  /**
   * Get mount point information for the local node.
   */
  private def getLocalMountPoints: Seq[MountPointInfo] = {
    val roots = File.listRoots()
    
    roots.toSeq.flatMap { root =>
      Try {
        MountPointInfo(
          path = root.getAbsolutePath,
          totalSpace = root.getTotalSpace,
          freeSpace = root.getFreeSpace,
          usableSpace = root.getUsableSpace,
          storageType = detectStorageType(root)
        )
      }.toOption
    }
  }

  /**
   * Attempt to detect the storage type (SSD, HDD, NVMe, etc.)
   * This is a best-effort detection based on heuristics.
   */
  private def detectStorageType(root: File): String = {
    val path = root.getAbsolutePath
    detectStorageTypeForPath(path)
  }

  /**
   * Get the block device name for a given path (Linux only).
   */
  private def getDeviceForPath(path: String): Option[String] = {
    Try {
      import scala.sys.process._
      val output = s"df $path".!!
      val lines = output.split("\n")
      if (lines.length > 1) {
        val device = lines(1).split("\\s+")(0)
        // Remove partition number to get base device name
        val deviceName = device.replaceAll(".*/", "").replaceAll("p?[0-9]+$", "")
        Some(deviceName)
      } else {
        None
      }
    }.toOption.flatten
  }
}
