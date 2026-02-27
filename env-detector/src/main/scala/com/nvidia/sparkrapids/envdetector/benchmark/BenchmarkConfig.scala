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

package com.nvidia.sparkrapids.envdetector.benchmark

import org.apache.spark.SparkContext

/**
 * Dynamic benchmark configuration based on cluster resources.
 * Automatically adjusts parameters to fully utilize available resources
 * while respecting resource limits.
 */
object BenchmarkConfig {
  
  // Resource limits (can be overridden via Spark conf)
  private val DEFAULT_MAX_NODES = 8           // Max nodes to use for benchmark
  private val DEFAULT_MAX_EXECUTORS_PER_NODE = 4
  private val DEFAULT_MAX_CORES_PER_EXECUTOR = 16
  private val DEFAULT_MAX_MEMORY_PER_EXECUTOR_GB = 32
  
  // Minimum resources
  private val MIN_PARTITIONS = 8
  private val MIN_DATA_SIZE_MB = 100
  
  case class ClusterResources(
    totalNodes: Int,
    totalExecutors: Int,
    totalCores: Int,
    executorMemoryMB: Long,
    executorCores: Int
  )
  
  case class BenchmarkParams(
    numPartitions: Int,           // Shuffle partitions
    shuffleDataSizeGB: Int,       // Total shuffle data size
    shuffleRounds: Int,           // Number of shuffle rounds
    dataPerPartitionMB: Int,      // Data per partition
    diskTestSizeMB: Int,          // Disk test file size
    maxExecutorsToUse: Int        // Max executors to use
  )
  
  /**
   * Detect cluster resources from SparkContext.
   */
  def detectClusterResources(sc: SparkContext): ClusterResources = {
    val conf = sc.getConf
    val master = sc.master
    
    // Check if running in local mode
    val isLocalMode = master.startsWith("local")
    
    if (isLocalMode) {
      // Local mode: parse cores from master string like "local[4]" or "local[*]"
      val localCores = if (master.contains("[*]")) {
        Runtime.getRuntime.availableProcessors()
      } else {
        val pattern = """local\[(\d+)\]""".r
        pattern.findFirstMatchIn(master).map(_.group(1).toInt)
          .getOrElse(Runtime.getRuntime.availableProcessors())
      }
      
      // Get memory (use system memory as estimate)
      val systemMemoryMB = Runtime.getRuntime.maxMemory() / (1024 * 1024)
      
      ClusterResources(
        totalNodes = 1,
        totalExecutors = 1,
        totalCores = localCores,
        executorMemoryMB = systemMemoryMB,
        executorCores = localCores
      )
    } else {
      // Cluster mode: detect from executor status
      val executorIds = sc.getExecutorMemoryStatus.keys.toSeq.filter(_ != "driver")
      val totalExecutors = math.max(executorIds.size, 1)
      
      // Get cores per executor (from config or default)
      val executorCores = conf.getInt("spark.executor.cores", 
        Runtime.getRuntime.availableProcessors())
      
      // Get executor memory
      val executorMemoryStr = conf.get("spark.executor.memory", "4g")
      val executorMemoryMB = parseMemoryString(executorMemoryStr)
      
      // Estimate nodes (assume executors are distributed)
      val totalNodes = math.max(totalExecutors / 4, 1)  // Assume ~4 executors per node
      
      val totalCores = totalExecutors * executorCores
      
      ClusterResources(
        totalNodes = totalNodes,
        totalExecutors = totalExecutors,
        totalCores = totalCores,
        executorMemoryMB = executorMemoryMB,
        executorCores = executorCores
      )
    }
  }
  
  /**
   * Calculate optimal benchmark parameters based on cluster resources.
   */
  def calculateBenchmarkParams(sc: SparkContext): BenchmarkParams = {
    val conf = sc.getConf
    val resources = detectClusterResources(sc)
    
    // Read limits from Spark conf (allow override)
    val maxNodes = conf.getInt("spark.rapids.benchmark.maxNodes", DEFAULT_MAX_NODES)
    val maxExecutorsPerNode = conf.getInt("spark.rapids.benchmark.maxExecutorsPerNode", DEFAULT_MAX_EXECUTORS_PER_NODE)
    val maxCoresPerExecutor = conf.getInt("spark.rapids.benchmark.maxCoresPerExecutor", DEFAULT_MAX_CORES_PER_EXECUTOR)
    val maxMemoryPerExecutorGB = conf.getInt("spark.rapids.benchmark.maxMemoryPerExecutorGB", DEFAULT_MAX_MEMORY_PER_EXECUTOR_GB)
    
    // Calculate how many executors to use
    val maxExecutors = maxNodes * maxExecutorsPerNode
    val executorsToUse = math.min(resources.totalExecutors, maxExecutors)
    
    // Calculate effective cores (capped per executor)
    val effectiveCoresPerExecutor = math.min(resources.executorCores, maxCoresPerExecutor)
    val totalCoresToUse = executorsToUse * effectiveCoresPerExecutor
    
    // Calculate effective memory
    val effectiveMemoryPerExecutorMB = math.min(resources.executorMemoryMB, maxMemoryPerExecutorGB * 1024L)
    
    // Partition count = total cores being used (good parallelism)
    val numPartitions = math.max(totalCoresToUse, MIN_PARTITIONS)
    
    // Shuffle data size: scale with cluster size, but cap at reasonable amount
    // Target: enough data to stress the network for at least 10 seconds on 100G network
    // 100G = 12.5 GB/s, 10 seconds = 125GB max, but we limit based on memory
    val maxShuffleDataGB = math.min(
      (effectiveMemoryPerExecutorMB * executorsToUse / 1024 / 4).toInt,  // Use 1/4 of total memory
      100  // Cap at 100GB
    )
    val shuffleDataSizeGB = math.max(maxShuffleDataGB, 2)
    
    // Shuffle rounds: more rounds for larger clusters to get stable measurements
    val shuffleRounds = if (executorsToUse > 16) 5 else if (executorsToUse > 8) 3 else 2
    
    // Data per partition: based on available memory per task
    val tasksPerExecutor = effectiveCoresPerExecutor
    val memoryPerTaskMB = effectiveMemoryPerExecutorMB / tasksPerExecutor / 2  // Use half for safety
    val dataPerPartitionMB = math.min(memoryPerTaskMB.toInt, 50).max(10)
    
    // Disk test size: larger for accurate measurement
    // Need to be large enough to bypass OS page cache for accurate disk speed
    // Local mode: 1GB (faster testing), Cluster mode: 10GB
    val isLocalMode = sc.master.startsWith("local")
    val defaultDiskTestSizeMB = if (isLocalMode) 1024 else 10 * 1024  // 1GB local, 10GB cluster
    val diskTestSizeMB = conf.getInt("spark.rapids.benchmark.diskTestSizeMB", defaultDiskTestSizeMB)
    
    BenchmarkParams(
      numPartitions = numPartitions,
      shuffleDataSizeGB = shuffleDataSizeGB,
      shuffleRounds = shuffleRounds,
      dataPerPartitionMB = dataPerPartitionMB,
      diskTestSizeMB = diskTestSizeMB,
      maxExecutorsToUse = executorsToUse
    )
  }
  
  /**
   * Parse memory string like "4g", "512m" to MB.
   */
  private def parseMemoryString(memStr: String): Long = {
    val str = memStr.trim.toLowerCase
    if (str.endsWith("g")) {
      str.dropRight(1).toLong * 1024
    } else if (str.endsWith("m")) {
      str.dropRight(1).toLong
    } else if (str.endsWith("k")) {
      str.dropRight(1).toLong / 1024
    } else {
      str.toLong / (1024 * 1024)  // Assume bytes
    }
  }
  
  /**
   * Print detected configuration.
   */
  def printConfig(resources: ClusterResources, params: BenchmarkParams): Unit = {
    println("=" * 80)
    println("BENCHMARK CONFIGURATION")
    println("=" * 80)
    println()
    println("Detected Cluster Resources:")
    println(f"  Total Nodes (estimated): ${resources.totalNodes}")
    println(f"  Total Executors:         ${resources.totalExecutors}")
    println(f"  Total Cores:             ${resources.totalCores}")
    println(f"  Executor Memory:         ${resources.executorMemoryMB / 1024}GB")
    println(f"  Cores per Executor:      ${resources.executorCores}")
    println()
    println("Benchmark Parameters (auto-configured):")
    println(f"  Executors to Use:        ${params.maxExecutorsToUse}")
    println(f"  Shuffle Partitions:      ${params.numPartitions}")
    println(f"  Shuffle Data Size:       ${params.shuffleDataSizeGB}GB x ${params.shuffleRounds} rounds")
    println(f"  Data per Partition:      ${params.dataPerPartitionMB}MB")
    println(f"  Disk Test Size:          ${params.diskTestSizeMB / 1024.0}%.1fGB")
    println()
    println("To override, set spark.rapids.benchmark.* properties:")
    println("  spark.rapids.benchmark.maxNodes=8")
    println("  spark.rapids.benchmark.maxExecutorsPerNode=4")
    println("  spark.rapids.benchmark.maxCoresPerExecutor=16")
    println("  spark.rapids.benchmark.maxMemoryPerExecutorGB=32")
    println("  spark.rapids.benchmark.diskTestSizeMB=10240")
    println("  spark.rapids.benchmark.diskTestPath=/mnt/raid/test  # Override disk test path")
    println()
  }
}

