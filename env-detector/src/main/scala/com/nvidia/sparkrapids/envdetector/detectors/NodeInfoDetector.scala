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

import com.nvidia.sparkrapids.envdetector.model.{NodeDetails, NodeInfo}
import org.apache.spark.SparkContext

import java.net.InetAddress
import scala.util.Try

/**
 * Detects node information across the cluster.
 */
object NodeInfoDetector {

  def detect(sc: SparkContext): NodeInfo = {
    // Get driver node info
    val driverNode = getDriverNodeDetails

    // Get executor node info by running tasks on each executor
    val executorNodes = getExecutorNodeDetails(sc)

    // Get unique hostnames to determine node count
    val allHostnames = (Seq(driverNode.hostname) ++ executorNodes.map(_.hostname)).distinct

    NodeInfo(
      totalNodes = allHostnames.size,
      driverNode = driverNode,
      executorNodes = executorNodes,
      isMultiNode = allHostnames.size > 1
    )
  }

  /**
   * Get driver node details.
   */
  private def getDriverNodeDetails: NodeDetails = {
    val hostname = Try(InetAddress.getLocalHost.getHostName).getOrElse("unknown")
    val ipAddress = Try(InetAddress.getLocalHost.getHostAddress).getOrElse("unknown")

    NodeDetails(
      hostname = hostname,
      ipAddress = ipAddress,
      nodeType = "driver",
      executorId = None
    )
  }

  /**
   * Get details of all executor nodes by running tasks on each.
   */
  private def getExecutorNodeDetails(sc: SparkContext): Seq[NodeDetails] = {
    // Create a dummy RDD with enough partitions to cover all executors
    val numPartitions = math.max(sc.defaultParallelism * 2, 100)
    
    val executorInfo = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val hostname = Try(InetAddress.getLocalHost.getHostName).getOrElse("unknown")
        val ipAddress = Try(InetAddress.getLocalHost.getHostAddress).getOrElse("unknown")
        val executorId = org.apache.spark.TaskContext.get().taskAttemptId().toString
        
        // Get actual executor ID from Spark environment
        val sparkEnv = org.apache.spark.SparkEnv.get
        val realExecutorId = if (sparkEnv != null) sparkEnv.executorId else "unknown"
        
        Iterator((hostname, ipAddress, realExecutorId))
      }
      .collect()
      .toSeq
      .groupBy(_._3).map(_._2.head).toSeq // Distinct by executor ID
      .filter(_._3 != "driver") // Exclude driver

    executorInfo.map { case (hostname, ip, execId) =>
      NodeDetails(
        hostname = hostname,
        ipAddress = ip,
        nodeType = "executor",
        executorId = Some(execId)
      )
    }
  }
}

