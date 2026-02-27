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

import com.nvidia.sparkrapids.envdetector.model.DeploymentInfo
import org.apache.spark.SparkContext

/**
 * Detects the cluster deployment mode: standalone, yarn, k8s, mesos, or local.
 */
object ClusterDeploymentDetector {

  def detect(sc: SparkContext): DeploymentInfo = {
    val masterUrl = sc.master
    val deployMode = sc.deployMode

    val (mode, description) = masterUrl match {
      case m if m.startsWith("local") =>
        ("local", s"Local mode with ${extractLocalThreads(m)} threads")
      
      case m if m.startsWith("spark://") =>
        ("standalone", "Spark Standalone cluster")
      
      case m if m.startsWith("yarn") || m == "yarn" || m == "yarn-client" || m == "yarn-cluster" =>
        ("yarn", "YARN (Hadoop) cluster manager")
      
      case m if m.startsWith("k8s://") || m.contains("kubernetes") =>
        ("kubernetes", "Kubernetes cluster manager")
      
      case m if m.startsWith("mesos://") =>
        ("mesos", "Apache Mesos cluster manager")
      
      case _ =>
        ("unknown", s"Unknown cluster manager: $masterUrl")
    }

    DeploymentInfo(
      mode = mode,
      masterUrl = masterUrl,
      deployMode = deployMode,
      description = description
    )
  }

  /**
   * Extract thread count from local mode string.
   */
  private def extractLocalThreads(master: String): String = {
    master match {
      case "local" => "1"
      case "local[*]" => "all available"
      case m if m.startsWith("local[") && m.endsWith("]") =>
        m.substring(6, m.length - 1)
      case _ => "unknown"
    }
  }
}

