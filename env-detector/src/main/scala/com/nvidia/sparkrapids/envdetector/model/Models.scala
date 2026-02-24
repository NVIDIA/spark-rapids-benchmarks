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

package com.nvidia.sparkrapids.envdetector.model

/**
 * Complete environment report containing all detected information.
 */
case class EnvironmentReport(
  timestamp: Long,
  deployment: DeploymentInfo,
  nodes: NodeInfo,
  network: NetworkInfo,
  storage: StorageInfo,
  software: SoftwareInfo,
  hardware: HardwareInfo,
  sparkConfig: SparkConfigInfo,
  readinessScore: Option[ReadinessScore] = None
)

/**
 * GPU acceleration readiness score.
 */
case class ReadinessScore(
  overallScore: String,          // Green, Yellow, Red
  overallDescription: String,
  hardwareScore: String,
  softwareScore: String,
  networkScore: String,
  storageScore: String,
  issues: Seq[String],
  recommendations: Seq[String]
)

/**
 * Cluster deployment mode information.
 */
case class DeploymentInfo(
  mode: String,           // standalone, yarn, k8s, local, mesos
  masterUrl: String,
  deployMode: String,     // client or cluster
  description: String
)

/**
 * Node information across the cluster.
 */
case class NodeInfo(
  totalNodes: Int,
  driverNode: NodeDetails,
  executorNodes: Seq[NodeDetails],
  isMultiNode: Boolean
)

/**
 * Details of a single node.
 */
case class NodeDetails(
  hostname: String,
  ipAddress: String,
  nodeType: String,       // driver or executor
  executorId: Option[String] = None
)

/**
 * Network configuration information.
 */
case class NetworkInfo(
  networkType: String,    // internal or external
  bandwidth: Option[String],
  latency: Option[String],
  interfaces: Seq[NetworkInterface],
  driverToExecutorLatency: Map[String, Long]  // executor -> latency in ms
)

/**
 * Network interface information.
 */
case class NetworkInterface(
  name: String,
  ipAddress: String,
  isUp: Boolean,
  isLoopback: Boolean,
  mtu: Option[Int],                    // Maximum Transmission Unit
  interfaceType: Option[String],       // Ethernet, InfiniBand, RoCE, etc.
  speed: Option[String],               // Link speed e.g., "100000 Mb/s" for 100GbE
  macAddress: Option[String]
)

/**
 * Storage configuration information.
 */
case class StorageInfo(
  storageTypes: Seq[String],       // HDFS, S3, OSS, local, etc.
  hdfsInfo: Option[HdfsInfo],
  localStorageInfo: Seq[LocalStorageInfo],
  defaultFileSystem: String,
  sparkLocalDirs: Seq[SparkLocalDirInfo]  // Spark shuffle/local directories
)

/**
 * Spark local directory information (for shuffle).
 */
case class SparkLocalDirInfo(
  hostname: String,
  path: String,
  totalSpace: Long,
  freeSpace: Long,
  storageType: String,             // SSD, HDD, NVMe, etc.
  isAccessible: Boolean
)

/**
 * HDFS specific information.
 */
case class HdfsInfo(
  nameNodeUrl: String,
  totalCapacity: Long,
  usedCapacity: Long,
  availableCapacity: Long,
  blockSize: Long,
  replication: Int
)

/**
 * Local storage information per node.
 */
case class LocalStorageInfo(
  hostname: String,
  mountPoints: Seq[MountPointInfo]
)

/**
 * Mount point details.
 */
case class MountPointInfo(
  path: String,
  totalSpace: Long,
  freeSpace: Long,
  usableSpace: Long,
  storageType: String     // SSD, HDD, NVMe, etc.
)

/**
 * Software version information.
 */
case class SoftwareInfo(
  sparkVersion: String,
  scalaVersion: String,
  javaVersion: String,
  hadoopVersion: Option[String],
  osInfo: OsInfo,
  gpuSoftware: GpuSoftwareInfo
)

/**
 * Operating system information.
 */
case class OsInfo(
  name: String,
  version: String,
  arch: String,
  kernelVersion: Option[String],       // e.g., "5.15.0-91-generic"
  distribution: Option[String]         // e.g., "Ubuntu 22.04.3 LTS"
)

/**
 * GPU software stack information.
 */
case class GpuSoftwareInfo(
  cudaVersion: Option[String],
  cudnnVersion: Option[String],
  ncclVersion: Option[String],
  nvidiaDriverVersion: Option[String],
  sparkRapidsVersion: Option[String],
  nvidiaPeermemLoaded: Option[Boolean],   // nvidia-peermem for GPUDirect Storage
  libcudaPresent: Option[Boolean],        // libcuda.so presence
  libcudaPath: Option[String],            // Path to libcuda.so
  gdsEnabled: Option[Boolean]             // GPUDirect Storage enabled
)

/**
 * Hardware configuration information.
 */
case class HardwareInfo(
  driverHardware: HardwareDetails,
  executorHardware: Seq[ExecutorHardwareInfo]
)

/**
 * Executor-specific hardware information.
 */
case class ExecutorHardwareInfo(
  executorId: String,
  hostname: String,
  hardware: HardwareDetails
)

/**
 * Hardware details of a single node.
 */
case class HardwareDetails(
  cpu: CpuInfo,
  memory: MemoryInfo,
  gpu: Option[GpuInfo]
)

/**
 * CPU information.
 */
case class CpuInfo(
  model: String,
  physicalCores: Int,           // Physical core count
  logicalCores: Int,            // Logical cores (threads)
  hyperThreadingEnabled: Boolean,
  socketsCount: Int,            // Number of CPU sockets
  coresPerSocket: Int,          // Cores per socket
  architecture: String,
  frequency: Option[String],
  maxFrequency: Option[String]
)

/**
 * Memory information.
 */
case class MemoryInfo(
  totalPhysical: Long,
  freePhysical: Long,
  jvmMaxHeap: Long,
  jvmTotalHeap: Long,
  jvmFreeHeap: Long,
  dimmSpeed: Option[String],           // e.g., "DDR4-3200", "3200 MT/s"
  dimmCount: Option[Int],              // Number of DIMMs
  numaNodes: Option[Int]               // Number of NUMA nodes
)

/**
 * GPU information.
 */
case class GpuInfo(
  gpuCount: Int,
  gpus: Seq[GpuDevice],
  nvlinkTopology: Option[NvlinkTopology]
)

/**
 * NVLink topology information.
 */
case class NvlinkTopology(
  nvlinkVersion: Option[String],
  nvlinkConnections: Seq[NvlinkConnection],
  nvlinkBandwidthGBps: Option[Double]
)

/**
 * Individual NVLink connection between GPUs.
 */
case class NvlinkConnection(
  gpu0: Int,
  gpu1: Int,
  linkCount: Int,                       // Number of NVLink connections
  linkBandwidthGBps: Option[Double]
)

/**
 * Individual GPU device information.
 */
case class GpuDevice(
  index: Int,
  name: String,
  memoryTotal: Long,
  memoryFree: Long,
  computeCapability: String,
  temperature: Option[Int],
  utilization: Option[Int]
)

/**
 * Spark configuration summary.
 */
case class SparkConfigInfo(
  executorCount: Int,
  executorCores: Int,
  executorMemory: String,
  driverMemory: String,
  shufflePartitions: Int,
  dynamicAllocationEnabled: Boolean,
  adaptiveExecutionEnabled: Boolean,
  rapidsEnabled: Boolean,
  importantConfigs: Map[String, String],
  sparkDefaultsConf: Map[String, String],  // Settings from spark-defaults.conf
  sparkHome: Option[String],               // SPARK_HOME path
  sparkConfDir: Option[String]             // SPARK_CONF_DIR path
)

