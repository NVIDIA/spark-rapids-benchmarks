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

/**
 * Complete benchmark report containing all performance test results.
 */
case class BenchmarkReport(
  timestamp: Long,
  diskBenchmark: DiskBenchmarkResult,
  networkBenchmark: NetworkBenchmarkResult,
  cpuBenchmark: CpuBenchmarkResult,
  memoryBenchmark: MemoryBenchmarkResult,
  gpuBenchmark: Option[GpuBenchmarkResult]
)

/**
 * Disk I/O benchmark result.
 */
case class DiskBenchmarkResult(
  sequentialReadMBps: Double,
  sequentialWriteMBps: Double,
  randomReadMBps: Double,      // 4MB block random read throughput (MB/s)
  randomWriteMBps: Double,     // 4MB block random write throughput (MB/s)
  testPath: String,
  testSizeMB: Int,
  executorResults: Seq[ExecutorDiskResult],
  // HDFS performance metrics (for Spark scan operations)
  hdfsReadBandwidthMBps: Option[Double] = None,
  hdfsWriteBandwidthMBps: Option[Double] = None,
  hdfsTestPath: Option[String] = None
)

case class ExecutorDiskResult(
  executorId: String,
  hostname: String,
  sequentialReadMBps: Double,
  sequentialWriteMBps: Double,
  randomReadMBps: Double,      // 4MB block random read throughput (MB/s)
  randomWriteMBps: Double      // 4MB block random write throughput (MB/s)
)

/**
 * Network benchmark result.
 */
case class NetworkBenchmarkResult(
  shuffleBandwidthMBps: Double,
  avgLatencyMs: Double,
  testDataSizeMB: Int
)

/**
 * CPU benchmark result.
 */
case class CpuBenchmarkResult(
  driverResult: CpuTestResult,
  executorResults: Seq[ExecutorCpuResult],
  sparkCpuTestGflops: Double  // Spark distributed CPU test
)

case class CpuTestResult(
  singleThreadGflops: Double,
  multiThreadGflops: Double,
  cores: Int,
  utilizationPercent: Double
)

case class ExecutorCpuResult(
  executorId: String,
  hostname: String,
  result: CpuTestResult
)

/**
 * Memory benchmark result.
 */
case class MemoryBenchmarkResult(
  driverResult: MemoryTestResult,
  executorResults: Seq[ExecutorMemoryResult],
  sparkMemoryTestGBps: Double  // Spark distributed memory test
)

case class MemoryTestResult(
  readBandwidthGBps: Double,
  writeBandwidthGBps: Double,
  copyBandwidthGBps: Double,
  totalMemoryGB: Double,
  usedMemoryGB: Double,
  utilizationPercent: Double
)

case class ExecutorMemoryResult(
  executorId: String,
  hostname: String,
  result: MemoryTestResult
)

/**
 * GPU benchmark result.
 */
case class GpuBenchmarkResult(
  driverResult: Option[GpuTestResult],
  executorResults: Seq[ExecutorGpuResult]
)

case class GpuTestResult(
  gpuIndex: Int,
  gpuName: String,
  computeTflops: Double,
  memoryBandwidthGBps: Double,
  utilizationPercent: Double,
  memoryUsedGB: Double,
  memoryTotalGB: Double
)

case class ExecutorGpuResult(
  executorId: String,
  hostname: String,
  results: Seq[GpuTestResult]
)
