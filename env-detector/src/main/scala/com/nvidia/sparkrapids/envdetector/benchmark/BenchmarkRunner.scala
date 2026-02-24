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
 * Main benchmark runner that executes all performance tests.
 * Automatically configures parameters based on cluster resources.
 */
object BenchmarkRunner {

  def run(sc: SparkContext, enableGpu: Boolean = true): BenchmarkReport = {
    println()
    
    // Detect cluster resources and calculate optimal parameters
    val resources = BenchmarkConfig.detectClusterResources(sc)
    val params = BenchmarkConfig.calculateBenchmarkParams(sc)
    
    // Print configuration
    BenchmarkConfig.printConfig(resources, params)
    
    println("=" * 80)
    println("PERFORMANCE BENCHMARK")
    println("=" * 80)
    println()
    println("Running performance benchmarks to stress test system resources...")
    println("This may take several minutes.")
    println()

    // Disk benchmark
    println("[1/5] Disk I/O Benchmark")
    val diskResult = DiskBenchmark.run(sc, params)
    println(f"      Sequential Read:  ${diskResult.sequentialReadMBps}%.2f MB/s")
    println(f"      Sequential Write: ${diskResult.sequentialWriteMBps}%.2f MB/s")
    println(f"      Random Read:  ${diskResult.randomReadMBps}%.2f MB/s")
    println(f"      Random Write: ${diskResult.randomWriteMBps}%.2f MB/s")
    println()

    // Network benchmark
    println("[2/5] Network Benchmark")
    val networkResult = NetworkBenchmark.run(sc, params)
    println(f"      Shuffle Bandwidth: ${networkResult.shuffleBandwidthMBps}%.2f MB/s")
    println(f"      Average Latency:   ${networkResult.avgLatencyMs}%.2f ms")
    println()

    // CPU benchmark
    println("[3/5] CPU Benchmark")
    val cpuResult = CpuBenchmark.run(sc, params)
    println(f"      Single-Thread:     ${cpuResult.driverResult.singleThreadGflops}%.2f GFLOPS")
    println(f"      Multi-Thread:      ${cpuResult.driverResult.multiThreadGflops}%.2f GFLOPS")
    println(f"      Spark Distributed: ${cpuResult.sparkCpuTestGflops}%.2f GFLOPS")
    println(f"      CPU Utilization:   ${cpuResult.driverResult.utilizationPercent}%.1f%%")
    println()

    // Memory benchmark
    println("[4/5] Memory Benchmark")
    val memoryResult = MemoryBenchmark.run(sc, params)
    println(f"      Read Bandwidth:    ${memoryResult.driverResult.readBandwidthGBps}%.2f GB/s")
    println(f"      Write Bandwidth:   ${memoryResult.driverResult.writeBandwidthGBps}%.2f GB/s")
    println(f"      Copy Bandwidth:    ${memoryResult.driverResult.copyBandwidthGBps}%.2f GB/s")
    println(f"      Spark Distributed: ${memoryResult.sparkMemoryTestGBps}%.2f GB/s")
    println()

    // GPU benchmark
    println("[5/5] GPU Benchmark")
    val gpuResult = if (enableGpu) {
      val result = GpuBenchmark.run(sc, params)
      result.foreach { r =>
        r.driverResult.foreach { gpu =>
          println(f"      GPU: ${gpu.gpuName}")
          println(f"      Compute:           ${gpu.computeTflops}%.2f TFLOPS (estimated)")
          println(f"      Memory Bandwidth:  ${gpu.memoryBandwidthGBps}%.2f GB/s (estimated)")
        }
      }
      result
    } else {
      println("      GPU benchmark disabled")
      None
    }
    println()

    println("Benchmark complete!")
    println()

    BenchmarkReport(
      timestamp = System.currentTimeMillis(),
      diskBenchmark = diskResult,
      networkBenchmark = networkResult,
      cpuBenchmark = cpuResult,
      memoryBenchmark = memoryResult,
      gpuBenchmark = gpuResult
    )
  }
}
