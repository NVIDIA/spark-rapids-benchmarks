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
import com.nvidia.sparkrapids.envdetector.benchmark.BenchmarkConfig.BenchmarkParams

import scala.util.Try

/**
 * CPU performance benchmark.
 * Tests single-thread and multi-thread floating-point performance.
 */
object CpuBenchmark {

  private val ITERATIONS = 2000000   // Number of floating-point operations
  private val TEST_DURATION_SEC = 2  // Duration for utilization test

  def run(sc: SparkContext, params: BenchmarkParams): CpuBenchmarkResult = {
    println(s"    Running CPU benchmark (${params.numPartitions} partitions)...")
    
    // Test on driver
    val driverResult = runLocalCpuBenchmark()
    
    // Test on all executors
    val executorResults = runExecutorCpuBenchmarks(sc, params.numPartitions)
    
    // Run Spark distributed CPU test
    val sparkGflops = runSparkCpuTest(sc, params.numPartitions)

    CpuBenchmarkResult(
      driverResult = driverResult,
      executorResults = executorResults,
      sparkCpuTestGflops = sparkGflops
    )
  }
  
  // Backward compatible
  def run(sc: SparkContext): CpuBenchmarkResult = {
    run(sc, BenchmarkConfig.calculateBenchmarkParams(sc))
  }

  private def runExecutorCpuBenchmarks(sc: SparkContext, numPartitions: Int): Seq[ExecutorCpuResult] = {
    
    sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val sparkEnv = org.apache.spark.SparkEnv.get
        val executorId = if (sparkEnv != null) sparkEnv.executorId else "unknown"
        val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
        
        val result = runLocalCpuBenchmark()
        
        Iterator(ExecutorCpuResult(
          executorId = executorId,
          hostname = hostname,
          result = result
        ))
      }
      .collect()
      .toSeq
      .groupBy(_.executorId).map(_._2.head).toSeq
      .filter(_.executorId != "driver")
  }

  private def runLocalCpuBenchmark(): CpuTestResult = {
    val cores = Runtime.getRuntime.availableProcessors()
    
    // Single-thread test
    val singleThreadGflops = runFloatPointTest(1)
    
    // Multi-thread test
    val multiThreadGflops = runFloatPointTest(cores)
    
    // CPU utilization test
    val utilization = measureCpuUtilization()

    CpuTestResult(
      singleThreadGflops = singleThreadGflops,
      multiThreadGflops = multiThreadGflops,
      cores = cores,
      utilizationPercent = utilization
    )
  }

  /**
   * Run floating-point benchmark with specified number of threads.
   */
  private def runFloatPointTest(numThreads: Int): Double = {
    val iterationsPerThread = ITERATIONS / numThreads
    
    val startTime = System.nanoTime()
    
    val threads = (0 until numThreads).map { _ =>
      new Thread(() => {
        var result = 0.0
        var a = 1.1
        var b = 2.2
        var c = 3.3
        
        for (_ <- 0 until iterationsPerThread) {
          // FMA-like operations
          result += a * b + c
          result += a / b - c
          result += Math.sqrt(a * a + b * b)
          result += Math.sin(a) + Math.cos(b)
          a += 0.001
          b += 0.001
          c += 0.001
        }
        
        // Prevent optimization
        if (result == Double.NaN) println(result)
      })
    }
    
    threads.foreach(_.start())
    threads.foreach(_.join())
    
    val endTime = System.nanoTime()
    val durationSec = (endTime - startTime) / 1e9
    
    // Each iteration has ~8 floating-point operations
    val totalOperations = ITERATIONS.toLong * 8
    val gflops = (totalOperations / durationSec) / 1e9
    
    gflops
  }

  /**
   * Measure CPU utilization by running a stress test.
   */
  private def measureCpuUtilization(): Double = {
    val osBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    
    val cores = Runtime.getRuntime.availableProcessors()
    
    // Start stress threads
    @volatile var running = true
    val threads = (0 until cores).map { _ =>
      new Thread(() => {
        var x = 0.0
        while (running) {
          x = Math.sin(x + 0.001)
        }
      })
    }
    
    threads.foreach(_.start())
    
    // Measure CPU usage over time
    Thread.sleep(500) // Warm up
    val samples = (0 until 10).map { _ =>
      Thread.sleep(100)
      osBean.getSystemCpuLoad * 100
    }
    
    running = false
    threads.foreach(_.join())
    
    samples.sum / samples.size
  }

  /**
   * Run distributed CPU test using Spark.
   */
  private def runSparkCpuTest(sc: SparkContext, numPartitions: Int): Double = {
    val iterationsPerPartition = ITERATIONS / numPartitions
    
    val startTime = System.nanoTime()
    
    val totalOps = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        var result = 0.0
        var a = 1.1
        var b = 2.2
        var c = 3.3
        
        for (_ <- 0 until iterationsPerPartition) {
          result += a * b + c
          result += a / b - c
          result += Math.sqrt(a * a + b * b)
          result += Math.sin(a) + Math.cos(b)
          a += 0.001
          b += 0.001
          c += 0.001
        }
        
        Iterator(iterationsPerPartition * 8L) // 8 ops per iteration
      }
      .reduce(_ + _)
    
    val endTime = System.nanoTime()
    val durationSec = (endTime - startTime) / 1e9
    
    (totalOps / durationSec) / 1e9
  }
}

