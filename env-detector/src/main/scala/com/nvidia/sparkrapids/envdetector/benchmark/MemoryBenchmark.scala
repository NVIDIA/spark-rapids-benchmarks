package com.nvidia.sparkrapids.envdetector.benchmark

import org.apache.spark.SparkContext
import com.nvidia.sparkrapids.envdetector.benchmark.BenchmarkConfig.BenchmarkParams

import scala.util.Try

/**
 * Memory performance benchmark.
 * Tests memory bandwidth (read, write, copy) and utilization.
 */
object MemoryBenchmark {

  private val TEST_SIZE_MB = 64   // 64MB test buffer (smaller to avoid OOM)
  private val ITERATIONS = 3     // Number of iterations for faster execution

  def run(sc: SparkContext, params: BenchmarkParams): MemoryBenchmarkResult = {
    println(s"    Running memory benchmark (${params.numPartitions} partitions)...")
    
    // Test on driver
    val driverResult = runLocalMemoryBenchmark()
    
    // Test on all executors
    val executorResults = runExecutorMemoryBenchmarks(sc, params.numPartitions)
    
    // Run Spark distributed memory test
    val sparkMemoryGBps = runSparkMemoryTest(sc, params.numPartitions)

    MemoryBenchmarkResult(
      driverResult = driverResult,
      executorResults = executorResults,
      sparkMemoryTestGBps = sparkMemoryGBps
    )
  }
  
  // Backward compatible
  def run(sc: SparkContext): MemoryBenchmarkResult = {
    run(sc, BenchmarkConfig.calculateBenchmarkParams(sc))
  }

  private def runExecutorMemoryBenchmarks(sc: SparkContext, maxPartitions: Int): Seq[ExecutorMemoryResult] = {
    val numPartitions = math.min(maxPartitions, 32) // Limit partitions to avoid memory pressure
    
    Try {
      sc.parallelize(1 to numPartitions, numPartitions)
        .mapPartitions { _ =>
          val sparkEnv = org.apache.spark.SparkEnv.get
          val executorId = if (sparkEnv != null) sparkEnv.executorId else "unknown"
          val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
          
          val result = Try(runLocalMemoryBenchmark()).getOrElse(
            MemoryTestResult(0, 0, 0, 0, 0, 0)
          )
          
          // Force garbage collection to free memory
          System.gc()
          
          Iterator(ExecutorMemoryResult(
            executorId = executorId,
            hostname = hostname,
            result = result
          ))
        }
        .collect()
        .toSeq
        .groupBy(_.executorId).map(_._2.head).toSeq
        .filter(_.executorId != "driver")
    }.getOrElse(Seq.empty)
  }

  private def runLocalMemoryBenchmark(): MemoryTestResult = {
    val runtime = Runtime.getRuntime
    val osBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    
    // Get memory info
    val totalMemoryGB = osBean.getTotalPhysicalMemorySize / (1024.0 * 1024.0 * 1024.0)
    val freeMemoryGB = osBean.getFreePhysicalMemorySize / (1024.0 * 1024.0 * 1024.0)
    val usedMemoryGB = totalMemoryGB - freeMemoryGB
    val utilizationPercent = (usedMemoryGB / totalMemoryGB) * 100
    
    // Memory bandwidth tests
    val readBandwidth = testMemoryRead()
    val writeBandwidth = testMemoryWrite()
    val copyBandwidth = testMemoryCopy()

    MemoryTestResult(
      readBandwidthGBps = readBandwidth,
      writeBandwidthGBps = writeBandwidth,
      copyBandwidthGBps = copyBandwidth,
      totalMemoryGB = totalMemoryGB,
      usedMemoryGB = usedMemoryGB,
      utilizationPercent = utilizationPercent
    )
  }

  /**
   * Test memory read bandwidth.
   */
  private def testMemoryRead(): Double = {
    val sizeBytes = TEST_SIZE_MB * 1024 * 1024
    val buffer = new Array[Long](sizeBytes / 8)
    
    // Initialize buffer
    for (i <- buffer.indices) {
      buffer(i) = i.toLong
    }
    
    // Warm up
    var sum = 0L
    for (i <- buffer.indices) {
      sum += buffer(i)
    }
    
    // Measure read
    val startTime = System.nanoTime()
    for (_ <- 0 until ITERATIONS) {
      sum = 0L
      var i = 0
      while (i < buffer.length) {
        sum += buffer(i)
        i += 1
      }
    }
    val endTime = System.nanoTime()
    
    // Prevent optimization
    if (sum == Long.MinValue) println(sum)
    
    val durationSec = (endTime - startTime) / 1e9
    val totalGB = (TEST_SIZE_MB.toLong * ITERATIONS) / 1024.0
    totalGB / durationSec
  }

  /**
   * Test memory write bandwidth.
   */
  private def testMemoryWrite(): Double = {
    val sizeBytes = TEST_SIZE_MB * 1024 * 1024
    val buffer = new Array[Long](sizeBytes / 8)
    
    // Warm up
    for (i <- buffer.indices) {
      buffer(i) = i.toLong
    }
    
    // Measure write
    val startTime = System.nanoTime()
    for (iter <- 0 until ITERATIONS) {
      var i = 0
      while (i < buffer.length) {
        buffer(i) = iter.toLong + i
        i += 1
      }
    }
    val endTime = System.nanoTime()
    
    val durationSec = (endTime - startTime) / 1e9
    val totalGB = (TEST_SIZE_MB.toLong * ITERATIONS) / 1024.0
    totalGB / durationSec
  }

  /**
   * Test memory copy bandwidth.
   */
  private def testMemoryCopy(): Double = {
    val sizeBytes = TEST_SIZE_MB * 1024 * 1024 / 2 // Half size for src and dst
    val src = new Array[Long](sizeBytes / 8)
    val dst = new Array[Long](sizeBytes / 8)
    
    // Initialize source
    for (i <- src.indices) {
      src(i) = i.toLong
    }
    
    // Warm up
    System.arraycopy(src, 0, dst, 0, src.length)
    
    // Measure copy
    val startTime = System.nanoTime()
    for (_ <- 0 until ITERATIONS) {
      System.arraycopy(src, 0, dst, 0, src.length)
    }
    val endTime = System.nanoTime()
    
    val durationSec = (endTime - startTime) / 1e9
    val totalGB = (TEST_SIZE_MB.toLong / 2 * ITERATIONS) / 1024.0
    totalGB / durationSec
  }

  /**
   * Run distributed memory test using Spark.
   * Each partition processes data with multiple iterations to stress memory bandwidth.
   * Data size is adjusted based on available memory to avoid OOM.
   */
  private def runSparkMemoryTest(sc: SparkContext, maxPartitions: Int): Double = {
    // Use fewer partitions for memory test to reduce concurrent memory usage
    val numPartitions = math.min(maxPartitions, 8)
    // 64MB per partition × 8 partitions = 512MB concurrent (safe for 4GB driver)
    val sizePerPartitionMB = 64
    val iterations = 5  // More iterations to stress memory bandwidth
    
    val startTime = System.nanoTime()
    
    val totalBytes = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val sizeBytes = sizePerPartitionMB * 1024 * 1024
        val buffer = new Array[Long](sizeBytes / 8)
        
        var totalProcessed = 0L
        
        for (_ <- 0 until iterations) {
          // Write pass
          var i = 0
          while (i < buffer.length) {
            buffer(i) = i.toLong
            i += 1
          }
          
          // Read pass
          var sum = 0L
          i = 0
          while (i < buffer.length) {
            sum += buffer(i)
            i += 1
          }
          
          // Prevent optimization
          if (sum == Long.MinValue) println(sum)
          
          totalProcessed += sizeBytes * 2L  // Read + Write bytes per iteration
        }
        
        Iterator(totalProcessed)
      }
      .reduce(_ + _)
    
    val endTime = System.nanoTime()
    val durationSec = (endTime - startTime) / 1e9
    
    (totalBytes / (1024.0 * 1024.0 * 1024.0)) / durationSec
  }
}

