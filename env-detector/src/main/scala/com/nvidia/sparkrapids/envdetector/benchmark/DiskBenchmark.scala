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

import java.io.{File, FileInputStream, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import scala.util.{Random, Try}

/**
 * Disk I/O performance benchmark.
 * Tests sequential and random read/write performance.
 */
object DiskBenchmark {

  // Small block for raw IOPS test (matches SSD/HDD sector size)
  private val SMALL_BLOCK_SIZE = 4 * 1024         // 4KB
  private val SMALL_BLOCK_IO_COUNT = 10000        // 10K ops = 40MB data
  
  // Large block for Spark-like I/O pattern (shuffle block / Parquet row group)
  private val LARGE_BLOCK_SIZE = 4 * 1024 * 1024  // 4MB
  private val LARGE_BLOCK_IO_COUNT = 500          // 500 ops = 2GB data
  
  // Sequential I/O buffer
  private val SEQ_BUFFER_SIZE = 4 * 1024 * 1024   // 4MB buffer

  def run(sc: SparkContext, params: BenchmarkParams): DiskBenchmarkResult = {
    val testFileSizeMB = params.diskTestSizeMB
    val sizeDisplay = if (testFileSizeMB >= 1024) f"${testFileSizeMB / 1024.0}%.1fGB" else s"${testFileSizeMB}MB"
    
    // Use spark.local.dir (shuffle path) for testing, not /tmp
    val sparkLocalDir = getSparkLocalDir(sc)
    println(s"    Running disk I/O benchmark ($sizeDisplay test file)...")
    println(s"      Test path: $sparkLocalDir (spark.local.dir)")
    
    // Test on driver
    val driverResult = runLocalBenchmark("driver", s"$sparkLocalDir/spark_disk_benchmark_${System.currentTimeMillis()}", testFileSizeMB)
    
    // Test on all executors
    val executorResults = runExecutorBenchmarks(sc, params, sparkLocalDir)
    
    // Calculate averages
    val avgSeqRead = if (executorResults.nonEmpty) executorResults.map(_.sequentialReadMBps).sum / executorResults.size else driverResult.sequentialReadMBps
    val avgSeqWrite = if (executorResults.nonEmpty) executorResults.map(_.sequentialWriteMBps).sum / executorResults.size else driverResult.sequentialWriteMBps
    val avgRandomRead = if (executorResults.nonEmpty) executorResults.map(_.randomReadMBps).sum / executorResults.size else driverResult.randomReadMBps
    val avgRandomWrite = if (executorResults.nonEmpty) executorResults.map(_.randomWriteMBps).sum / executorResults.size else driverResult.randomWriteMBps

    // HDFS performance test (if HDFS is available)
    val (hdfsReadBw, hdfsWriteBw, hdfsPath) = testHdfsPerformance(sc, params)

    DiskBenchmarkResult(
      sequentialReadMBps = avgSeqRead,
      sequentialWriteMBps = avgSeqWrite,
      randomReadMBps = avgRandomRead,
      randomWriteMBps = avgRandomWrite,
      testPath = sparkLocalDir,
      testSizeMB = testFileSizeMB,
      executorResults = executorResults,
      hdfsReadBandwidthMBps = hdfsReadBw,
      hdfsWriteBandwidthMBps = hdfsWriteBw,
      hdfsTestPath = hdfsPath
    )
  }
  
  /**
   * Test HDFS read/write performance (simulates Spark scan/write operations).
   * Uses Spark's distributed read to measure actual HDFS throughput across cluster.
   */
  private def testHdfsPerformance(sc: SparkContext, params: BenchmarkParams): (Option[Double], Option[Double], Option[String]) = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    
    val hadoopConf = sc.hadoopConfiguration
    val defaultFs = hadoopConf.get("fs.defaultFS", "file:///")
    
    // Only test if HDFS is configured
    if (!defaultFs.startsWith("hdfs://")) {
      println("      HDFS not configured, skipping HDFS performance test")
      return (None, None, None)
    }
    
    val hdfsTestPath = s"$defaultFs/tmp/spark_hdfs_benchmark_${System.currentTimeMillis()}"
    println(s"      Testing HDFS performance: $hdfsTestPath")
    
    try {
      val fs = FileSystem.get(hadoopConf)
      val testPath = new Path(hdfsTestPath)
      
      // Test data size: 1GB per partition minimum for accurate HDFS measurement
      // Use fewer partitions but larger data per partition
      val numPartitions = math.min(params.numPartitions, 16)  // Limit partitions for HDFS test
      val dataPerPartitionMB = 1024  // 1GB per partition
      val hdfsTestSizeMB = numPartitions * dataPerPartitionMB
      val bufferSizeMB = 64  // 64MB buffer (memory-friendly, matches HDFS block size)
      val buffersPerPartition = dataPerPartitionMB / bufferSizeMB
      
      println(s"      HDFS test: ${hdfsTestSizeMB / 1024}GB total ($numPartitions partitions x ${dataPerPartitionMB}MB each)")
      
      // Write test: use Hadoop FileSystem API for accurate measurement
      val hdfsConfBroadcast = sc.broadcast(sc.hadoopConfiguration)
      val testPathBroadcast = sc.broadcast(hdfsTestPath)
      
      val writeStartTime = System.nanoTime()
      val writeResults = sc.parallelize(1 to numPartitions, numPartitions)
        .mapPartitions { iter =>
          val partId = iter.next()
          val conf = hdfsConfBroadcast.value
          val partPath = new Path(s"${testPathBroadcast.value}/part-$partId")
          val hdfs = FileSystem.get(conf)
          
          val buffer = new Array[Byte](bufferSizeMB * 1024 * 1024)
          val random = new scala.util.Random(partId)
          
          val out = hdfs.create(partPath, true)
          try {
            for (_ <- 0 until buffersPerPartition) {
              random.nextBytes(buffer)
              out.write(buffer)
            }
            out.hflush()  // Ensure data is written to HDFS
          } finally {
            out.close()
          }
          
          Iterator(dataPerPartitionMB.toLong)
        }
        .reduce(_ + _)
      val writeEndTime = System.nanoTime()
      val writeDuration = (writeEndTime - writeStartTime) / 1e9
      val writeBandwidth = writeResults / writeDuration
      
      println(f"      HDFS Write: $writeBandwidth%.1f MB/s ($writeResults MB in ${writeDuration}%.1f s)")
      
      // Read test: read back using Hadoop FileSystem API
      val readStartTime = System.nanoTime()
      val readResults = sc.parallelize(1 to numPartitions, numPartitions)
        .mapPartitions { iter =>
          val partId = iter.next()
          val conf = hdfsConfBroadcast.value
          val partPath = new Path(s"${testPathBroadcast.value}/part-$partId")
          val hdfs = FileSystem.get(conf)
          
          val buffer = new Array[Byte](bufferSizeMB * 1024 * 1024)
          var totalRead = 0L
          
          val in = hdfs.open(partPath)
          try {
            var bytesRead = 0
            while ({ bytesRead = in.read(buffer); bytesRead != -1 }) {
              totalRead += bytesRead
            }
          } finally {
            in.close()
          }
          
          Iterator(totalRead / (1024 * 1024))  // Return MB read
        }
        .reduce(_ + _)
      val readEndTime = System.nanoTime()
      val readDuration = (readEndTime - readStartTime) / 1e9
      val readBandwidth = readResults / readDuration
      
      println(f"      HDFS Read: $readBandwidth%.1f MB/s ($readResults MB in ${readDuration}%.1f s)")
      
      // Cleanup
      Try(fs.delete(testPath, true))
      
      (Some(readBandwidth.toDouble), Some(writeBandwidth.toDouble), Some(hdfsTestPath))
    } catch {
      case e: Exception =>
        println(s"      HDFS test failed: ${e.getMessage}")
        (None, None, None)
    }
  }
  
  // Backward compatible
  def run(sc: SparkContext): DiskBenchmarkResult = {
    run(sc, BenchmarkConfig.calculateBenchmarkParams(sc))
  }
  
  /**
   * Get test directory for disk benchmark.
   * Priority:
   *   1. spark.rapids.benchmark.diskTestPath (custom override)
   *   2. spark.local.dir (actual shuffle directory)
   *   3. java.io.tmpdir (fallback)
   * 
   * Usage: --conf spark.rapids.benchmark.diskTestPath=/mnt/raid/test
   */
  private def getSparkLocalDir(sc: SparkContext): String = {
    // Allow user to override test path via config
    val customPath = sc.getConf.getOption("spark.rapids.benchmark.diskTestPath")
    
    val testDir = customPath.getOrElse {
      sc.getConf.get("spark.local.dir", System.getProperty("java.io.tmpdir", "/tmp"))
    }
    
    // If multiple dirs configured, use the first one
    testDir.split(",").head.trim
  }

  private def runExecutorBenchmarks(sc: SparkContext, params: BenchmarkParams, testBasePath: String): Seq[ExecutorDiskResult] = {
    val numPartitions = params.numPartitions
    val testFileSizeMB = params.diskTestSizeMB
    
    // Broadcast the test file size and base path to executors
    val testSizeBroadcast = sc.broadcast(testFileSizeMB)
    val testPathBroadcast = sc.broadcast(testBasePath)
    
    sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val sparkEnv = org.apache.spark.SparkEnv.get
        val executorId = if (sparkEnv != null) sparkEnv.executorId else "unknown"
        val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
        
        // Use spark.local.dir from executor's perspective
        val localSparkDir = Option(System.getProperty("spark.local.dir"))
          .orElse(Option(System.getenv("SPARK_LOCAL_DIRS")))
          .getOrElse(testPathBroadcast.value)
          .split(",").head.trim
        
        val testPath = s"$localSparkDir/spark_disk_benchmark_${System.currentTimeMillis()}"
        val result = runLocalBenchmark(executorId, testPath, testSizeBroadcast.value)
        
        Iterator(ExecutorDiskResult(
          executorId = executorId,
          hostname = hostname,
          sequentialReadMBps = result.sequentialReadMBps,
          sequentialWriteMBps = result.sequentialWriteMBps,
          randomReadMBps = result.randomReadMBps,
          randomWriteMBps = result.randomWriteMBps
        ))
      }
      .collect()
      .toSeq
      .groupBy(_.executorId).map(_._2.head).toSeq
      .filter(_.executorId != "driver")
  }

  case class LocalBenchmarkResult(
    sequentialReadMBps: Double,
    sequentialWriteMBps: Double,
    randomReadMBps: Double,
    randomWriteMBps: Double
  )

  private def runLocalBenchmark(id: String, testPath: String, testFileSizeMB: Int = 10240): LocalBenchmarkResult = {
    val testFile = new File(s"${testPath}_$id.dat")
    
    try {
      // Ensure parent directory exists
      Option(testFile.getParentFile).foreach(_.mkdirs())
      
      // Check available disk space
      val parentDir = Option(testFile.getParentFile).getOrElse(new File("/tmp"))
      val availableSpaceMB = parentDir.getUsableSpace / (1024 * 1024)
      val actualTestSizeMB = if (availableSpaceMB < testFileSizeMB * 1.5) {
        // If not enough space, use 50% of available space (minimum 64MB)
        val adjustedSize = math.max((availableSpaceMB * 0.5).toInt, 64)
        println(s"      Warning: Limited disk space (${availableSpaceMB}MB available), adjusting test size to ${adjustedSize}MB")
        adjustedSize
      } else {
        testFileSizeMB
      }
      
      // Sequential write test
      val seqWriteMBps = Try(testSequentialWrite(testFile, actualTestSizeMB)).getOrElse(0.0)
      
      // Sequential read test
      val seqReadMBps = Try(testSequentialRead(testFile, actualTestSizeMB)).getOrElse(0.0)
      
      // Random write test (4MB blocks, only if file exists)
      val randomWriteMBps = if (testFile.exists()) Try(testRandomWrite(testFile)).getOrElse(0.0) else 0.0
      
      // Random read test (4MB blocks, only if file exists)
      val randomReadMBps = if (testFile.exists()) Try(testRandomRead(testFile)).getOrElse(0.0) else 0.0
      
      LocalBenchmarkResult(seqReadMBps, seqWriteMBps, randomReadMBps, randomWriteMBps)
    } catch {
      case _: Exception => LocalBenchmarkResult(0.0, 0.0, 0.0, 0.0)
    } finally {
      // Cleanup
      Try(testFile.delete())
    }
  }

  private def testSequentialWrite(file: File, testFileSizeMB: Int): Double = {
    val buffer = new Array[Byte](SEQ_BUFFER_SIZE) // 4MB buffer (closer to HDFS I/O pattern)
    Random.nextBytes(buffer)
    
    val bufferSizeMB = SEQ_BUFFER_SIZE / (1024 * 1024)
    val iterations = testFileSizeMB / bufferSizeMB
    
    val startTime = System.nanoTime()
    val fos = new FileOutputStream(file)
    try {
      for (_ <- 0 until iterations) {
        fos.write(buffer)
      }
      fos.getFD.sync() // Ensure data is written to disk
    } finally {
      fos.close()
    }
    val endTime = System.nanoTime()
    
    val durationSec = (endTime - startTime) / 1e9
    (iterations * bufferSizeMB) / durationSec
  }

  private def testSequentialRead(file: File, testFileSizeMB: Int): Double = {
    val buffer = new Array[Byte](SEQ_BUFFER_SIZE) // 4MB buffer (closer to HDFS I/O pattern)
    
    val startTime = System.nanoTime()
    val fis = new FileInputStream(file)
    try {
      var bytesRead = 0
      var total = 0L
      while ({ bytesRead = fis.read(buffer); bytesRead != -1 }) {
        total += bytesRead
      }
    } finally {
      fis.close()
    }
    val endTime = System.nanoTime()
    
    val durationSec = (endTime - startTime) / 1e9
    val totalMB = file.length().toDouble / (1024 * 1024)
    totalMB / durationSec
  }

  /**
   * Random write test using 4MB blocks (Spark shuffle block size).
   * Returns throughput in MB/s (more relevant for Spark than raw IOPS).
   */
  private def testRandomWrite(file: File): Double = {
    val buffer = new Array[Byte](LARGE_BLOCK_SIZE)  // 4MB block
    Random.nextBytes(buffer)
    val fileSize = file.length()
    
    val startTime = System.nanoTime()
    val raf = new RandomAccessFile(file, "rw")
    try {
      for (_ <- 0 until LARGE_BLOCK_IO_COUNT) {
        val position = (Random.nextDouble() * (fileSize - LARGE_BLOCK_SIZE)).toLong
        raf.seek(position)
        raf.write(buffer)
      }
      raf.getFD.sync()
    } finally {
      raf.close()
    }
    val endTime = System.nanoTime()
    
    val durationSec = (endTime - startTime) / 1e9
    // Return MB/s instead of IOPS (more meaningful for large block I/O)
    val totalMB = (LARGE_BLOCK_IO_COUNT.toLong * LARGE_BLOCK_SIZE) / (1024.0 * 1024.0)
    totalMB / durationSec
  }

  /**
   * Random read test using 4MB blocks (Spark shuffle block size).
   * Returns throughput in MB/s (more relevant for Spark than raw IOPS).
   */
  private def testRandomRead(file: File): Double = {
    val buffer = new Array[Byte](LARGE_BLOCK_SIZE)  // 4MB block
    val fileSize = file.length()
    
    val startTime = System.nanoTime()
    val raf = new RandomAccessFile(file, "r")
    try {
      for (_ <- 0 until LARGE_BLOCK_IO_COUNT) {
        val position = (Random.nextDouble() * (fileSize - LARGE_BLOCK_SIZE)).toLong
        raf.seek(position)
        raf.readFully(buffer)
      }
    } finally {
      raf.close()
    }
    val endTime = System.nanoTime()
    
    val durationSec = (endTime - startTime) / 1e9
    // Return MB/s instead of IOPS (more meaningful for large block I/O)
    val totalMB = (LARGE_BLOCK_IO_COUNT.toLong * LARGE_BLOCK_SIZE) / (1024.0 * 1024.0)
    totalMB / durationSec
  }
}

