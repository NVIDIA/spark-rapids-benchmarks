package com.nvidia.sparkrapids.envdetector.benchmark

import org.apache.spark.SparkContext
import com.nvidia.sparkrapids.envdetector.benchmark.BenchmarkConfig.BenchmarkParams

/**
 * Network performance benchmark.
 * Tests shuffle bandwidth which is the primary network performance indicator for Spark.
 */
object NetworkBenchmark {

  def run(sc: SparkContext, params: BenchmarkParams): NetworkBenchmarkResult = {
    println("    Running network benchmark...")
    println(s"      Config: ${params.numPartitions} partitions, ${params.shuffleDataSizeGB}GB x ${params.shuffleRounds} rounds")
    
    // Test shuffle bandwidth (main network performance indicator)
    val (shuffleBandwidth, avgLatency) = testShuffleBandwidth(sc, params)

    NetworkBenchmarkResult(
      shuffleBandwidthMBps = shuffleBandwidth,
      avgLatencyMs = avgLatency,
      testDataSizeMB = params.shuffleRounds * params.shuffleDataSizeGB * 1024
    )
  }
  
  // Backward compatible version
  def run(sc: SparkContext): NetworkBenchmarkResult = {
    run(sc, BenchmarkConfig.calculateBenchmarkParams(sc))
  }

  /**
   * Test shuffle bandwidth using multiple rounds.
   * This is the primary network performance test as it reflects real Spark shuffle behavior.
   */
  private def testShuffleBandwidth(sc: SparkContext, params: BenchmarkParams): (Double, Double) = {
    val numPartitions = params.numPartitions
    val dataPerRoundBytes = params.shuffleDataSizeGB.toLong * 1024 * 1024 * 1024
    val bytesPerPartition = dataPerRoundBytes / numPartitions
    val recordSize = 4096  // 4KB per record
    val recordsPerPartition = (bytesPerPartition / recordSize).toInt
    
    val totalDataGB = params.shuffleRounds * params.shuffleDataSizeGB
    println(s"      Shuffle test: ${totalDataGB}GB total (${params.shuffleRounds} rounds x ${params.shuffleDataSizeGB}GB)")
    
    val startTime = System.nanoTime()
    
    for (round <- 1 to params.shuffleRounds) {
      val rdd = sc.parallelize(1 to numPartitions, numPartitions)
        .flatMap { partId =>
          val random = new scala.util.Random(partId + round * 1000)
          (0 until recordsPerPartition).iterator.map { i =>
            val key = random.nextInt(numPartitions)
            val value = new Array[Byte](recordSize)
            random.nextBytes(value)
            (key, value)
          }
        }
      
      rdd.reduceByKey((a, b) => a).count()
      
      if (round % 2 == 0 || round == params.shuffleRounds) {
        val elapsed = (System.nanoTime() - startTime) / 1e9
        println(s"        Round $round/${params.shuffleRounds} completed, ${elapsed.toInt}s elapsed")
      }
    }
    
    val endTime = System.nanoTime()
    val durationSec = (endTime - startTime) / 1e9
    
    val totalDataMB = (totalDataGB * 1024).toDouble
    val shuffleBandwidth = if (durationSec > 0) totalDataMB / durationSec else 0.0
    val avgLatency = (durationSec * 1000) / (numPartitions * params.shuffleRounds)
    
    println(s"      Shuffle completed: ${shuffleBandwidth.toInt} MB/s, ${durationSec.toInt}s total")
    
    (shuffleBandwidth, avgLatency)
  }
}
