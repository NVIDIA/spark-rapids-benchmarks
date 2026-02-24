package com.nvidia.sparkrapids.envdetector

import com.nvidia.sparkrapids.envdetector.detectors._
import com.nvidia.sparkrapids.envdetector.model._
import com.nvidia.sparkrapids.envdetector.benchmark.{BenchmarkReport, BenchmarkRunner}
import com.nvidia.sparkrapids.envdetector.report.ReportGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/**
 * Main entry point for the Spark Rapids Environment Detector.
 * This tool automatically detects and reports cluster environment configuration
 * and runs performance benchmarks to help optimize spark-rapids POC testing.
 * 
 * Usage:
 *   spark-submit --class com.nvidia.sparkrapids.envdetector.EnvDetector \
 *     spark-rapids-env-detector.jar [options] [output-path]
 * 
 * Options:
 *   --benchmark     Run performance benchmarks (disk, network, CPU, memory, GPU)
 *   --no-gpu        Skip GPU benchmarks
 *   --help          Show this help message
 */
object EnvDetector {

  case class Config(
    runBenchmark: Boolean = false,
    enableGpu: Boolean = true,
    outputPath: Option[String] = None
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    
    if (args.contains("--help")) {
      printHelp()
      return
    }

    val spark = SparkSession.builder()
      .appName("Spark Rapids Environment Detector")
      .getOrCreate()

    try {
      println("=" * 80)
      println("Spark Rapids Environment Detector")
      println("=" * 80)
      println()

      // Detect environment
      val envReport = detectEnvironment(spark)
      
      // Run benchmarks if requested
      val benchmarkReport = if (config.runBenchmark) {
        Some(BenchmarkRunner.run(spark.sparkContext, config.enableGpu))
      } else {
        None
      }
      
      // Print reports to console
      ReportGenerator.printReport(envReport)
      benchmarkReport.foreach(ReportGenerator.printBenchmarkReport)
      
      // Optionally save to file
      config.outputPath.foreach { path =>
        ReportGenerator.saveReport(envReport, path, spark)
        benchmarkReport.foreach { br =>
          ReportGenerator.saveBenchmarkReport(br, s"${path}_benchmark", spark)
        }
        println(s"\nReport saved to: $path")
      }

    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): Config = {
    var config = Config()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--benchmark" => config = config.copy(runBenchmark = true)
        case "--no-gpu" => config = config.copy(enableGpu = false)
        case "--help" => // handled separately
        case arg if !arg.startsWith("--") => config = config.copy(outputPath = Some(arg))
        case _ => // ignore unknown options
      }
      i += 1
    }
    config
  }

  private def printHelp(): Unit = {
    println("""
Spark Rapids Environment Detector
==================================

A tool to detect cluster environment configuration and run performance benchmarks
to help diagnose spark-rapids POC testing issues.

Usage:
  spark-submit --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    spark-rapids-env-detector.jar [options] [output-path]

Options:
  --benchmark     Run performance benchmarks (disk, network, CPU, memory, GPU)
                  This will stress test all system resources to measure peak performance.
  --no-gpu        Skip GPU benchmarks (useful if no GPU is available)
  --help          Show this help message

Examples:
  # Basic environment detection
  spark-submit --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    spark-rapids-env-detector.jar

  # With performance benchmarks
  spark-submit --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    spark-rapids-env-detector.jar --benchmark

  # Save report to file
  spark-submit --class com.nvidia.sparkrapids.envdetector.EnvDetector \
    spark-rapids-env-detector.jar --benchmark /tmp/env-report
""")
  }

  /**
   * Detect the complete cluster environment configuration.
   */
  def detectEnvironment(spark: SparkSession): EnvironmentReport = {
    val sc = spark.sparkContext

    println("Detecting cluster environment...")
    println()

    // 1. Detect cluster deployment mode
    println("[1/7] Detecting cluster deployment mode...")
    val deploymentInfo = ClusterDeploymentDetector.detect(sc)

    // 2. Detect node information
    println("[2/7] Detecting node information...")
    val nodeInfo = NodeInfoDetector.detect(sc)

    // 3. Detect network configuration
    println("[3/7] Detecting network configuration...")
    val networkInfo = NetworkDetector.detect(sc)

    // 4. Detect storage configuration
    println("[4/7] Detecting storage configuration...")
    val storageInfo = StorageDetector.detect(spark)

    // 5. Detect software versions
    println("[5/7] Detecting software versions...")
    val softwareInfo = SoftwareDetector.detect(spark)

    // 6. Detect hardware configuration
    println("[6/7] Detecting hardware configuration...")
    val hardwareInfo = HardwareDetector.detect(sc)

    // 7. Collect Spark configuration
    println("[7/7] Collecting Spark configuration...")
    val sparkConfigInfo = SparkConfigDetector.detect(sc)

    println()
    println("Detection complete!")
    println()

    val baseReport: EnvironmentReport = EnvironmentReport(
      timestamp = System.currentTimeMillis(),
      deployment = deploymentInfo,
      nodes = nodeInfo,
      network = networkInfo,
      storage = storageInfo,
      software = softwareInfo,
      hardware = hardwareInfo,
      sparkConfig = sparkConfigInfo,
      readinessScore = None
    )
    
    // Calculate readiness score
    println("Calculating GPU acceleration readiness score...")
    val readinessScore = ReadinessScoreCalculator.calculate(baseReport)
    
    baseReport.copy(readinessScore = Some(readinessScore))
  }
}
