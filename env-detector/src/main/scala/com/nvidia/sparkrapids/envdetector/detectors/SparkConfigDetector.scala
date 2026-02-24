package com.nvidia.sparkrapids.envdetector.detectors

import com.nvidia.sparkrapids.envdetector.model.SparkConfigInfo
import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Detects and summarizes important Spark configuration settings.
 */
object SparkConfigDetector {

  def detect(sc: SparkContext): SparkConfigInfo = {
    val conf = sc.getConf
    
    // Get executor configuration
    val executorCount = getExecutorCount(sc)
    val executorCores = conf.getInt("spark.executor.cores", 1)
    val executorMemory = conf.get("spark.executor.memory", "1g")
    val driverMemory = conf.get("spark.driver.memory", "1g")
    
    // Get shuffle configuration
    val shufflePartitions = conf.getInt("spark.sql.shuffle.partitions", 200)
    
    // Check dynamic allocation
    val dynamicAllocationEnabled = conf.getBoolean("spark.dynamicAllocation.enabled", false)
    
    // Check adaptive execution
    val adaptiveExecutionEnabled = conf.getBoolean("spark.sql.adaptive.enabled", false)
    
    // Check if RAPIDS is enabled
    val rapidsEnabled = isRapidsEnabled(conf)
    
    // Collect important configurations
    val importantConfigs = collectImportantConfigs(conf)
    
    // Read spark-defaults.conf
    val sparkDefaultsConf = readSparkDefaultsConf
    
    // Get SPARK_HOME and SPARK_CONF_DIR
    val sparkHome = getSparkHome
    val sparkConfDir = getSparkConfDir

    SparkConfigInfo(
      executorCount = executorCount,
      executorCores = executorCores,
      executorMemory = executorMemory,
      driverMemory = driverMemory,
      shufflePartitions = shufflePartitions,
      dynamicAllocationEnabled = dynamicAllocationEnabled,
      adaptiveExecutionEnabled = adaptiveExecutionEnabled,
      rapidsEnabled = rapidsEnabled,
      importantConfigs = importantConfigs,
      sparkDefaultsConf = sparkDefaultsConf,
      sparkHome = sparkHome,
      sparkConfDir = sparkConfDir
    )
  }

  /**
   * Get SPARK_HOME from environment or system properties.
   */
  private def getSparkHome: Option[String] = {
    sys.env.get("SPARK_HOME").orElse(
      Option(System.getProperty("spark.home"))
    )
  }

  /**
   * Get SPARK_CONF_DIR from environment or derive from SPARK_HOME.
   */
  private def getSparkConfDir: Option[String] = {
    sys.env.get("SPARK_CONF_DIR").orElse {
      getSparkHome.map(_ + "/conf")
    }
  }

  /**
   * Read settings from spark-defaults.conf file.
   */
  private def readSparkDefaultsConf: Map[String, String] = {
    val confDir = getSparkConfDir
    
    confDir.flatMap { dir =>
      val confFile = new java.io.File(dir, "spark-defaults.conf")
      if (confFile.exists) {
        Try {
          val source = scala.io.Source.fromFile(confFile)
          try {
            val lines = source.getLines().toList
            parseSparkDefaultsConf(lines)
          } finally {
            source.close()
          }
        }.toOption
      } else {
        None
      }
    }.getOrElse(Map.empty)
  }

  /**
   * Parse spark-defaults.conf content.
   */
  private def parseSparkDefaultsConf(lines: List[String]): Map[String, String] = {
    lines
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .flatMap { line =>
        // Split on first whitespace (space or tab)
        val parts = line.split("\\s+", 2)
        if (parts.length == 2) {
          Some(parts(0).trim -> parts(1).trim)
        } else {
          None
        }
      }
      .toMap
  }

  /**
   * Get the number of executors in the cluster.
   */
  private def getExecutorCount(sc: SparkContext): Int = {
    Try {
      // Get executor IDs from status tracker
      val executorIds = sc.statusTracker.getExecutorInfos.length
      math.max(executorIds - 1, 0) // Subtract 1 for driver if included
    }.getOrElse {
      // Fallback to default parallelism estimation
      sc.defaultParallelism
    }
  }

  /**
   * Check if RAPIDS is enabled.
   */
  private def isRapidsEnabled(conf: org.apache.spark.SparkConf): Boolean = {
    val plugins = conf.get("spark.plugins", "")
    val sqlPlugins = conf.get("spark.sql.extensions", "")
    
    plugins.contains("com.nvidia.spark") || 
    sqlPlugins.contains("com.nvidia.spark") ||
    conf.getBoolean("spark.rapids.sql.enabled", false)
  }

  /**
   * Collect important Spark configurations for reference.
   */
  private def collectImportantConfigs(conf: org.apache.spark.SparkConf): Map[String, String] = {
    val importantKeys = Seq(
      // Core settings
      "spark.master",
      "spark.app.name",
      "spark.submit.deployMode",
      
      // Executor settings
      "spark.executor.instances",
      "spark.executor.cores",
      "spark.executor.memory",
      "spark.executor.memoryOverhead",
      
      // Driver settings
      "spark.driver.cores",
      "spark.driver.memory",
      "spark.driver.memoryOverhead",
      
      // Parallelism settings
      "spark.default.parallelism",
      "spark.sql.shuffle.partitions",
      
      // Shuffle settings
      "spark.shuffle.manager",
      "spark.shuffle.compress",
      "spark.shuffle.spill.compress",
      "spark.local.dir",
      
      // Memory settings
      "spark.memory.fraction",
      "spark.memory.storageFraction",
      "spark.memory.offHeap.enabled",
      "spark.memory.offHeap.size",
      
      // SQL settings
      "spark.sql.adaptive.enabled",
      "spark.sql.adaptive.coalescePartitions.enabled",
      "spark.sql.adaptive.skewJoin.enabled",
      "spark.sql.codegen.wholeStage",
      
      // Dynamic allocation
      "spark.dynamicAllocation.enabled",
      "spark.dynamicAllocation.minExecutors",
      "spark.dynamicAllocation.maxExecutors",
      "spark.dynamicAllocation.initialExecutors",
      
      // RAPIDS settings
      "spark.plugins",
      "spark.sql.extensions",
      "spark.rapids.sql.enabled",
      "spark.rapids.sql.concurrentGpuTasks",
      "spark.rapids.memory.pinnedPool.size",
      "spark.rapids.sql.explain",
      
      // GPU settings
      "spark.executor.resource.gpu.amount",
      "spark.task.resource.gpu.amount",
      "spark.rapids.sql.batchSizeBytes",
      
      // Serialization
      "spark.serializer",
      "spark.kryo.registrationRequired",
      
      // Network settings
      "spark.network.timeout",
      "spark.rpc.askTimeout",
      
      // Speculation
      "spark.speculation",
      "spark.speculation.multiplier",
      
      // Event log
      "spark.eventLog.enabled",
      "spark.eventLog.dir",
      
      // History server
      "spark.history.fs.logDirectory"
    )

    importantKeys.flatMap { key =>
      Try(conf.get(key)).toOption.map(key -> _)
    }.toMap
  }
}
