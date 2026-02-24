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

import com.nvidia.sparkrapids.envdetector.model.{NetworkInfo, NetworkInterface => NetInterface}
import org.apache.spark.SparkContext

import java.net.{InetAddress, NetworkInterface}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Detects network configuration across the cluster.
 */
object NetworkDetector {

  def detect(sc: SparkContext): NetworkInfo = {
    // Get local network interfaces with detailed info
    val localInterfaces = getLocalNetworkInterfaces

    // Detect network type (internal vs external)
    val networkType = detectNetworkType(localInterfaces)

    // Measure latency to executors
    val executorLatencies = measureExecutorLatencies(sc)

    // Note: Bandwidth estimation removed - use --benchmark for accurate network tests
    NetworkInfo(
      networkType = networkType,
      bandwidth = None,  // Use benchmark for accurate measurement
      latency = calculateAverageLatency(executorLatencies),
      interfaces = localInterfaces,
      driverToExecutorLatency = executorLatencies
    )
  }

  /**
   * Get all network interfaces on the driver node with detailed info.
   * Filters out container/virtual interfaces and prioritizes physical interfaces.
   */
  private def getLocalNetworkInterfaces: Seq[NetInterface] = {
    Try {
      val javaInterfaces = NetworkInterface.getNetworkInterfaces.asScala.toSeq
      
      val allInterfaces = javaInterfaces.flatMap { ni =>
        val addresses = ni.getInetAddresses.asScala.toSeq
        if (addresses.isEmpty) {
          Seq.empty
        } else {
          // Get MTU
          val mtu = Try(ni.getMTU).toOption.filter(_ > 0)
          
          // Get MAC address
          val macAddress = Try {
            val mac = ni.getHardwareAddress
            if (mac != null && mac.nonEmpty) {
              Some(mac.map(b => String.format("%02X", Byte.box(b))).mkString(":"))
            } else {
              None
            }
          }.toOption.flatten
          
          // Detect interface type and speed
          val (interfaceType, speed) = detectInterfaceTypeAndSpeed(ni.getName)
          
          addresses.map { addr =>
            NetInterface(
              name = ni.getName,
              ipAddress = addr.getHostAddress,
              isUp = ni.isUp,
              isLoopback = ni.isLoopback,
              mtu = mtu,
              interfaceType = interfaceType,
              speed = speed,
              macAddress = macAddress
            )
          }
        }
      }
      
      // Filter to only show important interfaces (physical NICs, IB, loopback)
      // Exclude all container/overlay virtual interfaces
      allInterfaces.filter(isImportantInterface)
    }.getOrElse(Seq.empty)
  }
  
  /**
   * Determine if a network interface is important for reporting.
   * Important = physical network cards, InfiniBand, loopback
   * Not important = veth (container pairs), docker bridges, overlay networks
   */
  private def isImportantInterface(ni: NetInterface): Boolean = {
    val name = ni.name
    
    // Loopback is always important (but only show IPv4)
    if (ni.isLoopback) {
      return !ni.ipAddress.contains(":")  // Only IPv4 loopback (127.0.0.1)
    }
    
    // Always important: physical NICs, InfiniBand, bonded interfaces
    val importantPrefixes = Seq("eth", "en", "em", "ib", "mlx", "bond", "eno", "ens", "enp")
    val isPhysical = importantPrefixes.exists(p => name.startsWith(p))
    
    // Always exclude: container/overlay virtual interfaces
    val excludedPrefixes = Seq("veth", "cali", "tunl", "flannel", "cni", "docker", "br-", "virbr")
    val isContainerInterface = excludedPrefixes.exists(p => name.startsWith(p))
    
    if (isContainerInterface) {
      false
    } else if (isPhysical) {
      // For physical interfaces, prefer IPv4 addresses
      !ni.ipAddress.contains("%")  // Exclude link-local IPv6 with zone ID
    } else {
      // For other interfaces, only include if UP and has real IPv4
      val hasRealIPv4 = !ni.ipAddress.startsWith("fe80:") && 
                        !ni.ipAddress.startsWith("169.254.") &&
                        !ni.ipAddress.contains("%") &&
                        !ni.ipAddress.contains(":")
      ni.isUp && hasRealIPv4
    }
  }

  /**
   * Detect interface type (Ethernet, InfiniBand, RoCE, etc.) and speed.
   */
  private def detectInterfaceTypeAndSpeed(ifName: String): (Option[String], Option[String]) = {
    // Determine interface type based on name patterns
    val interfaceType = if (ifName.startsWith("ib") || ifName.startsWith("mlx")) {
      Some("InfiniBand")
    } else if (ifName.startsWith("roce") || ifName.startsWith("rdma")) {
      Some("RoCE")
    } else if (ifName.startsWith("eth") || ifName.startsWith("en") || ifName.startsWith("em")) {
      Some("Ethernet")
    } else if (ifName.startsWith("bond")) {
      Some("Bonded")
    } else if (ifName.startsWith("veth") || ifName.startsWith("docker") || ifName.startsWith("br-")) {
      Some("Virtual")
    } else if (ifName.startsWith("lo")) {
      Some("Loopback")
    } else if (ifName.startsWith("wl") || ifName.startsWith("wlan")) {
      Some("WiFi")
    } else {
      None
    }
    
    // Try to get link speed from /sys/class/net
    val speed = getInterfaceSpeed(ifName)
    
    (interfaceType, speed)
  }

  /**
   * Get interface speed from /sys/class/net.
   */
  private def getInterfaceSpeed(ifName: String): Option[String] = {
    // Try to read speed from sysfs
    val speedFromSysfs = Try {
      val speedPath = s"/sys/class/net/$ifName/speed"
      val speedFile = new java.io.File(speedPath)
      if (speedFile.exists) {
        val source = scala.io.Source.fromFile(speedFile)
        try {
          val speedMbps = source.mkString.trim.toInt
          if (speedMbps > 0) {
            val speedStr = if (speedMbps >= 100000) {
              f"${speedMbps / 1000}%d Gb/s"
            } else if (speedMbps >= 1000) {
              f"${speedMbps / 1000}%d Gb/s"
            } else {
              f"$speedMbps Mb/s"
            }
            Some(speedStr)
          } else {
            None
          }
        } finally {
          source.close()
        }
      } else {
        None
      }
    }.toOption.flatten

    // For InfiniBand, try ibstat
    speedFromSysfs.orElse {
      if (ifName.startsWith("ib") || ifName.startsWith("mlx")) {
        getInfiniBandSpeed(ifName)
      } else {
        None
      }
    }
  }

  /**
   * Get InfiniBand interface speed using ibstat.
   */
  private def getInfiniBandSpeed(ifName: String): Option[String] = {
    Try {
      import scala.sys.process._
      
      // Try ibstat first
      val output = "ibstat".!!
      
      // Parse rate from ibstat output
      val ratePattern = """Rate:\s+(\d+)""".r
      ratePattern.findFirstMatchIn(output).map { m =>
        val rateGbps = m.group(1).toInt
        s"$rateGbps Gb/s (IB)"
      }
    }.toOption.flatten.orElse {
      // Try to read from sysfs for IB
      Try {
        import scala.sys.process._
        val output = s"cat /sys/class/infiniband/*/ports/*/rate 2>/dev/null".!!
        val lines = output.trim.split("\n").filter(_.nonEmpty)
        if (lines.nonEmpty) {
          Some(lines.head.trim)
        } else {
          None
        }
      }.toOption.flatten
    }
  }

  /**
   * Detect if the cluster is using internal or external network.
   */
  private def detectNetworkType(interfaces: Seq[NetInterface]): String = {
    val hasPrivateIp = interfaces.exists { ni =>
      val ip = ni.ipAddress
      ip.startsWith("10.") || 
      ip.startsWith("172.16.") || ip.startsWith("172.17.") || ip.startsWith("172.18.") ||
      ip.startsWith("172.19.") || ip.startsWith("172.2") || ip.startsWith("172.30.") ||
      ip.startsWith("172.31.") ||
      ip.startsWith("192.168.")
    }

    if (hasPrivateIp) "internal" else "external"
  }

  /**
   * Measure round-trip latency to each executor.
   */
  private def measureExecutorLatencies(sc: SparkContext): Map[String, Long] = {
    val numPartitions = math.max(sc.defaultParallelism, 10)
    
    val startTime = System.currentTimeMillis()
    
    val latencies = sc.parallelize(1 to numPartitions, numPartitions)
      .mapPartitions { _ =>
        val sparkEnv = org.apache.spark.SparkEnv.get
        val executorId = if (sparkEnv != null) sparkEnv.executorId else "unknown"
        val timestamp = System.currentTimeMillis()
        Iterator((executorId, timestamp))
      }
      .collect()
      .toSeq
      .groupBy(_._1).map(_._2.head).toSeq
      .filter(_._1 != "driver")

    latencies.map { case (execId, execTime) =>
      // Calculate round-trip time approximation
      val latency = (System.currentTimeMillis() - startTime) / 2
      execId -> latency
    }.toMap
  }

  /**
   * Calculate average latency across all executors.
   */
  private def calculateAverageLatency(latencies: Map[String, Long]): Option[String] = {
    if (latencies.isEmpty) {
      None
    } else {
      val avgLatency = latencies.values.sum.toDouble / latencies.size
      Some(f"$avgLatency%.2f ms (average)")
    }
  }
}
