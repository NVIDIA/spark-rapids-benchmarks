/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package com.nvidia.spark.rapids.listener

import org.apache.spark.SparkContext

object Manager {
  /* Manager class to manage all extra customized listeners.
  */
  private var listeners: Map[String, Listener] = Map()
  private val spark_listener = new TaskFailureListener()
  private var isRegistered = false

  def register(listener: Listener): String = {
    /* Note this register method has nothing to do with SparkContext.addSparkListener method.
    * This method is only to provide an interface to developers to have a better control over
    * all customized listeners.
    */
    this.synchronized {
      // We register to the spark listener when the first listener is registered.
      registerSparkListener()
      val uuid = java.util.UUID.randomUUID().toString
      listeners = listeners + (uuid -> listener)
      uuid
    }
  }

  def unregister(uuid: String) = {
    this.synchronized {
      listeners = listeners - uuid
    }
  }

  def notifyAll(message: String): Unit = {
    for { (_, listener) <- listeners } listener.notify(message)
  }

  def registerSparkListener() : Unit = {
    if (!isRegistered) {
      SparkContext.getOrCreate().addSparkListener(spark_listener)
      isRegistered = true
    }
  }

  def unregisterSparkListener() : Unit = {
    if (isRegistered) {
      SparkContext.getOrCreate().removeSparkListener(spark_listener)
      isRegistered = false
    }
  }
}
