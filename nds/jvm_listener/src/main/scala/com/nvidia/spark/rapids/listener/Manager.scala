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
      if (!isRegistered) {
        SparkContext.getOrCreate().addSparkListener(spark_listener)
        isRegistered = true
      }
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
