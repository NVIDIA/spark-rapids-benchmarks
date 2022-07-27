package com.nvidia.spark.rapids.listener


object Manager {
  /* Manager class to manage all extra customized listeners.
  */
  var listeners: Map[String, Listener] = Map()

  def register(listener: Listener): String = {
    /* Note this register method has nothing to do with SparkContext.addSparkListener method.
    * This method is only to provide an interface to developers to have a better control over
    * all customized listeners.
    */
    this.synchronized {
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
    val listener = new TaskFailureListener
    SparkContext.getOrCreate().addSparkListener(listener)
  }
}
