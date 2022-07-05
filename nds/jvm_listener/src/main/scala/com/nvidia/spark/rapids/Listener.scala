package com.nvidia.spark.rapids.listener

trait Listener {
  /* Listener interface to be implemented at Python side
   */
  def notify(x: Any): Any
}
