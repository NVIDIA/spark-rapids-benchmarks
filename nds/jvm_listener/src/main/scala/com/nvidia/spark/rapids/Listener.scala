package com.nvidia.spark.rapids

trait Listener {
  /* Listener interface to be implemented at Python side
   */
  def notify(x: Any): Any
}
