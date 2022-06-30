package com.nvidia.spark

trait Listener {
  /* This will be implemented by a Python class.
   * You can of course use more specific types, 
   * for example here String => Unit */
  def notify(x: Any): Any
}