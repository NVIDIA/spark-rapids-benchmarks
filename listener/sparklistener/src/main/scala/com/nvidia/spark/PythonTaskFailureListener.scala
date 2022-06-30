package com.nvidia.spark

import org.apache.spark.{Success, TaskEndReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import scala.collection.mutable.ListBuffer


/* A simple listener which captures SparkListenerTaskEnd,
 * extracts numbers of records written by the task
 * and converts to JSON. You can of course add handlers 
 * for other events as well.
 */
class PythonTaskFailureListener extends SparkListener {
  val taskFailures = ListBuffer[TaskEndReason]()
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    taskEnd.reason match {
      case Success =>
      case reason => Manager.notifyAll(reason.toString)
    }
    super.onTaskEnd(taskEnd)
  }
}