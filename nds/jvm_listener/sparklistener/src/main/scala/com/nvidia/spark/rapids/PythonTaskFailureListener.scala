package com.nvidia.spark

import org.apache.spark.{Success, TaskEndReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import scala.collection.mutable.ListBuffer


/* A simple listener which captures SparkListenerTaskEnd,
 * extracts "reason" of the task. If the reason is not "Success",
 * send this reason to python side.
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
