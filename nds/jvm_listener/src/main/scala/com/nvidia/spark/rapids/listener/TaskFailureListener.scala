package com.nvidia.spark.rapids.listener

import org.apache.spark.{Success, TaskEndReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import scala.collection.mutable.ListBuffer


/* A simple listener which captures SparkListenerTaskEnd,
 * extracts "reason" of the task. If the reason is not "Success",
 * send this reason to python side.
 */
class TaskFailureListener extends SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    taskEnd.reason match {
      case Success =>
      case reason => Manager.notifyAll(reason.toString)
    }
    super.onTaskEnd(taskEnd)
  }
}
