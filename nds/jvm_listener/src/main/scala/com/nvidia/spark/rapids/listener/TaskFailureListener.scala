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

import org.apache.spark.{Success, TaskEndReason, TaskFailedReason, TaskKilled}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import scala.collection.mutable.ListBuffer


/* A simple listener which captures SparkListenerTaskEnd,
 * extracts "reason" of the task. If the reason is not "Success",
 * send this reason to python side.
 */
class TaskFailureListener extends SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    taskEnd.reason match {
      case Success => ()
      case _: TaskKilled => ()
      case failedReason: TaskFailedReason => Manager.notifyAll(failedReason.toErrorString)
    }
    super.onTaskEnd(taskEnd)
  }
}
