/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import org.apache.spark.util.Utils

/**
 * Information about a running task attempt inside a TaskSet.
 */
private[spark]
class TaskInfo(
    val taskId: Long,
    val index: Int,
    val launchTime: Long,
    val executorId: String,
    val host: String,
    val taskLocality: TaskLocality.TaskLocality) {

  var finishTime: Long = 0
  var failed = false

  def markSuccessful(time: Long = System.currentTimeMillis) {
    finishTime = time
  }

  def markFailed(time: Long = System.currentTimeMillis) {
    finishTime = time
    failed = true
  }

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed

  def running: Boolean = !finished

  def status: String = {
    if (running)
      "RUNNING"
    else if (failed)
      "FAILED"
    else if (successful)
      "SUCCESS"
    else
      "UNKNOWN"
  }

  def duration: Long = {
    if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished tasks")
    } else {
      finishTime - launchTime
    }
  }

  def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
