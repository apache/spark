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

package org.apache.spark.scheduler

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

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  var gettingResultTime: Long = 0

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  var finishTime: Long = 0

  var failed = false

  var serializedSize: Int = 0

  def markGettingResult(time: Long = System.currentTimeMillis) {
    gettingResultTime = time
  }

  def markSuccessful(time: Long = System.currentTimeMillis) {
    finishTime = time
  }

  def markFailed(time: Long = System.currentTimeMillis) {
    finishTime = time
    failed = true
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      "RUNNING"
    } else if (gettingResult) {
      "GET RESULT"
    } else if (failed) {
      "FAILED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
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
