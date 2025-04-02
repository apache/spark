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

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.errors.SparkCoreErrors

/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside a TaskSet.
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,
    /**
     * The index of this task within its task set. Not necessarily the same as the ID of the RDD
     * partition that the task is computing.
     */
    val index: Int,
    val attemptNumber: Int,
    /**
     * The actual RDD partition ID in this task.
     * The ID of the RDD partition is always same across task attempts.
     * This will be -1 for historical data, and available for all applications since Spark 3.3.
     */
    val partitionId: Int,
    val launchTime: Long,
    val executorId: String,
    val host: String,
    val taskLocality: TaskLocality.TaskLocality,
    val speculative: Boolean) extends Cloneable {

  /**
   * This api doesn't contains partitionId, please use the new api.
   * Remain it for backward compatibility before Spark 3.3.
   */
  def this(
      taskId: Long,
      index: Int,
      attemptNumber: Int,
      launchTime: Long,
      executorId: String,
      host: String,
      taskLocality: TaskLocality.TaskLocality,
      speculative: Boolean) = {
    this(taskId, index, attemptNumber, -1, launchTime, executorId, host, taskLocality, speculative)
  }

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
   */
  def accumulables: Seq[AccumulableInfo] = _accumulables

  private[this] var _accumulables: Seq[AccumulableInfo] = Nil

  private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
    _accumulables = newAccumulables
  }

  override def clone(): TaskInfo = super.clone().asInstanceOf[TaskInfo]

  private[scheduler] def cloneWithEmptyAccumulables(): TaskInfo = {
    val cloned = clone()
    cloned.setAccumulables(Nil)
    cloned
  }

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  var finishTime: Long = 0

  var failed = false

  var killed = false

  var launching = true

  private[spark] def markGettingResult(time: Long): Unit = {
    gettingResultTime = time
  }

  private[spark] def markFinished(state: TaskState, time: Long): Unit = {
    // finishTime should be set larger than 0, otherwise "finished" below will return false.
    assert(time > 0)
    finishTime = time
    failed = state == TaskState.FAILED
    killed = state == TaskState.KILLED
  }

  private[spark] def launchSucceeded(): Unit = {
    launching = false
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed && !killed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  def id: String = s"$index.$attemptNumber"

  def duration: Long = {
    if (!finished) {
      throw SparkCoreErrors.durationCalledOnUnfinishedTaskError()
    } else {
      finishTime - launchTime
    }
  }

  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
