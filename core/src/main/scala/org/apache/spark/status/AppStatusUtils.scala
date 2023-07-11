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

package org.apache.spark.status

import org.apache.spark.status.api.v1.TaskData

private[spark] object AppStatusUtils {

  private val TASK_FINISHED_STATES = Set("FAILED", "KILLED", "SUCCESS")

  private def isTaskFinished(task: TaskData): Boolean = {
    TASK_FINISHED_STATES.contains(task.status)
  }

  def schedulerDelay(task: TaskData): Long = {
    if (isTaskFinished(task) && task.taskMetrics.isDefined && task.duration.isDefined) {
      val m = task.taskMetrics.get
      schedulerDelay(task.launchTime.getTime(), fetchStart(task), task.duration.get,
        m.executorDeserializeTime, m.resultSerializationTime, m.executorRunTime)
    } else {
      // The task is still running and the metrics like executorRunTime are not available.
      0L
    }
  }

  def gettingResultTime(task: TaskData): Long = {
    gettingResultTime(task.launchTime.getTime(), fetchStart(task), task.duration.getOrElse(-1L))
  }

  def schedulerDelay(
      launchTime: Long,
      fetchStart: Long,
      duration: Long,
      deserializeTime: Long,
      serializeTime: Long,
      runTime: Long): Long = {
    math.max(0, duration - runTime - deserializeTime - serializeTime -
      gettingResultTime(launchTime, fetchStart, duration))
  }

  def gettingResultTime(launchTime: Long, fetchStart: Long, duration: Long): Long = {
    if (fetchStart > 0) {
      if (duration > 0) {
        launchTime + duration - fetchStart
      } else {
        System.currentTimeMillis() - fetchStart
      }
    } else {
      0L
    }
  }

  private def fetchStart(task: TaskData): Long = {
    if (task.resultFetchStart.isDefined) {
      task.resultFetchStart.get.getTime()
    } else {
      -1
    }
  }

  def getQuantilesValue(
    values: IndexedSeq[Double],
    quantiles: Array[Double]): IndexedSeq[Double] = {
    val count = values.size
    if (count > 0) {
      val indices = quantiles.map { q => math.min((q * count).toLong, count - 1) }
      indices.map(i => values(i.toInt)).toIndexedSeq
    } else {
      IndexedSeq.fill(quantiles.length)(0.0)
    }
  }
}
