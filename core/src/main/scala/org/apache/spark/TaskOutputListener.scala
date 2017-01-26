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

package org.apache.spark

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler._

/**
 * A SparkListener that monitor the number of records read, generated and produced by each task.
 * The tasks that produce excessive number of output rows are terminated. The threshold can be
 * altered by setting the configurable parameter spark.outputRatioKillThreshold.
 *
 * @param sparkContext
 * @param killRatioThreshold custom input/output ration
 */
class TaskOutputListener(val sparkContext: SparkContext, killRatioThreshold: Long = 0)
    extends SparkListener with Logging {

  // If a task exceeds this output rows to input rows ratio, it will be terminated.
  val KillRatioThreshold =
    if (killRatioThreshold > 0) killRatioThreshold
    else SparkEnv.get.conf.get(config.OUTPUT_RATIO_KILL_THRESHOLD)

  // The tasks can be killed only after this time in seconds has elapsed since their first update.
  val MinRunningTime = 10

  // Tasks currently monitored by the listener.
  val activeTasks = new ConcurrentHashMap[TaskIdentity, TaskMetrics]()

  case class TaskIdentity(stageId: Int, stageAttemptId: Int, taskId: Long)

  /**
   * Helper class for managing the metric updates already received from a given task.
   * Stores only the highest received values of the number of read/generated/produced records.
   */
  class TaskMetrics(val taskIdent: TaskIdentity,
      val trackingStartTime: Long = System.currentTimeMillis(),
      var recordsIn: Long = 0,
      var recordsGen: Long = 0,
      var recordsOut: Long = 0,
      var cancelRequestIssued: Boolean = false) {

    // Analyze the provided metric update.
    def update(metricName: String, metricUpdate: Long): Unit = {
      if (metricName.contains("recordsRead") && (recordsIn < metricUpdate)) {
        recordsIn = metricUpdate
      } else if (metricName.contains("generated rows") && (recordsGen < metricUpdate)) {
        recordsGen = metricUpdate
      } else if (metricName.contains("output rows") && (recordsOut < metricUpdate)) {
        recordsOut = metricUpdate
      } else {
        return
      }
      logDebug(s"Updated task $taskIdent with $metricName = $metricUpdate.")
      checkOutputRatio()
    }

    // Compare the (rows produced) to (rows read + rows generated) with the threshold.
    def checkOutputRatio(): Unit = {
      if (KillRatioThreshold < 0) return
      val runningTime = (System.currentTimeMillis() - trackingStartTime) / 1000
      val outputRatio =
        if (recordsIn + recordsGen == 0) 0
        else recordsOut / (recordsIn + recordsGen)

      if (runningTime > MinRunningTime && outputRatio > KillRatioThreshold &&
          !cancelRequestIssued) {
        terminateTask(outputRatio)
      }
    }

    // Request cancellation of the job to which this task belongs.
    def terminateTask(outputRatio: Long): Unit = {
      val taskId = taskIdent.taskId
      val sumRecordsIn = recordsGen + recordsIn
      logWarning(s"Task $taskId has exceeded the maximum allowed ratio of input to output " +
        s"records (in = $sumRecordsIn, out = $recordsOut, ratio = $outputRatio) and will be " +
        s"terminated.")
      var msg = s"Task $taskId exceeded the maximum allowed ratio of input to output records " +
        s"(1 to $outputRatio, max allowed 1 to $KillRatioThreshold); this limit can be " +
        s"modified with configuration parameter spark.outputRatioKillThreshold"
      sparkContext.cancelStage(taskIdent.stageId, msg)
      cancelRequestIssued = true
    }
  }

  def removeTaskMetrics(stageId: Int, stageAttemptId: Int, taskId: Long): Unit = {
    val taskIdent = TaskIdentity(stageId, stageAttemptId, taskId)
    activeTasks.remove(taskIdent)
  }

  def getTaskMetrics(stageId: Int, stageAttemptId: Int, taskId : Long): TaskMetrics = {
    val taskIdent = TaskIdentity(stageId, stageAttemptId, taskId)
    Option(activeTasks.get(taskIdent)) match {
      case Some(existingTaskMetrics) => existingTaskMetrics
      case _ =>
        val newTaskMetrics = new TaskMetrics(taskIdent)
        activeTasks.put(taskIdent, newTaskMetrics)
        newTaskMetrics
    }
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    executorMetricsUpdate.accumUpdates.foreach { au =>
      val taskMetrics = getTaskMetrics(au._2, au._3, au._1)

      au._4.foreach { upd =>
        val metricName = upd.name.toString()
        upd.update match {
          case Some(n: Number) =>
            taskMetrics.update(metricName, n.longValue())
          case _ =>
        }
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    removeTaskMetrics(taskEnd.stageId, taskEnd.stageAttemptId, taskEnd.taskInfo.taskId)
  }
}
