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

package org.apache.spark.ui.exec

import scala.collection.mutable.{LinkedHashMap, ListBuffer}

import org.apache.spark.{ExceptionFailure, Resubmitted, SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.{StorageStatus, StorageStatusListener}
import org.apache.spark.ui.{SparkUI, SparkUITab}

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {
  val listener = parent.executorsListener
  val sc = parent.sc
  val threadDumpEnabled =
    sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

  attachPage(new ExecutorsPage(this, threadDumpEnabled))
  if (threadDumpEnabled) {
    attachPage(new ExecutorThreadDumpPage(this))
  }
}

private[ui] case class ExecutorTaskSummary(
    var executorId: String,
    var totalCores: Int = 0,
    var tasksMax: Int = 0,
    var tasksActive: Int = 0,
    var tasksFailed: Int = 0,
    var tasksComplete: Int = 0,
    var duration: Long = 0L,
    var jvmGCTime: Long = 0L,
    var inputBytes: Long = 0L,
    var inputRecords: Long = 0L,
    var outputBytes: Long = 0L,
    var outputRecords: Long = 0L,
    var shuffleRead: Long = 0L,
    var shuffleWrite: Long = 0L,
    var executorLogs: Map[String, String] = Map.empty,
    var isAlive: Boolean = true
)

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the ExecutorsTab
 */
@DeveloperApi
class ExecutorsListener(storageStatusListener: StorageStatusListener, conf: SparkConf)
    extends SparkListener {
  var executorToTaskSummary = LinkedHashMap[String, ExecutorTaskSummary]()
  var executorEvents = new ListBuffer[SparkListenerEvent]()

  private val maxTimelineExecutors = conf.getInt("spark.ui.timeline.executors.maximum", 1000)
  private val retainedDeadExecutors = conf.getInt("spark.ui.retainedDeadExecutors", 100)
  private var deadExecutorCount = 0

  def activeStorageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  def deadStorageStatusList: Seq[StorageStatus] = storageStatusListener.deadStorageStatusList

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    if (!executorToTaskSummary.contains(eid)) {
      executorToTaskSummary(eid) = ExecutorTaskSummary(eid)
    }
    executorToTaskSummary(eid).executorLogs = executorAdded.executorInfo.logUrlMap
    executorToTaskSummary(eid).totalCores = executorAdded.executorInfo.totalCores
    executorToTaskSummary(eid).tasksMax =
      executorToTaskSummary(eid).totalCores / conf.getInt("spark.task.cpus", 1)
    executorEvents += executorAdded
    if (executorEvents.size > maxTimelineExecutors) {
      executorEvents.remove(0)
    }
    if (deadExecutorCount > retainedDeadExecutors) {
      val head = executorToTaskSummary.filter(e => !e._2.isAlive).head
      executorToTaskSummary.remove(head._1)
      deadExecutorCount -= 1
    }
  }

  override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    executorEvents += executorRemoved
    if (executorEvents.size > maxTimelineExecutors) {
      executorEvents.remove(0)
    }
    deadExecutorCount += 1
    executorToTaskSummary(executorRemoved.executorId).isAlive = false
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationStart.driverLogs.foreach { logs =>
      val storageStatus = activeStorageStatusList.find { s =>
        s.blockManagerId.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER ||
        s.blockManagerId.executorId == SparkContext.DRIVER_IDENTIFIER
      }
      storageStatus.foreach { s =>
        val eid = s.blockManagerId.executorId
        if (!executorToTaskSummary.contains(eid)) {
          executorToTaskSummary(eid) = ExecutorTaskSummary(eid)
        }
        executorToTaskSummary(eid).executorLogs = logs.toMap
      }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    if (!executorToTaskSummary.contains(eid)) {
      executorToTaskSummary(eid) = ExecutorTaskSummary(eid)
    }
    executorToTaskSummary(eid).tasksActive += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId
      if (!executorToTaskSummary.contains(eid)) {
        executorToTaskSummary(eid) = ExecutorTaskSummary(eid)
      }
      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure =>
          executorToTaskSummary(eid).tasksFailed += 1
        case _ =>
          executorToTaskSummary(eid).tasksComplete += 1
      }
      if (executorToTaskSummary(eid).tasksActive >= 1) {
        executorToTaskSummary(eid).tasksActive -= 1
      }
      executorToTaskSummary(eid).duration += info.duration

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        executorToTaskSummary(eid).inputBytes += metrics.inputMetrics.bytesRead
        executorToTaskSummary(eid).inputRecords += metrics.inputMetrics.recordsRead
        executorToTaskSummary(eid).outputBytes += metrics.outputMetrics.bytesWritten
        executorToTaskSummary(eid).outputRecords += metrics.outputMetrics.recordsWritten

        executorToTaskSummary(eid).shuffleRead += metrics.shuffleReadMetrics.remoteBytesRead
        executorToTaskSummary(eid).shuffleWrite += metrics.shuffleWriteMetrics.bytesWritten
        executorToTaskSummary(eid).jvmGCTime += metrics.jvmGCTime
      }
    }
  }

}
