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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.ExceptionFailure
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.Page.Executors
import org.apache.spark.ui.{SparkUI, UIUtils}
import org.apache.spark.util.Utils

private[ui] class ExecutorsUI(parent: SparkUI) {
  private val basePath = parent.basePath
  private var _listener: Option[ExecutorsListener] = None

  private def appName = parent.appName

  lazy val listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener(parent.storageStatusListener))
  }

  def getHandlers = Seq[ServletContextHandler](
    createServletHandler("/executors",
      (request: HttpServletRequest) => render(request), parent.securityManager, basePath)
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = listener.storageStatusList
    val maxMem = storageStatusList.map(_.maxMem).fold(0L)(_ + _)
    val memUsed = storageStatusList.map(_.memUsed()).fold(0L)(_ + _)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_ + _)
    val execInfo = for (statusId <- 0 until storageStatusList.size) yield getExecInfo(statusId)
    val execInfoSorted = execInfo.sortBy(_.getOrElse("Executor ID", ""))
    val execTable = UIUtils.listingTable(execHeader, execRow, execInfoSorted)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Memory:</strong>
              {Utils.bytesToString(memUsed)} Used
              ({Utils.bytesToString(maxMem)} Total) </li>
            <li><strong>Disk:</strong> {Utils.bytesToString(diskSpaceUsed)} Used </li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTable}
        </div>
      </div>;

    UIUtils.headerSparkPage(
      content, basePath, appName, "Executors (" + execInfo.size + ")", Executors)
  }

  /** Header fields for the executors table */
  private def execHeader = Seq(
    "Executor ID",
    "Address",
    "RDD Blocks",
    "Memory Used",
    "Disk Used",
    "Active Tasks",
    "Failed Tasks",
    "Complete Tasks",
    "Total Tasks",
    "Task Time",
    "Shuffle Read",
    "Shuffle Write")

  /** Render an HTML row representing an executor */
  private def execRow(values: Map[String, String]): Seq[Node] = {
    val maximumMemory = values("Maximum Memory")
    val memoryUsed = values("Memory Used")
    val diskUsed = values("Disk Used")
    <tr>
      <td>{values("Executor ID")}</td>
      <td>{values("Address")}</td>
      <td>{values("RDD Blocks")}</td>
      <td sorttable_customkey={memoryUsed}>
        {Utils.bytesToString(memoryUsed.toLong)} /
        {Utils.bytesToString(maximumMemory.toLong)}
      </td>
      <td sorttable_customkey={diskUsed}>
        {Utils.bytesToString(diskUsed.toLong)}
      </td>
      <td>{values("Active Tasks")}</td>
      <td>{values("Failed Tasks")}</td>
      <td>{values("Complete Tasks")}</td>
      <td>{values("Total Tasks")}</td>
      <td>{Utils.msDurationToString(values("Task Time").toLong)}</td>
      <td>{Utils.bytesToString(values("Shuffle Read").toLong)}</td>
      <td>{Utils.bytesToString(values("Shuffle Write").toLong)}</td>
    </tr>
  }

  /** Represent an executor's info as a map given a storage status index */
  private def getExecInfo(statusId: Int): Map[String, String] = {
    val status = listener.storageStatusList(statusId)
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.blocks.size
    val memUsed = status.memUsed()
    val maxMem = status.maxMem
    val diskUsed = status.diskUsed()
    val activeTasks = listener.executorToTasksActive.getOrElse(execId, 0)
    val failedTasks = listener.executorToTasksFailed.getOrElse(execId, 0)
    val completedTasks = listener.executorToTasksComplete.getOrElse(execId, 0)
    val totalTasks = activeTasks + failedTasks + completedTasks
    val totalDuration = listener.executorToDuration.getOrElse(execId, 0)
    val totalShuffleRead = listener.executorToShuffleRead.getOrElse(execId, 0)
    val totalShuffleWrite = listener.executorToShuffleWrite.getOrElse(execId, 0)

    // Also include fields not in the header
    val execFields = execHeader ++ Seq("Maximum Memory")

    val execValues = Seq(
      execId,
      hostPort,
      rddBlocks,
      memUsed,
      diskUsed,
      activeTasks,
      failedTasks,
      completedTasks,
      totalTasks,
      totalDuration,
      totalShuffleRead,
      totalShuffleWrite,
      maxMem
    ).map(_.toString)

    execFields.zip(execValues).toMap
  }
}

/**
 * A SparkListener that prepares information to be displayed on the ExecutorsUI
 */
private[ui] class ExecutorsListener(storageStatusListener: StorageStatusListener)
  extends SparkListener {

  val executorToTasksActive = HashMap[String, Int]()
  val executorToTasksComplete = HashMap[String, Int]()
  val executorToTasksFailed = HashMap[String, Int]()
  val executorToDuration = HashMap[String, Long]()
  val executorToShuffleRead = HashMap[String, Long]()
  val executorToShuffleWrite = HashMap[String, Long]()

  def storageStatusList = storageStatusListener.storageStatusList

  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val eid = formatExecutorId(taskStart.taskInfo.executorId)
    executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 0) + 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = formatExecutorId(info.executorId)
      executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 1) - 1
      executorToDuration(eid) = executorToDuration.getOrElse(eid, 0L) + info.duration
      taskEnd.reason match {
        case e: ExceptionFailure =>
          executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
        case _ =>
          executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
      }

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        metrics.shuffleReadMetrics.foreach { shuffleRead =>
          executorToShuffleRead(eid) =
            executorToShuffleRead.getOrElse(eid, 0L) + shuffleRead.remoteBytesRead
        }
        metrics.shuffleWriteMetrics.foreach { shuffleWrite =>
          executorToShuffleWrite(eid) =
            executorToShuffleWrite.getOrElse(eid, 0L) + shuffleWrite.shuffleBytesWritten
        }
      }
    }
  }

  // This addresses executor ID inconsistencies in the local mode
  private def formatExecutorId(execId: String) = storageStatusListener.formatExecutorId(execId)
}
