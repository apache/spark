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

import scala.collection.mutable.{HashMap, HashSet}
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import org.apache.spark.{ExceptionFailure, Logging, SparkContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerTaskStart, SparkListenerTaskEnd, SparkListener}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.Page.Executors
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils


private[spark] class ExecutorsUI(val sc: SparkContext) {

  private var _listener: Option[ExecutorsListener] = None
  def listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener)
    sc.addSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = sc.getExecutorStorageStatus

    val maxMem = storageStatusList.map(_.maxMem).fold(0L)(_+_)
    val memUsed = storageStatusList.map(_.memUsed()).fold(0L)(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_+_)

    val execHead = Seq("Executor ID", "Address", "RDD blocks", "Memory used", "Disk used",
      "Active tasks", "Failed tasks", "Complete tasks", "Total tasks")

    def execRow(kv: Seq[String]) = {
      <tr>
        <td>{kv(0)}</td>
        <td>{kv(1)}</td>
        <td>{kv(2)}</td>
        <td sorttable_customkey={kv(3)}>
          {Utils.bytesToString(kv(3).toLong)} / {Utils.bytesToString(kv(4).toLong)}
        </td>
        <td sorttable_customkey={kv(5)}>
          {Utils.bytesToString(kv(5).toLong)}
        </td>
        <td>{kv(6)}</td>
        <td>{kv(7)}</td>
        <td>{kv(8)}</td>
        <td>{kv(9)}</td>
      </tr>
    }

    val execInfo = for (statusId <- 0 until storageStatusList.size) yield getExecInfo(statusId)
    val execTable = UIUtils.listingTable(execHead, execRow, execInfo)

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

    UIUtils.headerSparkPage(content, sc, "Executors (" + execInfo.size + ")", Executors)
  }

  def getExecInfo(statusId: Int): Seq[String] = {
    val status = sc.getExecutorStorageStatus(statusId)
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.blocks.size.toString
    val memUsed = status.memUsed().toString
    val maxMem = status.maxMem.toString
    val diskUsed = status.diskUsed().toString
    val activeTasks = listener.executorToTasksActive.getOrElse(execId, HashSet.empty[Long]).size
    val failedTasks = listener.executorToTasksFailed.getOrElse(execId, 0)
    val completedTasks = listener.executorToTasksComplete.getOrElse(execId, 0)
    val totalTasks = activeTasks + failedTasks + completedTasks

    Seq(
      execId,
      hostPort,
      rddBlocks,
      memUsed,
      maxMem,
      diskUsed,
      activeTasks.toString,
      failedTasks.toString,
      completedTasks.toString,
      totalTasks.toString
    )
  }

  private[spark] class ExecutorsListener extends SparkListener with Logging {
    val executorToTasksActive = HashMap[String, HashSet[TaskInfo]]()
    val executorToTasksComplete = HashMap[String, Int]()
    val executorToTasksFailed = HashMap[String, Int]()

    override def onTaskStart(taskStart: SparkListenerTaskStart) {
      val eid = taskStart.taskInfo.executorId
      val activeTasks = executorToTasksActive.getOrElseUpdate(eid, new HashSet[TaskInfo]())
      activeTasks += taskStart.taskInfo
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      val eid = taskEnd.taskInfo.executorId
      val activeTasks = executorToTasksActive.getOrElseUpdate(eid, new HashSet[TaskInfo]())
      activeTasks -= taskEnd.taskInfo
      val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
        taskEnd.reason match {
          case e: ExceptionFailure =>
            executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
            (Some(e), e.metrics)
          case _ =>
            executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
            (None, Option(taskEnd.taskMetrics))
        }
    }
  }
}
