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

import java.net.URLEncoder
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

// This isn't even used anymore -- but we need to keep it b/c of a MiMa false positive
private[ui] case class ExecutorSummaryInfo(
    id: String,
    hostPort: String,
    rddBlocks: Int,
    memoryUsed: Long,
    diskUsed: Long,
    activeTasks: Int,
    failedTasks: Int,
    completedTasks: Int,
    totalTasks: Int,
    totalDuration: Long,
    totalInputBytes: Long,
    totalShuffleRead: Long,
    totalShuffleWrite: Long,
    maxMemory: Long,
    executorLogs: Map[String, String])


private[ui] class ExecutorsPage(
    parent: ExecutorsTab,
    threadDumpEnabled: Boolean)
  extends WebUIPage("") {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div>
        {
          <div id="active-executors"></div> ++
          <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri("/static/executorspage.js")}></script> ++
          <script>setThreadDumpEnabled({threadDumpEnabled})</script>
        }
      </div>;

    UIUtils.headerSparkPage("Executors", content, parent, useDataTables = true)
  }
}

private[spark] object ExecutorsPage {
  /** Represent an executor's info as a map given a storage status index */
  def getExecInfo(
      listener: ExecutorsListener,
      statusId: Int,
      isActive: Boolean): ExecutorSummary = {
    val status = if (isActive) {
      listener.activeStorageStatusList(statusId)
    } else {
      listener.deadStorageStatusList(statusId)
    }
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.numBlocks
    val memUsed = status.memUsed
    val maxMem = status.maxMem
    val diskUsed = status.diskUsed
    val totalCores = listener.executorToTaskSummary.get(execId).map(_.totalCores).getOrElse(0)
    val maxTasks = listener.executorToTaskSummary.get(execId).map(_.tasksMax).getOrElse(0)
    val activeTasks = listener.executorToTaskSummary.get(execId).map(_.tasksActive).getOrElse(0)
    val failedTasks = listener.executorToTaskSummary.get(execId).map(_.tasksFailed).getOrElse(0)
    val completedTasks = listener.executorToTaskSummary.get(execId)
      .map(_.tasksComplete).getOrElse(0)
    val totalTasks = activeTasks + failedTasks + completedTasks
    val totalDuration = listener.executorToTaskSummary.get(execId).map(_.duration).getOrElse(0L)
    val totalGCTime = listener.executorToTaskSummary.get(execId).map(_.jvmGCTime).getOrElse(0L)
    val totalInputBytes = listener.executorToTaskSummary.get(execId).map(_.inputBytes).getOrElse(0L)
    val totalShuffleRead = listener.executorToTaskSummary.get(execId)
      .map(_.shuffleRead).getOrElse(0L)
    val totalShuffleWrite = listener.executorToTaskSummary.get(execId)
      .map(_.shuffleWrite).getOrElse(0L)
    val executorLogs = listener.executorToTaskSummary.get(execId)
      .map(_.executorLogs).getOrElse(Map.empty)

    new ExecutorSummary(
      execId,
      hostPort,
      isActive,
      rddBlocks,
      memUsed,
      diskUsed,
      totalCores,
      maxTasks,
      activeTasks,
      failedTasks,
      completedTasks,
      totalTasks,
      totalDuration,
      totalGCTime,
      totalInputBytes,
      totalShuffleRead,
      totalShuffleWrite,
      maxMem,
      executorLogs
    )
  }
}
