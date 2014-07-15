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

import scala.xml.Node

import org.apache.spark.storage.StorageLevel
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/** Summary information about an executor to display in the UI. */
private case class ExecutorSummaryInfo(
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
    maxMemory: Long)

private[ui] class ExecutorsPage(parent: ExecutorsTab) extends WebUIPage("") {
  private val appName = parent.appName
  private val basePath = parent.basePath
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val storageStatusList = listener.storageStatusList
    val maxMem = storageStatusList.map(_.maxMem).fold(0L)(_ + _)
    val memUsed = storageStatusList.map(_.memUsed).fold(0L)(_ + _)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_ + _)
    val execInfo = for (statusId <- 0 until storageStatusList.size) yield getExecInfo(statusId)
    val execInfoSorted = execInfo.sortBy(_.id)

    val execTable =
      <table class={UIUtils.TABLE_CLASS}>
        <thead>
          <th>Executor ID</th>
          <th>Address</th>
          <th>RDD Blocks</th>
          <th>Memory Used</th>
          <th>Disk Used</th>
          <th>Active Tasks</th>
          <th>Failed Tasks</th>
          <th>Complete Tasks</th>
          <th>Total Tasks</th>
          <th>Task Time</th>
          <th><span data-toggle="tooltip" title={ToolTips.INPUT}>Input</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>Shuffle Read</span></th>
          <th>
            <!-- Place the shuffle write tooltip on the left (rather than the default position
              of on top) because the shuffle write column is the last column on the right side and
              the tooltip is wider than the column, so it doesn't fit on top. -->
            <span data-toggle="tooltip" data-placement="left" title={ToolTips.SHUFFLE_WRITE}>
              Shuffle Write
            </span>
          </th>
        </thead>
        <tbody>
          {execInfoSorted.map(execRow(_))}
        </tbody>
      </table>

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

    UIUtils.headerSparkPage(content, basePath, appName, "Executors (" + execInfo.size + ")",
      parent.headerTabs, parent)
  }

  /** Render an HTML row representing an executor */
  private def execRow(info: ExecutorSummaryInfo): Seq[Node] = {
    val maximumMemory = info.maxMemory
    val memoryUsed = info.memoryUsed
    val diskUsed = info.diskUsed
    <tr>
      <td>{info.id}</td>
      <td>{info.hostPort}</td>
      <td>{info.rddBlocks}</td>
      <td sorttable_customkey={memoryUsed.toString}>
        {Utils.bytesToString(memoryUsed)} /
        {Utils.bytesToString(maximumMemory)}
      </td>
      <td sorttable_customkey={diskUsed.toString}>
        {Utils.bytesToString(diskUsed)}
      </td>
      <td>{info.activeTasks}</td>
      <td>{info.failedTasks}</td>
      <td>{info.completedTasks}</td>
      <td>{info.totalTasks}</td>
      <td sorttable_customkey={info.totalDuration.toString}>
        {Utils.msDurationToString(info.totalDuration)}
      </td>
      <td sorttable_customkey={info.totalInputBytes.toString}>
        {Utils.bytesToString(info.totalInputBytes)}
      </td>
      <td sorttable_customkey={info.totalShuffleRead.toString}>
        {Utils.bytesToString(info.totalShuffleRead)}
      </td>
      <td sorttable_customkey={info.totalShuffleWrite.toString}>
        {Utils.bytesToString(info.totalShuffleWrite)}
      </td>
    </tr>
  }

  /** Represent an executor's info as a map given a storage status index */
  private def getExecInfo(statusId: Int): ExecutorSummaryInfo = {
    val status = listener.storageStatusList(statusId)
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.blocks.size
    val memUsed = status.memUsed
    val maxMem = status.maxMem
    val diskUsed = status.diskUsed
    val activeTasks = listener.executorToTasksActive.getOrElse(execId, 0)
    val failedTasks = listener.executorToTasksFailed.getOrElse(execId, 0)
    val completedTasks = listener.executorToTasksComplete.getOrElse(execId, 0)
    val totalTasks = activeTasks + failedTasks + completedTasks
    val totalDuration = listener.executorToDuration.getOrElse(execId, 0L)
    val totalInputBytes = listener.executorToInputBytes.getOrElse(execId, 0L)
    val totalShuffleRead = listener.executorToShuffleRead.getOrElse(execId, 0L)
    val totalShuffleWrite = listener.executorToShuffleWrite.getOrElse(execId, 0L)

    new ExecutorSummaryInfo(
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
      totalInputBytes,
      totalShuffleRead,
      totalShuffleWrite,
      maxMem
    )
  }
}
