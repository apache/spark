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

package org.apache.spark.ui.jobs

import scala.collection.mutable
import scala.xml.{Node, Unparsed}

import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.ui.jobs.UIData.StageUIData
import org.apache.spark.util.Utils

/** Stage summary grouped by executors. */
private[ui] class ExecutorTable(stageId: Int, stageAttemptId: Int, parent: StagesTab) {
  private val listener = parent.progressListener

  def toNodeSeq: Seq[Node] = {
    listener.synchronized {
      executorTable()
    }
  }

  /** Special table which merges two header cells. */
  private def executorTable[T](): Seq[Node] = {
    val stageData = listener.stageIdToData.get((stageId, stageAttemptId))
    var hasInput = false
    var hasOutput = false
    var hasShuffleWrite = false
    var hasShuffleRead = false
    var hasBytesSpilled = false
    stageData.foreach { data =>
        hasInput = data.hasInput
        hasOutput = data.hasOutput
        hasShuffleRead = data.hasShuffleRead
        hasShuffleWrite = data.hasShuffleWrite
        hasBytesSpilled = data.hasBytesSpilled
    }

    <table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
      <thead>
        <th id="executorid">Executor ID</th>
        <th>Address</th>
        <th>Task Time</th>
        <th>Total Tasks</th>
        <th>Failed Tasks</th>
        <th>Killed Tasks</th>
        <th>Succeeded Tasks</th>
        {if (hasInput) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.INPUT}>Input Size / Records</span>
          </th>
        }}
        {if (hasOutput) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.OUTPUT}>Output Size / Records</span>
          </th>
        }}
        {if (hasShuffleRead) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>
            Shuffle Read Size / Records</span>
          </th>
        }}
        {if (hasShuffleWrite) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.SHUFFLE_WRITE}>
            Shuffle Write Size / Records</span>
          </th>
        }}
        {if (hasBytesSpilled) {
          <th>Shuffle Spill (Memory)</th>
          <th>Shuffle Spill (Disk)</th>
        }}
      </thead>
      <tbody>
        {createExecutorTable()}
      </tbody>
    </table>
    <script>
      {Unparsed {
        """
          |      window.onload = function() {
          |        sorttable.innerSortFunction.apply(document.getElementById('executorid'), [])
          |      };
        """.stripMargin
      }}
    </script>
  }

  private def createExecutorTable() : Seq[Node] = {
    // Make an executor-id -> address map
    val executorIdToAddress = mutable.HashMap[String, String]()
    listener.blockManagerIds.foreach { blockManagerId =>
      val address = blockManagerId.hostPort
      val executorId = blockManagerId.executorId
      executorIdToAddress.put(executorId, address)
    }

    listener.stageIdToData.get((stageId, stageAttemptId)) match {
      case Some(stageData: StageUIData) =>
        stageData.executorSummary.toSeq.sortBy(_._1).map { case (k, v) =>
          <tr>
            <td>
              <div style="float: left">{k}</div>
              <div style="float: right">
              {
                val logs = parent.executorsListener.executorToLogUrls.getOrElse(k, Map.empty)
                logs.map {
                  case (logName, logUrl) => <div><a href={logUrl}>{logName}</a></div>
                }
              }
              </div>
            </td>
            <td>{executorIdToAddress.getOrElse(k, "CANNOT FIND ADDRESS")}</td>
            <td sorttable_customkey={v.taskTime.toString}>{UIUtils.formatDuration(v.taskTime)}</td>
            <td>{v.failedTasks + v.succeededTasks + v.killedTasks}</td>
            <td>{v.failedTasks}</td>
            <td>{v.killedTasks}</td>
            <td>{v.succeededTasks}</td>
            {if (stageData.hasInput) {
              <td sorttable_customkey={v.inputBytes.toString}>
                {s"${Utils.bytesToString(v.inputBytes)} / ${v.inputRecords}"}
              </td>
            }}
            {if (stageData.hasOutput) {
              <td sorttable_customkey={v.outputBytes.toString}>
                {s"${Utils.bytesToString(v.outputBytes)} / ${v.outputRecords}"}
              </td>
            }}
            {if (stageData.hasShuffleRead) {
              <td sorttable_customkey={v.shuffleRead.toString}>
                {s"${Utils.bytesToString(v.shuffleRead)} / ${v.shuffleReadRecords}"}
              </td>
            }}
            {if (stageData.hasShuffleWrite) {
              <td sorttable_customkey={v.shuffleWrite.toString}>
                {s"${Utils.bytesToString(v.shuffleWrite)} / ${v.shuffleWriteRecords}"}
              </td>
            }}
            {if (stageData.hasBytesSpilled) {
              <td sorttable_customkey={v.memoryBytesSpilled.toString}>
                {Utils.bytesToString(v.memoryBytesSpilled)}
              </td>
              <td sorttable_customkey={v.diskBytesSpilled.toString}>
                {Utils.bytesToString(v.diskBytesSpilled)}
              </td>
            }}
          </tr>
        }
      case None =>
        Seq.empty[Node]
    }
  }
}
