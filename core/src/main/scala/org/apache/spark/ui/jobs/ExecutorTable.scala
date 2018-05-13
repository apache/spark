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

import scala.xml.{Node, Unparsed}

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageData
import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.util.Utils

/** Stage summary grouped by executors. */
private[ui] class ExecutorTable(stage: StageData, store: AppStatusStore) {

  import ApiHelper._

  def toNodeSeq: Seq[Node] = {
    <table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
      <thead>
        <th id="executorid">Executor ID</th>
        <th>Address</th>
        <th>Task Time</th>
        <th>Total Tasks</th>
        <th>Failed Tasks</th>
        <th>Killed Tasks</th>
        <th>Succeeded Tasks</th>
        {if (hasInput(stage)) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.INPUT}>Input Size / Records</span>
          </th>
        }}
        {if (hasOutput(stage)) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.OUTPUT}>Output Size / Records</span>
          </th>
        }}
        {if (hasShuffleRead(stage)) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>
            Shuffle Read Size / Records</span>
          </th>
        }}
        {if (hasShuffleWrite(stage)) {
          <th>
            <span data-toggle="tooltip" title={ToolTips.SHUFFLE_WRITE}>
            Shuffle Write Size / Records</span>
          </th>
        }}
        {if (hasBytesSpilled(stage)) {
          <th>Shuffle Spill (Memory)</th>
          <th>Shuffle Spill (Disk)</th>
        }}
        <th>
          <span data-toggle="tooltip" title={ToolTips.BLACKLISTED}>
          Blacklisted
          </span>
        </th>
      </thead>
      <tbody>
        {createExecutorTable(stage)}
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

  private def createExecutorTable(stage: StageData) : Seq[Node] = {
    val executorSummary = store.executorSummary(stage.stageId, stage.attemptId)

    executorSummary.toSeq.sortBy(_._1).map { case (k, v) =>
      val executor = store.asOption(store.executorSummary(k))
      <tr>
        <td>
          <div style="float: left">{k}</div>
          <div style="float: right">
          {
            executor.map(_.executorLogs).getOrElse(Map.empty).map {
              case (logName, logUrl) => <div><a href={logUrl}>{logName}</a></div>
            }
          }
          </div>
        </td>
        <td>{executor.map { e => e.hostPort }.getOrElse("CANNOT FIND ADDRESS")}</td>
        <td sorttable_customkey={v.taskTime.toString}>{UIUtils.formatDuration(v.taskTime)}</td>
        <td>{v.failedTasks + v.succeededTasks + v.killedTasks}</td>
        <td>{v.failedTasks}</td>
        <td>{v.killedTasks}</td>
        <td>{v.succeededTasks}</td>
        {if (hasInput(stage)) {
          <td sorttable_customkey={v.inputBytes.toString}>
            {s"${Utils.bytesToString(v.inputBytes)} / ${v.inputRecords}"}
          </td>
        }}
        {if (hasOutput(stage)) {
          <td sorttable_customkey={v.outputBytes.toString}>
            {s"${Utils.bytesToString(v.outputBytes)} / ${v.outputRecords}"}
          </td>
        }}
        {if (hasShuffleRead(stage)) {
          <td sorttable_customkey={v.shuffleRead.toString}>
            {s"${Utils.bytesToString(v.shuffleRead)} / ${v.shuffleReadRecords}"}
          </td>
        }}
        {if (hasShuffleWrite(stage)) {
          <td sorttable_customkey={v.shuffleWrite.toString}>
            {s"${Utils.bytesToString(v.shuffleWrite)} / ${v.shuffleWriteRecords}"}
          </td>
        }}
        {if (hasBytesSpilled(stage)) {
          <td sorttable_customkey={v.memoryBytesSpilled.toString}>
            {Utils.bytesToString(v.memoryBytesSpilled)}
          </td>
          <td sorttable_customkey={v.diskBytesSpilled.toString}>
            {Utils.bytesToString(v.diskBytesSpilled)}
          </td>
        }}
        {
          if (executor.map(_.isBlacklisted).getOrElse(false)) {
            <td>for application</td>
          } else if (v.isBlacklistedForStage) {
            <td>for stage</td>
          } else {
            <td>false</td>
          }
        }
      </tr>
    }
  }

}
