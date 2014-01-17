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

import java.util.Date

import scala.xml.Node
import scala.collection.mutable.HashSet

import org.apache.spark.scheduler.{SchedulingMode, StageInfo, TaskInfo}
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils


/** Page showing list of all ongoing and recently finished stages */
private[spark] class StageTable(val stages: Seq[StageInfo], val parent: JobProgressUI) {

  val listener = parent.listener
  val dateFmt = parent.dateFmt
  val isFairScheduler = listener.sc.getSchedulingMode == SchedulingMode.FAIR

  def toNodeSeq(): Seq[Node] = {
    listener.synchronized {
      stageTable(stageRow, stages)
    }
  }

  /** Special table which merges two header cells. */
  private def stageTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Stage Id</th>
        {if (isFairScheduler) {<th>Pool Name</th>} else {}}
        <th>Description</th>
        <th>Submitted</th>
        <th>Duration</th>
        <th>Tasks: Succeeded/Total</th>
        <th>Shuffle Read</th>
        <th>Shuffle Write</th>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  private def makeProgressBar(started: Int, completed: Int, failed: String, total: Int): Seq[Node] = {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    val startWidth = "width: %s%%".format((started.toDouble/total)*100)

    <div class="progress">
      <span style="text-align:center; position:absolute; width:100%;">
        {completed}/{total} {failed}
      </span>
      <div class="bar bar-completed" style={completeWidth}></div>
      <div class="bar bar-running" style={startWidth}></div>
    </div>
  }


  private def stageRow(s: StageInfo): Seq[Node] = {
    val submissionTime = s.submissionTime match {
      case Some(t) => dateFmt.format(new Date(t))
      case None => "Unknown"
    }

    val shuffleReadSortable = listener.stageIdToShuffleRead.getOrElse(s.stageId, 0L)
    val shuffleRead = shuffleReadSortable match {
      case 0 => ""
      case b => Utils.bytesToString(b)
    }

    val shuffleWriteSortable = listener.stageIdToShuffleWrite.getOrElse(s.stageId, 0L)
    val shuffleWrite = shuffleWriteSortable match {
      case 0 => ""
      case b => Utils.bytesToString(b)
    }

    val startedTasks = listener.stageIdToTasksActive.getOrElse(s.stageId, HashSet[TaskInfo]()).size
    val completedTasks = listener.stageIdToTasksComplete.getOrElse(s.stageId, 0)
    val failedTasks = listener.stageIdToTasksFailed.getOrElse(s.stageId, 0) match {
        case f if f > 0 => "(%s failed)".format(f)
        case _ => ""
    }
    val totalTasks = s.numTasks

    val poolName = listener.stageIdToPool.get(s.stageId)

    val nameLink =
      <a href={"%s/stages/stage?id=%s".format(UIUtils.prependBaseUri(),s.stageId)}>{s.name}</a>
    val description = listener.stageIdToDescription.get(s.stageId)
      .map(d => <div><em>{d}</em></div><div>{nameLink}</div>).getOrElse(nameLink)
    val finishTime = s.completionTime.getOrElse(System.currentTimeMillis())
    val duration =  s.submissionTime.map(t => finishTime - t)

    <tr>
      <td>{s.stageId}</td>
      {if (isFairScheduler) {
        <td><a href={"%s/stages/pool?poolname=%s".format(UIUtils.prependBaseUri(),poolName.get)}>
          {poolName.get}</a></td>}
      }
      <td>{description}</td>
      <td valign="middle">{submissionTime}</td>
      <td sorttable_customkey={duration.getOrElse(-1).toString}>
        {duration.map(d => parent.formatDuration(d)).getOrElse("Unknown")}
      </td>
      <td class="progress-cell">
        {makeProgressBar(startedTasks, completedTasks, failedTasks, totalTasks)}
      </td>
      <td sorttable_customekey={shuffleReadSortable.toString}>{shuffleRead}</td>
      <td sorttable_customekey={shuffleWriteSortable.toString}>{shuffleWrite}</td>
    </tr>
  }
}
