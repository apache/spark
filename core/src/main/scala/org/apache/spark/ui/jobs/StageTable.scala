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

import scala.collection.mutable.HashMap
import scala.xml.Node

import org.apache.spark.scheduler.{StageInfo, TaskInfo}
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils

/** Page showing list of all ongoing and recently finished stages */
private[ui] class StageTableBase(
    stages: Seq[StageInfo],
    parent: JobProgressTab,
    killEnabled: Boolean = false) {

  private val basePath = parent.basePath
  private val listener = parent.listener
  protected def isFairScheduler = parent.isFairScheduler

  protected def columns: Seq[Node] = {
    <th>Stage Id</th> ++
    {if (isFairScheduler) {<th>Pool Name</th>} else Seq.empty} ++
    <th>Description</th>
    <th>Submitted</th>
    <th>Duration</th>
    <th>Tasks: Succeeded/Total</th>
    <th>Shuffle Read</th>
    <th>Shuffle Write</th>
  }

  def toNodeSeq: Seq[Node] = {
    listener.synchronized {
      stageTable(renderStageRow, stages)
    }
  }

  /** Special table that merges two header cells. */
  protected def stageTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>{columns}</thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  private def makeProgressBar(started: Int, completed: Int, failed: String, total: Int): Seq[Node] =
  {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    val startWidth = "width: %s%%".format((started.toDouble/total)*100)

    <div class="progress">
      <span style="text-align:center; position:absolute; width:100%; left:0;">
        {completed}/{total} {failed}
      </span>
      <div class="bar bar-completed" style={completeWidth}></div>
      <div class="bar bar-running" style={startWidth}></div>
    </div>
  }

  private def makeDescription(s: StageInfo): Seq[Node] = {
    // scalastyle:off
    val killLink = if (killEnabled) {
      <span class="kill-link">
        (<a href={"%s/stages/stage/kill?id=%s&terminate=true".format(UIUtils.prependBaseUri(basePath), s.stageId)}>kill</a>)
      </span>
    }
    // scalastyle:on

    val nameLink =
      <a href={"%s/stages/stage?id=%s".format(UIUtils.prependBaseUri(basePath), s.stageId)}>
        {s.name}
      </a>

    val details = if (s.details.nonEmpty) (
      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +show details
      </span>
      <pre class="stage-details collapsed">{s.details}</pre>
    )

    listener.stageIdToDescription.get(s.stageId)
      .map(d => <div><em>{d}</em></div><div>{nameLink} {killLink}</div>)
      .getOrElse(<div>{killLink} {nameLink} {details}</div>)
  }

  protected def stageRow(s: StageInfo): Seq[Node] = {
    val poolName = listener.stageIdToPool.get(s.stageId)
    val submissionTime = s.submissionTime match {
      case Some(t) => UIUtils.formatDate(new Date(t))
      case None => "Unknown"
    }
    val finishTime = s.completionTime.getOrElse(System.currentTimeMillis)
    val duration = s.submissionTime.map { t =>
      if (finishTime > t) finishTime - t else System.currentTimeMillis - t
    }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
    val startedTasks =
      listener.stageIdToTasksActive.getOrElse(s.stageId, HashMap[Long, TaskInfo]()).size
    val completedTasks = listener.stageIdToTasksComplete.getOrElse(s.stageId, 0)
    val failedTasks = listener.stageIdToTasksFailed.getOrElse(s.stageId, 0) match {
      case f if f > 0 => "(%s failed)".format(f)
      case _ => ""
    }
    val totalTasks = s.numTasks
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
    <td>{s.stageId}</td> ++
    {if (isFairScheduler) {
      <td>
        <a href={"%s/stages/pool?poolname=%s"
          .format(UIUtils.prependBaseUri(basePath), poolName.get)}>
          {poolName.get}
        </a>
      </td>
    } else {
      Seq.empty
    }} ++
    <td>{makeDescription(s)}</td>
    <td valign="middle">{submissionTime}</td>
    <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
    <td class="progress-cell">
      {makeProgressBar(startedTasks, completedTasks, failedTasks, totalTasks)}
    </td>
    <td sorttable_customekey={shuffleReadSortable.toString}>{shuffleRead}</td>
    <td sorttable_customekey={shuffleWriteSortable.toString}>{shuffleWrite}</td>
  }

  /** Render an HTML row that represents a stage */
  private def renderStageRow(s: StageInfo): Seq[Node] = <tr>{stageRow(s)}</tr>
}

private[ui] class FailedStageTable(
    stages: Seq[StageInfo],
    parent: JobProgressTab,
    killEnabled: Boolean = false)
  extends StageTableBase(stages, parent, killEnabled) {

  override protected def columns: Seq[Node] = super.columns ++ <th>Failure Reason</th>

  override protected def stageRow(s: StageInfo): Seq[Node] = {
    val basicColumns = super.stageRow(s)
    val failureReason = <td valign="middle">{s.failureReason.getOrElse("")}</td>
    basicColumns ++ failureReason
  }
}
