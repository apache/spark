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

import scala.xml.Node
import scala.xml.Text

import java.util.Date

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.util.Utils

/** Page showing list of all ongoing and recently finished stages */
private[ui] class StageTableBase(
    stages: Seq[StageInfo],
    parent: JobProgressTab,
    killEnabled: Boolean = false) {

  private val listener = parent.listener
  protected def isFairScheduler = parent.isFairScheduler

  protected def columns: Seq[Node] = {
    <th>Stage Id</th> ++
    {if (isFairScheduler) {<th>Pool Name</th>} else Seq.empty} ++
    <th>Description</th>
    <th>Submitted</th>
    <th>Duration</th>
    <th>Tasks: Succeeded/Total</th>
    <th><span data-toggle="tooltip" title={ToolTips.INPUT}>Input</span></th>
    <th><span data-toggle="tooltip" title={ToolTips.OUTPUT}>Output</span></th>
    <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>Shuffle Read</span></th>
    <th>
      <!-- Place the shuffle write tooltip on the left (rather than the default position
        of on top) because the shuffle write column is the last column on the right side and
        the tooltip is wider than the column, so it doesn't fit on top. -->
      <span data-toggle="tooltip" data-placement="left" title={ToolTips.SHUFFLE_WRITE}>
        Shuffle Write
      </span>
    </th>
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

  private def makeProgressBar(started: Int, completed: Int, failed: Int, total: Int): Seq[Node] =
  {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    val startWidth = "width: %s%%".format((started.toDouble/total)*100)

    <div class="progress">
      <span style="text-align:center; position:absolute; width:100%; left:0;">
        {completed}/{total} { if (failed > 0) s"($failed failed)" else "" }
      </span>
      <div class="bar bar-completed" style={completeWidth}></div>
      <div class="bar bar-running" style={startWidth}></div>
    </div>
  }

  private def makeDescription(s: StageInfo): Seq[Node] = {
    // scalastyle:off
    val killLink = if (killEnabled) {
      val killLinkUri = "%s/stages/stage/kill?id=%s&terminate=true"
        .format(UIUtils.prependBaseUri(parent.basePath), s.stageId)
      val confirm = "return window.confirm('Are you sure you want to kill stage %s ?');"
        .format(s.stageId)
      <span class="kill-link">
        (<a href={killLinkUri} onclick={confirm}>kill</a>)
      </span>
    }
    // scalastyle:on

    val nameLinkUri ="%s/stages/stage?id=%s&attempt=%s"
      .format(UIUtils.prependBaseUri(parent.basePath), s.stageId, s.attemptId)
    val nameLink = <a href={nameLinkUri}>{s.name}</a>

    val cachedRddInfos = s.rddInfos.filter(_.numCachedPartitions > 0)
    val details = if (s.details.nonEmpty) {
      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
      <div class="stage-details collapsed">
        {if (cachedRddInfos.nonEmpty) {
          Text("RDD: ") ++
          // scalastyle:off
          cachedRddInfos.map { i =>
            <a href={"%s/storage/rdd?id=%d".format(UIUtils.prependBaseUri(parent.basePath), i.id)}>{i.name}</a>
          }
          // scalastyle:on
        }}
        <pre>{s.details}</pre>
      </div>
    }

    val stageDesc = for {
      stageData <- listener.stageIdToData.get((s.stageId, s.attemptId))
      desc <- stageData.description
    } yield {
      <div><em>{desc}</em></div>
    }

    <div>{stageDesc.getOrElse("")} {killLink} {nameLink} {details}</div>
  }

  protected def stageRow(s: StageInfo): Seq[Node] = {
    val stageDataOption = listener.stageIdToData.get((s.stageId, s.attemptId))
    if (stageDataOption.isEmpty) {
      return <td>{s.stageId}</td><td>No data available for this stage</td>
    }

    val stageData = stageDataOption.get
    val submissionTime = s.submissionTime match {
      case Some(t) => UIUtils.formatDate(new Date(t))
      case None => "Unknown"
    }
    val finishTime = s.completionTime.getOrElse(System.currentTimeMillis)
    val duration = s.submissionTime.map { t =>
      if (finishTime > t) finishTime - t else System.currentTimeMillis - t
    }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")

    val inputRead = stageData.inputBytes
    val inputReadWithUnit = if (inputRead > 0) Utils.bytesToString(inputRead) else ""
    val outputWrite = stageData.outputBytes
    val outputWriteWithUnit = if (outputWrite > 0) Utils.bytesToString(outputWrite) else ""
    val shuffleRead = stageData.shuffleReadBytes
    val shuffleReadWithUnit = if (shuffleRead > 0) Utils.bytesToString(shuffleRead) else ""
    val shuffleWrite = stageData.shuffleWriteBytes
    val shuffleWriteWithUnit = if (shuffleWrite > 0) Utils.bytesToString(shuffleWrite) else ""

    {if (s.attemptId > 0) {
      <td>{s.stageId} (retry {s.attemptId})</td>
    } else {
      <td>{s.stageId}</td>
    }} ++
    {if (isFairScheduler) {
      <td>
        <a href={"%s/stages/pool?poolname=%s"
          .format(UIUtils.prependBaseUri(parent.basePath), stageData.schedulingPool)}>
          {stageData.schedulingPool}
        </a>
      </td>
    } else {
      Seq.empty
    }} ++
    <td>{makeDescription(s)}</td>
    <td sorttable_customkey={s.submissionTime.getOrElse(0).toString} valign="middle">
      {submissionTime}
    </td>
    <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
    <td class="progress-cell">
      {makeProgressBar(stageData.numActiveTasks, stageData.completedIndices.size,
        stageData.numFailedTasks, s.numTasks)}
    </td>
    <td sorttable_customkey={inputRead.toString}>{inputReadWithUnit}</td>
    <td sorttable_customkey={outputWrite.toString}>{outputWriteWithUnit}</td>
    <td sorttable_customkey={shuffleRead.toString}>{shuffleReadWithUnit}</td>
    <td sorttable_customkey={shuffleWrite.toString}>{shuffleWriteWithUnit}</td>
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
    val failureReason = s.failureReason.getOrElse("")
    val isMultiline = failureReason.indexOf('\n') >= 0
    // Display the first line by default
    val failureReasonSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        failureReason.substring(0, failureReason.indexOf('\n'))
      } else {
        failureReason
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{failureReason}</pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    val failureReasonHtml = <td valign="middle">{failureReasonSummary}{details}</td>
    basicColumns ++ failureReasonHtml
  }
}
