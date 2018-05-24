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

import java.util.Locale
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.xml.{Node, NodeSeq, Unparsed, Utility}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._

/** Page showing statistics and stage list for a given job */
private[ui] class JobPage(parent: JobsTab, store: AppStatusStore) extends WebUIPage("job") {

  private val STAGES_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="completed-stage-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Completed</text>
      <rect class="failed-stage-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="active-stage-legend"
        x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Active</text>
    </svg></div>.toString.filter(_ != '\n')

  private val EXECUTORS_LEGEND =
    <div class="legend-area"><svg width="150px" height="55px">
      <rect class="executor-added-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Added</text>
      <rect class="executor-removed-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Removed</text>
    </svg></div>.toString.filter(_ != '\n')

  private def makeStageEvent(stageInfos: Seq[v1.StageData]): Seq[String] = {
    stageInfos.map { stage =>
      val stageId = stage.stageId
      val attemptId = stage.attemptId
      val name = stage.name
      val status = stage.status.toString
      val submissionTime = stage.submissionTime.get.getTime()
      val completionTime = stage.completionTime.map(_.getTime())
        .getOrElse(System.currentTimeMillis())

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a Javascript string literal.
      val escapedName = Utility.escape(name)
      val jsEscapedName = StringEscapeUtils.escapeEcmaScript(escapedName)
      s"""
         |{
         |  'className': 'stage job-timeline-object ${status}',
         |  'group': 'stages',
         |  'start': new Date(${submissionTime}),
         |  'end': new Date(${completionTime}),
         |  'content': '<div class="job-timeline-content" data-toggle="tooltip"' +
         |   'data-placement="top" data-html="true"' +
         |   'data-title="${jsEscapedName} (Stage ${stageId}.${attemptId})<br>' +
         |   'Status: ${status.toUpperCase(Locale.ROOT)}<br>' +
         |   'Submitted: ${UIUtils.formatDate(submissionTime)}' +
         |   '${
                 if (status != "running") {
                   s"""<br>Completed: ${UIUtils.formatDate(completionTime)}"""
                 } else {
                   ""
                 }
              }">' +
         |    '${jsEscapedName} (Stage ${stageId}.${attemptId})</div>',
         |}
       """.stripMargin
    }
  }

  def makeExecutorEvent(executors: Seq[v1.ExecutorSummary]): Seq[String] = {
    val events = ListBuffer[String]()
    executors.foreach { e =>
      val addedEvent =
        s"""
           |{
           |  'className': 'executor added',
           |  'group': 'executors',
           |  'start': new Date(${e.addTime.getTime()}),
           |  'content': '<div class="executor-event-content"' +
           |    'data-toggle="tooltip" data-placement="bottom"' +
           |    'data-title="Executor ${e.id}<br>' +
           |    'Added at ${UIUtils.formatDate(e.addTime)}"' +
           |    'data-html="true">Executor ${e.id} added</div>'
           |}
         """.stripMargin
      events += addedEvent

      e.removeTime.foreach { removeTime =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${removeTime.getTime()}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${e.id}<br>' +
             |    'Removed at ${UIUtils.formatDate(removeTime)}' +
             |    '${
                      e.removeReason.map { reason =>
                        s"""<br>Reason: ${reason.replace("\n", " ")}"""
                      }.getOrElse("")
                   }"' +
             |    'data-html="true">Executor ${e.id} removed</div>'
             |}
           """.stripMargin
          events += removedEvent
      }
    }
    events.toSeq
  }

  private def makeTimeline(
      stages: Seq[v1.StageData],
      executors: Seq[v1.ExecutorSummary],
      appStartTime: Long): Seq[Node] = {

    val stageEventJsonAsStrSeq = makeStageEvent(stages)
    val executorsJsonAsStrSeq = makeExecutorEvent(executors)

    val groupJsonArrayAsStr =
      s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${EXECUTORS_LEGEND}',
          |  },
          |  {
          |    'id': 'stages',
          |    'content': '<div>Stages</div>${STAGES_LEGEND}',
          |  }
          |]
        """.stripMargin

    val eventArrayAsStr =
      (stageEventJsonAsStrSeq ++ executorsJsonAsStrSeq).mkString("[", ",", "]")

    <span class="expand-job-timeline">
      <span class="expand-job-timeline-arrow arrow-closed"></span>
      <a data-toggle="tooltip" title={ToolTips.STAGE_TIMELINE} data-placement="right">
        Event Timeline
      </a>
    </span> ++
    <div id="job-timeline" class="collapsed">
      <div class="control-panel">
        <div id="job-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawJobTimeline(${groupJsonArrayAsStr}, ${eventArrayAsStr}, " +
      s"${appStartTime}, ${UIUtils.getTimeZoneOffset()});")}
    </script>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    // stripXSS is called first to remove suspicious characters used in XSS attacks
    val parameterId = UIUtils.stripXSS(request.getParameter("id"))
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val jobId = parameterId.toInt
    val jobData = store.asOption(store.job(jobId)).getOrElse {
      val content =
        <div id="no-info">
          <p>No information to display for job {jobId}</p>
        </div>
      return UIUtils.headerSparkPage(
        request, s"Details for Job $jobId", content, parent)
    }
    val isComplete = jobData.status != JobExecutionStatus.RUNNING
    val stages = jobData.stageIds.map { stageId =>
      // This could be empty if the listener hasn't received information about the
      // stage or if the stage information has been garbage collected
      store.asOption(store.lastStageAttempt(stageId)).getOrElse {
        new v1.StageData(
          v1.StageStatus.PENDING,
          stageId,
          0, 0, 0, 0, 0, 0, 0,
          0L, 0L, None, None, None, None,
          0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
          "Unknown",
          None,
          "Unknown",
          null,
          Nil,
          Nil,
          None,
          None,
          Map())
      }
    }

    val activeStages = Buffer[v1.StageData]()
    val completedStages = Buffer[v1.StageData]()
    // If the job is completed, then any pending stages are displayed as "skipped":
    val pendingOrSkippedStages = Buffer[v1.StageData]()
    val failedStages = Buffer[v1.StageData]()
    for (stage <- stages) {
      if (stage.submissionTime.isEmpty) {
        pendingOrSkippedStages += stage
      } else if (stage.completionTime.isDefined) {
        if (stage.status == v1.StageStatus.FAILED) {
          failedStages += stage
        } else {
          completedStages += stage
        }
      } else {
        activeStages += stage
      }
    }

    val basePath = "jobs/job"

    val pendingOrSkippedTableId =
      if (isComplete) {
        "pending"
      } else {
        "skipped"
      }

    val activeStagesTable =
      new StageTableBase(store, request, activeStages, "active", "activeStage", parent.basePath,
        basePath, parent.isFairScheduler,
        killEnabled = parent.killEnabled, isFailedStage = false)
    val pendingOrSkippedStagesTable =
      new StageTableBase(store, request, pendingOrSkippedStages, pendingOrSkippedTableId,
        "pendingStage", parent.basePath, basePath, parent.isFairScheduler,
        killEnabled = false, isFailedStage = false)
    val completedStagesTable =
      new StageTableBase(store, request, completedStages, "completed", "completedStage",
        parent.basePath, basePath, parent.isFairScheduler,
        killEnabled = false, isFailedStage = false)
    val failedStagesTable =
      new StageTableBase(store, request, failedStages, "failed", "failedStage", parent.basePath,
        basePath, parent.isFairScheduler,
        killEnabled = false, isFailedStage = true)

    val shouldShowActiveStages = activeStages.nonEmpty
    val shouldShowPendingStages = !isComplete && pendingOrSkippedStages.nonEmpty
    val shouldShowCompletedStages = completedStages.nonEmpty
    val shouldShowSkippedStages = isComplete && pendingOrSkippedStages.nonEmpty
    val shouldShowFailedStages = failedStages.nonEmpty

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          <li>
            <Strong>Status:</Strong>
            {jobData.status}
          </li>
          {
            if (jobData.jobGroup.isDefined) {
              <li>
                <strong>Job Group:</strong>
                {jobData.jobGroup.get}
              </li>
            }
          }
          {
            if (shouldShowActiveStages) {
              <li>
                <a href="#active"><strong>Active Stages:</strong></a>
                {activeStages.size}
              </li>
            }
          }
          {
            if (shouldShowPendingStages) {
              <li>
                <a href="#pending">
                  <strong>Pending Stages:</strong>
                </a>{pendingOrSkippedStages.size}
              </li>
            }
          }
          {
            if (shouldShowCompletedStages) {
              <li>
                <a href="#completed"><strong>Completed Stages:</strong></a>
                {completedStages.size}
              </li>
            }
          }
          {
            if (shouldShowSkippedStages) {
            <li>
              <a href="#skipped"><strong>Skipped Stages:</strong></a>
              {pendingOrSkippedStages.size}
            </li>
          }
          }
          {
            if (shouldShowFailedStages) {
              <li>
                <a href="#failed"><strong>Failed Stages:</strong></a>
                {failedStages.size}
              </li>
            }
          }
        </ul>
      </div>

    var content = summary
    val appStartTime = store.applicationInfo().attempts.head.startTime.getTime()

    content ++= makeTimeline(activeStages ++ completedStages ++ failedStages,
      store.executorList(false), appStartTime)

    val operationGraphContent = store.asOption(store.operationGraphForJob(jobId)) match {
      case Some(operationGraph) => UIUtils.showDagVizForJob(jobId, operationGraph)
      case None =>
        <div id="no-info">
          <p>No DAG visualization information to display for job {jobId}</p>
        </div>
    }
    content ++= operationGraphContent

    if (shouldShowActiveStages) {
      content ++=
        <span id="active" class="collapse-aggregated-activeStages collapse-table"
            onClick="collapseTable('collapse-aggregated-activeStages','aggregated-activeStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Active Stages ({activeStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-activeStages collapsible-table">
          {activeStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowPendingStages) {
      content ++=
        <span id="pending" class="collapse-aggregated-pendingOrSkippedStages collapse-table"
            onClick="collapseTable('collapse-aggregated-pendingOrSkippedStages',
            'aggregated-pendingOrSkippedStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Pending Stages ({pendingOrSkippedStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-pendingOrSkippedStages collapsible-table">
          {pendingOrSkippedStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowCompletedStages) {
      content ++=
        <span id="completed" class="collapse-aggregated-completedStages collapse-table"
            onClick="collapseTable('collapse-aggregated-completedStages',
            'aggregated-completedStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Completed Stages ({completedStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-completedStages collapsible-table">
          {completedStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowSkippedStages) {
      content ++=
        <span id="skipped" class="collapse-aggregated-pendingOrSkippedStages collapse-table"
            onClick="collapseTable('collapse-aggregated-pendingOrSkippedStages',
            'aggregated-pendingOrSkippedStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Skipped Stages ({pendingOrSkippedStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-pendingOrSkippedStages collapsible-table">
          {pendingOrSkippedStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowFailedStages) {
      content ++=
        <span id ="failed" class="collapse-aggregated-failedStages collapse-table"
            onClick="collapseTable('collapse-aggregated-failedStages','aggregated-failedStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Failed Stages ({failedStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-failedStages collapsible-table">
          {failedStagesTable.toNodeSeq}
        </div>
    }
    UIUtils.headerSparkPage(
      request, s"Details for Job $jobId", content, parent, showVisualization = true)
  }
}
