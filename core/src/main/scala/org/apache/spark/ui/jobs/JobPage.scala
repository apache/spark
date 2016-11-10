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

import scala.collection.mutable.{Buffer, HashMap, ListBuffer}
import scala.xml.{NodeSeq, Node, Unparsed, Utility}

import javax.servlet.http.HttpServletRequest

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}
import org.apache.spark.ui.jobs.UIData.ExecutorUIData

/** Page showing statistics and stage list for a given job */
private[ui] class JobPage(parent: JobsTab) extends WebUIPage("job") {

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

  private def makeStageEvent(stageInfos: Seq[StageInfo]): Seq[String] = {
    stageInfos.map { stage =>
      val stageId = stage.stageId
      val attemptId = stage.attemptId
      val name = stage.name
      val status = stage.getStatusString
      val submissionTime = stage.submissionTime.get
      val completionTime = stage.completionTime.getOrElse(System.currentTimeMillis())

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
         |   'Status: ${status.toUpperCase}<br>' +
         |   'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
         |   '${
                 if (status != "running") {
                   s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
                 } else {
                   ""
                 }
              }">' +
         |    '${jsEscapedName} (Stage ${stageId}.${attemptId})</div>',
         |}
       """.stripMargin
    }
  }

  def makeExecutorEvent(executorUIDatas: HashMap[String, ExecutorUIData]): Seq[String] = {
    val events = ListBuffer[String]()
    executorUIDatas.foreach {
      case (executorId, event) =>
        val addedEvent =
          s"""
             |{
             |  'className': 'executor added',
             |  'group': 'executors',
             |  'start': new Date(${event.startTime}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${executorId}<br>' +
             |    'Added at ${UIUtils.formatDate(new Date(event.startTime))}"' +
             |    'data-html="true">Executor ${executorId} added</div>'
             |}
           """.stripMargin
        events += addedEvent

        if (event.finishTime.isDefined) {
          val removedEvent =
            s"""
               |{
               |  'className': 'executor removed',
               |  'group': 'executors',
               |  'start': new Date(${event.finishTime.get}),
               |  'content': '<div class="executor-event-content"' +
               |    'data-toggle="tooltip" data-placement="bottom"' +
               |    'data-title="Executor ${executorId}<br>' +
               |    'Removed at ${UIUtils.formatDate(new Date(event.finishTime.get))}' +
               |    '${
                        if (event.finishReason.isDefined) {
                          s"""<br>Reason: ${event.finishReason.get.replace("\n", " ")}"""
                        } else {
                          ""
                        }
                     }"' +
               |    'data-html="true">Executor ${executorId} removed</div>'
               |}
             """.stripMargin
            events += removedEvent
        }
    }
    events.toSeq
  }

  private def makeTimeline(
      stages: Seq[StageInfo],
      executors: HashMap[String, ExecutorUIData],
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
      {Unparsed(s"drawJobTimeline(${groupJsonArrayAsStr}, ${eventArrayAsStr}, ${appStartTime});")}
    </script>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener

    listener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val jobId = parameterId.toInt
      val jobDataOption = listener.jobIdToData.get(jobId)
      if (jobDataOption.isEmpty) {
        val content =
          <div id="no-info">
            <p>No information to display for job {jobId}</p>
          </div>
        return UIUtils.headerSparkPage(
          s"Details for Job $jobId", content, parent)
      }
      val jobData = jobDataOption.get
      val isComplete = jobData.status != JobExecutionStatus.RUNNING
      val stages = jobData.stageIds.map { stageId =>
        // This could be empty if the JobProgressListener hasn't received information about the
        // stage or if the stage information has been garbage collected
        listener.stageIdToInfo.getOrElse(stageId,
          new StageInfo(stageId, 0, "Unknown", 0, Seq.empty, Seq.empty, "Unknown"))
      }

      val activeStages = Buffer[StageInfo]()
      val completedStages = Buffer[StageInfo]()
      // If the job is completed, then any pending stages are displayed as "skipped":
      val pendingOrSkippedStages = Buffer[StageInfo]()
      val failedStages = Buffer[StageInfo]()
      for (stage <- stages) {
        if (stage.submissionTime.isEmpty) {
          pendingOrSkippedStages += stage
        } else if (stage.completionTime.isDefined) {
          if (stage.failureReason.isDefined) {
            failedStages += stage
          } else {
            completedStages += stage
          }
        } else {
          activeStages += stage
        }
      }

      val activeStagesTable =
        new StageTableBase(activeStages.sortBy(_.submissionTime).reverse,
          parent.basePath, parent.jobProgresslistener, isFairScheduler = parent.isFairScheduler,
          killEnabled = parent.killEnabled)
      val pendingOrSkippedStagesTable =
        new StageTableBase(pendingOrSkippedStages.sortBy(_.stageId).reverse,
          parent.basePath, parent.jobProgresslistener, isFairScheduler = parent.isFairScheduler,
          killEnabled = false)
      val completedStagesTable =
        new StageTableBase(completedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.jobProgresslistener, isFairScheduler = parent.isFairScheduler, killEnabled = false)
      val failedStagesTable =
        new FailedStageTable(failedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.jobProgresslistener, isFairScheduler = parent.isFairScheduler)

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
      val appStartTime = listener.startTime
      val executorListener = parent.executorListener
      val operationGraphListener = parent.operationGraphListener

      content ++= makeTimeline(activeStages ++ completedStages ++ failedStages,
          executorListener.executorIdToData, appStartTime)

      content ++= UIUtils.showDagVizForJob(
        jobId, operationGraphListener.getOperationGraphForJob(jobId))

      if (shouldShowActiveStages) {
        content ++= <h4 id="active">Active Stages ({activeStages.size})</h4> ++
          activeStagesTable.toNodeSeq
      }
      if (shouldShowPendingStages) {
        content ++= <h4 id="pending">Pending Stages ({pendingOrSkippedStages.size})</h4> ++
          pendingOrSkippedStagesTable.toNodeSeq
      }
      if (shouldShowCompletedStages) {
        content ++= <h4 id="completed">Completed Stages ({completedStages.size})</h4> ++
          completedStagesTable.toNodeSeq
      }
      if (shouldShowSkippedStages) {
        content ++= <h4 id="skipped">Skipped Stages ({pendingOrSkippedStages.size})</h4> ++
          pendingOrSkippedStagesTable.toNodeSeq
      }
      if (shouldShowFailedStages) {
        content ++= <h4 id ="failed">Failed Stages ({failedStages.size})</h4> ++
          failedStagesTable.toNodeSeq
      }
      UIUtils.headerSparkPage(s"Details for Job $jobId", content, parent, showVisualization = true)
    }
  }
}
