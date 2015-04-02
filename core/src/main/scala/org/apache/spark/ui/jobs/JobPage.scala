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

import scala.collection.mutable
import scala.xml.{NodeSeq, Node, Unparsed}

import javax.servlet.http.HttpServletRequest

import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.ui.TimelineViewUtils._

/** Page showing statistics and stage list for a given job */
private[ui] class JobPage(parent: JobsTab) extends WebUIPage("job") {
  private val listener = parent.listener

  private val controlPanel : Seq[Node] = {
    <div class="control-panel">
      <div id="job-timeline-zoom-lock">
        <input type="checkbox" checked="checked"></input>
        <span>Zoom Lock</span>
      </div>
    </div>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val jobId = parameterId.toInt
      val jobDataOption = listener.jobIdToData.get(jobId)
      if (jobDataOption.isEmpty) {
        val content =
          <div>
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
          new StageInfo(stageId, 0, "Unknown", 0, Seq.empty, "Unknown"))
      }

      val activeStages = mutable.Buffer[StageInfo]()
      val completedStages = mutable.Buffer[StageInfo]()
      // If the job is completed, then any pending stages are displayed as "skipped":
      val pendingOrSkippedStages = mutable.Buffer[StageInfo]()
      val failedStages = mutable.Buffer[StageInfo]()
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
          parent.basePath, parent.listener, isFairScheduler = parent.isFairScheduler,
          killEnabled = parent.killEnabled)
      val pendingOrSkippedStagesTable =
        new StageTableBase(pendingOrSkippedStages.sortBy(_.stageId).reverse,
          parent.basePath, parent.listener, isFairScheduler = parent.isFairScheduler,
          killEnabled = false)
      val completedStagesTable =
        new StageTableBase(completedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, isFairScheduler = parent.isFairScheduler, killEnabled = false)
      val failedStagesTable =
        new FailedStageTable(failedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, isFairScheduler = parent.isFairScheduler)

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

      val groupArrayStr =
        s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${nodesToFlatString(executorsLegend)}',
          |  },
          |  {
          |    'id': 'stages',
          |    'content': '<div>Stages</div>${nodesToFlatString(stagesLegend)}',
          |  }
          |]
        """.stripMargin

      val stageEventArray = (activeStages ++ completedStages ++ failedStages).map { stage =>
        val stageId = stage.stageId
        val attemptId = stage.attemptId
        val status = {
          if (stage.completionTime.isDefined) {
            if (stage.failureReason.isDefined) {
              "failed"
            } else {
              "succeeded"
            }
          } else {
            "running"
          }
        }

        val submissionTime = stage.submissionTime.get
        val completionTime = stage.completionTime.getOrElse(System.currentTimeMillis())

        s"""
           |{
           |  'className': 'stage job-timeline-object ${status}',
           |  'group': 'stages',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="job-timeline-content">' +
           |    'Stage ${stageId}.${attemptId}</div>',
           |  'title': 'Stage ${stageId}.${attemptId}\\nStatus: ${status.toUpperCase}\\n' +
           |    'Submission Time: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |    '${
                   if (status != "running") {
                     s"""\\nCompletion Time: ${UIUtils.formatDate(new Date(completionTime))}"""
                   } else {
                     ""
                   }
                 }'
           |}
         """.stripMargin
      }

      val executorAddedEventArray = listener.executorIdToAddedTime.map {
        case (executorId, addedTime) =>
          s"""
            |{
            |  'className': 'executor application-tmeline-object added',
            |  'group': 'executors',
            |  'start': new Date(${addedTime}),
            |  'content': '<div>Executor ${executorId} added</div>',
            |  'title': 'Added at ${UIUtils.formatDate(new Date(addedTime))}'
            |}
          """.stripMargin
      }

      val executorRemovedEventArray = listener.executorIdToRemovedTimeAndReason.map {
        case (executorId, (removedTime, reason)) =>
          s"""
            |{
            |  'className': 'executor application-timeline-object removed',
            |  'group': 'executors',
            |  'start': new Date(${removedTime}),
            |  'content': '<div>Executor ${executorId} removed (${reason})</div>',
            |  'title': 'Removed at ${UIUtils.formatDate(new Date(removedTime))}\\n' +
            |    'Reason: ${reason}'
            |}
          """.stripMargin
      }

      val eventArrayStr =
        (stageEventArray ++ executorAddedEventArray ++
          executorRemovedEventArray).mkString("[", ",", "]")

      content ++= <h4>Events on Job Timeline</h4> ++ controlPanel ++
        <div id="job-timeline"></div>
      content ++=
        <script type="text/javascript">
          {Unparsed(s"drawJobTimeline(${groupArrayStr}, ${eventArrayStr});")}
        </script>

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
      UIUtils.headerSparkPage(s"Details for Job $jobId", content, parent)
    }
  }
}
