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

import javax.servlet.http.HttpServletRequest

import org.apache.spark.scheduler.StageInfo

import scala.xml.{NodeSeq, Node}

import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing statistics and stage list for a given job */
private[ui] class JobPage(parent: JobsTab) extends WebUIPage("job") {
  private val listener = parent.listener
  private val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val jobId = request.getParameter("id").toInt
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
      val stages = jobData.stageIds.map { stageId =>
        listener.stageIdToInfo.getOrElse(stageId,
          new StageInfo(stageId, 0, "Unknown", 0, Seq.empty, "Unknown"))
      }

      val (activeStages, completedOrFailedStages) = stages.partition(_.completionTime.isDefined)
      val (failedStages, completedStages) =
        completedOrFailedStages.partition(_.failureReason.isDefined)

      val activeStagesTable =
        new StageTableBase(activeStages.sortBy(_.submissionTime).reverse,
          parent.basePath, parent.listener, parent.killEnabled)
      val completedStagesTable =
        new StageTableBase(completedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, parent.killEnabled)
      val failedStagesTable =
        new FailedStageTable(failedStages.sortBy(_.submissionTime).reverse, parent.basePath,
          parent.listener, parent.killEnabled)

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {
              if (jobData.jobGroup.isDefined) {
                <li>
                  <strong>Job Group:</strong>
                  {jobData.jobGroup.get}
                </li>
              } else Seq.empty
            }
            <li>
              <a href="#active"><strong>Active Stages:</strong></a>
              {activeStages.size}
            </li>
            <li>
              <a href="#completed"><strong>Completed Stages:</strong></a>
              {completedStages.size}
            </li>
            <li>
              <a href="#failed"><strong>Failed Stages:</strong></a>
              {failedStages.size}
            </li>
          </ul>
        </div>

      val content = summary ++
        <h4 id="active">Active Stages ({activeStages.size})</h4> ++
        activeStagesTable.toNodeSeq ++
        <h4 id="completed">Completed Stages ({completedStages.size})</h4> ++
        completedStagesTable.toNodeSeq ++
        <h4 id ="failed">Failed Stages ({failedStages.size})</h4> ++
        failedStagesTable.toNodeSeq
      UIUtils.headerSparkPage(s"Details for Job $jobId", content, parent)
    }
  }
}
