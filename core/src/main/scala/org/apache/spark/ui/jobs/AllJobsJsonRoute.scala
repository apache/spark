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

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.{InputMetrics => InternalInputMetrics, OutputMetrics => InternalOutputMetrics, ShuffleReadMetrics => InternalShuffleReadMetrics, ShuffleWriteMetrics => InternalShuffleWriteMetrics, TaskMetrics => InternalTaskMetrics}
import org.apache.spark.status.api._
import org.apache.spark.status.{RouteUtils, JsonRequestHandler, StatusJsonRoute}
import org.apache.spark.ui.jobs.UIData.JobUIData

class AllJobsJsonRoute(parent: JsonRequestHandler) extends StatusJsonRoute[Seq[JobData]] {

  override def renderJson(request: HttpServletRequest): Seq[JobData] = {
    parent.withSparkUI(request){case(ui, request) =>
      val statusToJobs = ui.jobProgressListener.synchronized{
        Seq(
          JobExecutionStatus.RUNNING -> ui.jobProgressListener.activeJobs.values.toSeq,
          JobExecutionStatus.SUCCEEDED -> ui.jobProgressListener.completedJobs.toSeq,
          JobExecutionStatus.FAILED -> ui.jobProgressListener.failedJobs.reverse.toSeq
        )
      }
      val statusSet = RouteUtils.extractParamSet(request, "status", JobExecutionStatus.values())
      for {
        (status, jobs) <- statusToJobs
        job <- jobs if statusSet.contains(status)
      } yield {
        AllJobsJsonRoute.convertJobData(job, ui.jobProgressListener, false)
      }
    }
  }
}

private[jobs] object AllJobsJsonRoute {
  def convertJobData(
    job: JobUIData,
    listener: JobProgressListener,
    includeStageDetails: Boolean
  ): JobData = {
    listener.synchronized {
      val lastStageInfo = listener.stageIdToInfo.get(job.stageIds.max)
      val lastStageData = lastStageInfo.flatMap { s =>
        listener.stageIdToData.get((s.stageId, s.attemptId))
      }
      val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap(_.description)
      JobData(
        jobId = job.jobId,
        name = lastStageName,
        description = lastStageDescription,
        submissionTime = job.submissionTime,
        completionTime = job.completionTime,
        stageIds = job.stageIds,
        jobGroup = job.jobGroup,
        status = job.status,
        numTasks = job.numTasks,
        numActiveTasks = job.numActiveTasks,
        numCompletedTasks = job.numCompletedTasks,
        numSkippedTasks = job.numCompletedTasks,
        numFailedTasks = job.numFailedTasks,
        numActiveStages = job.numActiveStages,
        numCompletedStages = job.completedStageIndices.size,
        numSkippedStages = job.numSkippedStages,
        numFailedStages = job.numFailedStages
      )
    }
  }
}
