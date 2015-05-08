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
package org.apache.spark.status.api.v1

import javax.ws.rs.{PathParam, GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.JobUIData

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneJobResource(ui: SparkUI) {

  @GET
  def oneJob(@PathParam("jobId") jobId: Int): JobData = {
    val statusToJobs: Seq[(JobExecutionStatus, Seq[JobUIData])] =
      AllJobsResource.getStatusToJobs(ui)
    val jobOpt = statusToJobs.map {_._2} .flatten.find { jobInfo => jobInfo.jobId == jobId}
    jobOpt.map { job =>
      AllJobsResource.convertJobData(job, ui.jobProgressListener, false)
    }.getOrElse {
      throw new NotFoundException("unknown job: " + jobId)
    }
  }

}
