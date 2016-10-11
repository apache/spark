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

import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.ui.{SparkUI, SparkUITab}

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[ui] class JobsTab(parent: SparkUI) extends SparkUITab(parent, "jobs") {
  val sc = parent.sc
  val killEnabled = parent.killEnabled
  val jobProgresslistener = parent.jobProgressListener
  val executorListener = parent.executorsListener
  val operationGraphListener = parent.operationGraphListener

  def isFairScheduler: Boolean =
    jobProgresslistener.schedulingMode == Some(SchedulingMode.FAIR)

  def getSparkUser: String = parent.getSparkUser

  attachPage(new AllJobsPage(this))
  attachPage(new JobPage(this))

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && (parent.securityManager.checkModifyPermissions(request.getRemoteUser))) {
      val killFlag = Option(request.getParameter("terminate")).getOrElse("false").toBoolean
      val jobId = Option(request.getParameter("id")).getOrElse("-1").toInt
      if (jobId >= 0 && killFlag && jobProgresslistener.activeJobs.contains(jobId)) {
        sc.get.cancelJob(jobId)
      }
      // Do a quick pause here to give Spark time to kill the job so it shows up as
      // killed after the refresh. Note that this will block the serving thread so the
      // time should be limited in duration.
      Thread.sleep(100)
    }
  }
}
