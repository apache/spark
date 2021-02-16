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
import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[ui] class JobsTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "jobs") {

  val sc = parent.sc
  val killEnabled = parent.killEnabled

  // Show pool information for only live UI.
  def isFairScheduler: Boolean = {
    sc.isDefined &&
    store
      .environmentInfo()
      .sparkProperties
      .contains((SCHEDULER_MODE.key, SchedulingMode.FAIR.toString))
  }

  def getSparkUser: String = parent.getSparkUser

  attachPage(new AllJobsPage(this, store))
  attachPage(new JobPage(this, store))

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      Option(request.getParameter("id")).map(_.toInt).foreach { id =>
        store.asOption(store.job(id)).foreach { job =>
          if (job.status == JobExecutionStatus.RUNNING) {
            sc.foreach(_.cancelJob(id))
            // Do a quick pause here to give Spark time to kill the job so it shows up as
            // killed after the refresh. Note that this will block the serving thread so the
            // time should be limited in duration.
            Thread.sleep(100)
          }
        }
      }
    }
  }
}
