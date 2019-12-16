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
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils}

/** Web UI showing progress status of all stages in the given SparkContext. */
private[ui] class StagesTab(val parent: SparkUI, val store: AppStatusStore)
  extends SparkUITab(parent, "stages") {

  val sc = parent.sc
  val conf = parent.conf
  val killEnabled = parent.killEnabled

  attachPage(new AllStagesPage(this))
  attachPage(new StagePage(this, store))
  attachPage(new PoolPage(this))

  // Show pool information for only live UI.
  def isFairScheduler: Boolean = {
    sc.isDefined &&
    store
      .environmentInfo()
      .sparkProperties
      .contains(("spark.scheduler.mode", SchedulingMode.FAIR.toString))
  }

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      // stripXSS is called first to remove suspicious characters used in XSS attacks
      val stageId = Option(UIUtils.stripXSS(request.getParameter("id"))).map(_.toInt)
      stageId.foreach { id =>
        store.asOption(store.lastStageAttempt(id)).foreach { stage =>
          val status = stage.status
          if (status == StageStatus.ACTIVE || status == StageStatus.PENDING) {
            sc.foreach(_.cancelStage(id, "killed via the Web UI"))
            // Do a quick pause here to give Spark time to kill the stage so it shows up as
            // killed after the refresh. Note that this will block the serving thread so the
            // time should be limited in duration.
            Thread.sleep(100)
          }
        }
      }
    }
  }

}
