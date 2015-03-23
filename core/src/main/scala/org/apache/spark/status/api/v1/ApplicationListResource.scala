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

import java.util.Date
import javax.ws.rs.{DefaultValue, GET, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.deploy.history.ApplicationHistoryInfo
import org.apache.spark.deploy.master.{ApplicationInfo => InternalApplicationInfo}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ApplicationListResource(uiRoot: UIRoot) {

  @GET
  def appList(
    @QueryParam("status") status: java.util.List[ApplicationStatus],
    @DefaultValue("2010-01-01") @QueryParam("minDate") minDate: SimpleDateParam,
    @DefaultValue("3000-01-01") @QueryParam("maxDate") maxDate: SimpleDateParam
  ): Seq[ApplicationInfo] = {
    val allApps = uiRoot.getApplicationInfoList
    val adjStatus = {
      if (status.isEmpty) {
        java.util.Arrays.asList(ApplicationStatus.values: _*)
      } else {
        status
      }
    }
    val includeCompleted = adjStatus.contains(ApplicationStatus.COMPLETED)
    val includeRunning = adjStatus.contains(ApplicationStatus.RUNNING)
    allApps.filter { app =>
      val statusOk = (app.completed && includeCompleted) ||
        (!app.completed && includeRunning)
      val dateOk = app.startTime.getTime >= minDate.timestamp &&
        app.startTime.getTime <= maxDate.timestamp
      statusOk && dateOk
    }
  }
}

private[spark] object ApplicationsListResource {
  def appHistoryInfoToPublicAppInfo(app: ApplicationHistoryInfo): ApplicationInfo = {
    new ApplicationInfo(
      id = app.id,
      name = app.name,
      startTime = new Date(app.startTime),
      endTime = new Date(app.endTime),
      sparkUser = app.sparkUser,
      completed = app.completed
    )
  }

  def convertApplicationInfo(
      internal: InternalApplicationInfo,
      completed: Boolean): ApplicationInfo = {
    new ApplicationInfo(
      id = internal.id,
      name = internal.desc.name,
      startTime = new Date(internal.startTime),
      endTime = new Date(internal.endTime),
      sparkUser = internal.desc.user,
      completed = completed
    )
  }

}
