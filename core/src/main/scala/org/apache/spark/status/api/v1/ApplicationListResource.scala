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

import java.util.{Arrays, Date, List => JList}
import javax.ws.rs.{DefaultValue, GET, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.deploy.history.ApplicationHistoryInfo
import org.apache.spark.deploy.master.{ApplicationInfo => InternalApplicationInfo}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ApplicationListResource(uiRoot: UIRoot) {

  @GET
  def appList(
      @QueryParam("status") status: JList[ApplicationStatus],
      @DefaultValue("2010-01-01") @QueryParam("minDate") minDate: SimpleDateParam,
      @DefaultValue("3000-01-01") @QueryParam("maxDate") maxDate: SimpleDateParam)
  : Iterator[ApplicationInfo] = {
    val allApps = uiRoot.getApplicationInfoList
    val adjStatus = {
      if (status.isEmpty) {
        Arrays.asList(ApplicationStatus.values(): _*)
      } else {
        status
      }
    }
    val includeCompleted = adjStatus.contains(ApplicationStatus.COMPLETED)
    val includeRunning = adjStatus.contains(ApplicationStatus.RUNNING)
    allApps.filter { app =>
      val anyRunning = app.attempts.exists(!_.completed)
      // if any attempt is still running, we consider the app to also still be running
      val statusOk = (!anyRunning && includeCompleted) ||
        (anyRunning && includeRunning)
      // keep the app if *any* attempts fall in the right time window
      val dateOk = app.attempts.exists { attempt =>
        attempt.startTime.getTime >= minDate.timestamp &&
          attempt.startTime.getTime <= maxDate.timestamp
      }
      statusOk && dateOk
    }
  }
}

private[spark] object ApplicationsListResource {
  def appHistoryInfoToPublicAppInfo(app: ApplicationHistoryInfo): ApplicationInfo = {
    new ApplicationInfo(
      id = app.id,
      name = app.name,
      coresGranted = None,
      maxCores = None,
      coresPerExecutor = None,
      memoryPerExecutorMB = None,
      attempts = app.attempts.map { internalAttemptInfo =>
        new ApplicationAttemptInfo(
          attemptId = internalAttemptInfo.attemptId,
          startTime = new Date(internalAttemptInfo.startTime),
          endTime = new Date(internalAttemptInfo.endTime),
          sparkUser = internalAttemptInfo.sparkUser,
          completed = internalAttemptInfo.completed
        )
      }
    )
  }

  def convertApplicationInfo(
      internal: InternalApplicationInfo,
      completed: Boolean): ApplicationInfo = {
    // standalone application info always has just one attempt
    new ApplicationInfo(
      id = internal.id,
      name = internal.desc.name,
      coresGranted = Some(internal.coresGranted),
      maxCores = internal.desc.maxCores,
      coresPerExecutor = internal.desc.coresPerExecutor,
      memoryPerExecutorMB = Some(internal.desc.memoryPerExecutorMB),
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(internal.startTime),
        endTime = new Date(internal.endTime),
        sparkUser = internal.desc.user,
        completed = completed
      ))
    )
  }

}
