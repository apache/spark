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

import java.util.{Date, List => JList}
import javax.ws.rs.{DefaultValue, GET, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.deploy.history.ApplicationHistoryInfo

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ApplicationListResource(uiRoot: UIRoot) {

  @GET
  def appList(
      @QueryParam("status") status: JList[ApplicationStatus],
      @DefaultValue("2010-01-01") @QueryParam("minDate") minDate: SimpleDateParam,
      @DefaultValue("3000-01-01") @QueryParam("maxDate") maxDate: SimpleDateParam,
      @DefaultValue("2010-01-01") @QueryParam("minEndDate") minEndDate: SimpleDateParam,
      @DefaultValue("3000-01-01") @QueryParam("maxEndDate") maxEndDate: SimpleDateParam,
      @QueryParam("limit") limit: Integer)
  : Iterator[ApplicationInfo] = {

    val numApps = Option(limit).map(_.toInt).getOrElse(Integer.MAX_VALUE)
    val includeCompleted = status.isEmpty || status.contains(ApplicationStatus.COMPLETED)
    val includeRunning = status.isEmpty || status.contains(ApplicationStatus.RUNNING)

    uiRoot.getApplicationInfoList.filter { app =>
      val anyRunning = app.attempts.exists(!_.completed)
      // if any attempt is still running, we consider the app to also still be running;
      // keep the app if *any* attempts fall in the right time window
      ((!anyRunning && includeCompleted) || (anyRunning && includeRunning)) &&
      app.attempts.exists { attempt =>
        isAttemptInRange(attempt, minDate, maxDate, minEndDate, maxEndDate, anyRunning)
      }
    }.take(numApps)
  }

  private def isAttemptInRange(
      attempt: ApplicationAttemptInfo,
      minStartDate: SimpleDateParam,
      maxStartDate: SimpleDateParam,
      minEndDate: SimpleDateParam,
      maxEndDate: SimpleDateParam,
      anyRunning: Boolean): Boolean = {
    val startTimeOk = attempt.startTime.getTime >= minStartDate.timestamp &&
      attempt.startTime.getTime <= maxStartDate.timestamp
    // If the maxEndDate is in the past, exclude all running apps.
    val endTimeOkForRunning = anyRunning && (maxEndDate.timestamp > System.currentTimeMillis())
    val endTimeOkForCompleted = !anyRunning && (attempt.endTime.getTime >= minEndDate.timestamp &&
      attempt.endTime.getTime <= maxEndDate.timestamp)
    val endTimeOk = endTimeOkForRunning || endTimeOkForCompleted
    startTimeOk && endTimeOk
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
          duration =
            if (internalAttemptInfo.endTime > 0) {
              internalAttemptInfo.endTime - internalAttemptInfo.startTime
            } else {
              0
            },
          lastUpdated = new Date(internalAttemptInfo.lastUpdated),
          sparkUser = internalAttemptInfo.sparkUser,
          completed = internalAttemptInfo.completed
        )
      }
    )
  }
}
