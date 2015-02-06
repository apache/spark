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
package org.apache.spark.deploy.history

import javax.servlet.http.HttpServletRequest

import org.apache.spark.status.{UIRoot, StatusJsonRoute}
import org.apache.spark.status.api.ApplicationInfo
import org.apache.spark.deploy.master.{ApplicationInfo => InternalApplicationInfo}

class AllApplicationsJsonRoute(val uiRoot: UIRoot) extends StatusJsonRoute[Seq[ApplicationInfo]] {

  override def renderJson(request: HttpServletRequest): Seq[ApplicationInfo] = {
    //TODO filter on some query params, eg. completed, minStartTime, etc
    uiRoot.getApplicationInfoList
  }

}

object AllApplicationsJsonRoute {
  def appHistoryInfoToPublicAppInfo(app: ApplicationHistoryInfo): ApplicationInfo = {
    ApplicationInfo(
      id = app.id,
      name = app.name,
      startTime = app.startTime,
      endTime = app.endTime,
      sparkUser = app.sparkUser,
      completed = app.completed
    )
  }

  def convertApplicationInfo(internal: InternalApplicationInfo, completed: Boolean): ApplicationInfo = {
    ApplicationInfo(
      id = internal.id,
      name = internal.desc.name,
      startTime = internal.startTime,
      endTime = internal.endTime,
      sparkUser = internal.desc.user,
      completed = completed
    )
  }
}