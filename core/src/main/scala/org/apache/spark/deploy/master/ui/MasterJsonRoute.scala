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
package org.apache.spark.deploy.master.ui

import javax.servlet.http.HttpServletRequest


import akka.pattern.ask

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{ApplicationInfo => MasterAppInfo}

import org.apache.spark.status.StatusJsonRoute
import org.apache.spark.status.api.ApplicationInfo

import scala.concurrent.Await

class MasterJsonRoute(parent: MasterWebUI) extends StatusJsonRoute[Seq[ApplicationInfo]]{

  private val master = parent.masterActorRef
  private val timeout = parent.timeout

  override def renderJson(request: HttpServletRequest): Seq[ApplicationInfo] = {

    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)

    val apps = state.activeApps ++ state.completedApps
    apps.toSeq.map{MasterJsonRoute.masterAppInfoToPublicAppInfo}
  }

}

object MasterJsonRoute {
  def masterAppInfoToPublicAppInfo(app: MasterAppInfo) : ApplicationInfo = {
    ApplicationInfo(
      id = app.id,
      name = app.desc.name,
      startTime = app.startTime,
      endTime = app.endTime,
      sparkUser = app.desc.user,
      completed = app.endTime != -1
    )
  }
}
