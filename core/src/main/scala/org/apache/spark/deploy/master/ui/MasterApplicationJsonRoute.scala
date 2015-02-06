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

import org.apache.spark.deploy.DeployMessages.{RequestMasterState, MasterStateResponse}
import org.apache.spark.status.StatusJsonRoute
import org.apache.spark.status.api.ApplicationInfo

import scala.concurrent.Await

class MasterApplicationJsonRoute(val parent: MasterWebUI) extends StatusJsonRoute[ApplicationInfo] {
  private val master = parent.masterActorRef
  private val timeout = parent.timeout


  override def renderJson(request: HttpServletRequest): ApplicationInfo = {
    //TODO not really the app id
    val appId = request.getPathInfo()
    println("pathInfo = " + request.getPathInfo())
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)
    state.activeApps.find(_.id == appId).orElse({
      state.completedApps.find(_.id == appId)
    }).map{MasterJsonRoute.masterAppInfoToPublicAppInfo}.getOrElse(null)
  }
}
