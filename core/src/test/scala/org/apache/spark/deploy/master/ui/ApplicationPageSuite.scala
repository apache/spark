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

import java.util.Date

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.{mock, when}

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{ApplicationInfo, ApplicationState, Master}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEndpointRef

class ApplicationPageSuite extends SparkFunSuite {

  private val master = mock(classOf[Master])
  when(master.historyServerUrl).thenReturn(Some("http://my-history-server:18080"))

  private val rp = new ResourceProfile(Map.empty, Map.empty)
  private val desc = ApplicationDescription("name", Some(4), null, "appUiUrl", rp)
  private val descWithoutUI = ApplicationDescription("name", Some(4), null, "", rp)
  private val appFinished = new ApplicationInfo(0, "app-finished", desc, new Date, null, 1)
  appFinished.markFinished(ApplicationState.FINISHED)
  private val appLive = new ApplicationInfo(0, "app-live", desc, new Date, null, 1)
  private val appLiveWithoutUI =
    new ApplicationInfo(0, "app-live-without-ui", descWithoutUI, new Date, null, 1)

  private val state = mock(classOf[MasterStateResponse])
  when(state.completedApps).thenReturn(Array(appFinished))
  when(state.activeApps).thenReturn(Array(appLive, appLiveWithoutUI))

  private val rpc = mock(classOf[RpcEndpointRef])
  when(rpc.askSync[MasterStateResponse](RequestMasterState)).thenReturn(state)

  private val masterWebUI = mock(classOf[MasterWebUI])
  when(masterWebUI.master).thenReturn(master)
  when(masterWebUI.masterEndpointRef).thenReturn(rpc)

  test("SPARK-45774: Application Detail UI") {
    val request = mock(classOf[HttpServletRequest])
    when(request.getParameter("appId")).thenReturn("app-live")

    val result = new ApplicationPage(masterWebUI).render(request).toString()
    assert(result.contains("Application Detail UI"))
    assert(!result.contains("Application History UI"))
    assert(!result.contains(master.historyServerUrl.get))
  }

  test("SPARK-50021: Application Detail UI is empty when spark.ui.enabled=false") {
    val request = mock(classOf[HttpServletRequest])
    when(request.getParameter("appId")).thenReturn("app-live-without-ui")

    val result = new ApplicationPage(masterWebUI).render(request).toString()
    assert(result.contains("Application UI:</strong> Disabled"))
    assert(!result.contains("Application History UI"))
    assert(!result.contains(master.historyServerUrl.get))
  }

  test("SPARK-45774: Application History UI") {
    val request = mock(classOf[HttpServletRequest])
    when(request.getParameter("appId")).thenReturn("app-finished")

    val result = new ApplicationPage(masterWebUI).render(request).toString()
    assert(!result.contains("Application Detail UI"))
    assert(result.contains("Application History UI"))
    assert(result.contains(master.historyServerUrl.get))
  }
}
