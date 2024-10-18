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
import javax.servlet.http.HttpServletResponse.SC_OK

import scala.io.Source

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master._
import org.apache.spark.deploy.master.ui.MasterWebUISuite._
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.UI.UI_KILL_ENABLED
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.util.Utils

class ReadOnlyMasterWebUISuite extends SparkFunSuite {

  import org.apache.spark.deploy.DeployTestUtils._

  val conf = new SparkConf()
    .set(UI_KILL_ENABLED, false)
    .set(DECOMMISSION_ENABLED, false)
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = mock(classOf[RpcEnv])
  val master = mock(classOf[Master])
  val masterEndpointRef = mock(classOf[RpcEndpointRef])
  when(master.securityMgr).thenReturn(securityMgr)
  when(master.conf).thenReturn(conf)
  when(master.rpcEnv).thenReturn(rpcEnv)
  when(master.self).thenReturn(masterEndpointRef)
  val desc1 = createAppDesc().copy(name = "WithUI")
  val desc2 = desc1.copy(name = "WithoutUI", appUiUrl = "")
  val app1 = new ApplicationInfo(new Date().getTime, "app1", desc1, new Date(), null, Int.MaxValue)
  val app2 = new ApplicationInfo(new Date().getTime, "app2", desc2, new Date(), null, Int.MaxValue)
  val state = new MasterStateResponse(
    "host", 8080, None, Array.empty, Array(app1, app2), Array.empty,
    Array.empty, Array.empty, RecoveryState.ALIVE)
  when(masterEndpointRef.askSync[MasterStateResponse](RequestMasterState)).thenReturn(state)
  val masterWebUI = new MasterWebUI(master, 0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    masterWebUI.bind()
  }

  override def afterAll(): Unit = {
    try {
      masterWebUI.stop()
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-50022: Fix 'MasterPage' to hide App UI links when UI is disabled") {
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/"
    val conn = sendHttpRequest(url, "GET")
    assert(conn.getResponseCode === SC_OK)
    val result = Source.fromInputStream(conn.getInputStream).mkString
    assert(result.contains("<a href=\"appUiUrl\">WithUI</a>"))
    assert(result.contains("  WithoutUI\n"))
  }
}
