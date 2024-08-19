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

import scala.io.Source

import jakarta.servlet.http.HttpServletResponse.{SC_METHOD_NOT_ALLOWED, SC_OK}
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.master._
import org.apache.spark.deploy.master.ui.MasterWebUISuite._
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.UI.MASTER_UI_VISIBLE_ENV_VAR_PREFIXES
import org.apache.spark.internal.config.UI.UI_KILL_ENABLED
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.util.Utils

class ReadOnlyMasterWebUISuite extends SparkFunSuite {

  val conf = new SparkConf()
    .set(UI_KILL_ENABLED, false)
    .set(DECOMMISSION_ENABLED, false)
    .set(MASTER_UI_VISIBLE_ENV_VAR_PREFIXES.key, "SPARK_SCALA_")
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = mock(classOf[RpcEnv])
  val master = mock(classOf[Master])
  val masterEndpointRef = mock(classOf[RpcEndpointRef])
  when(master.securityMgr).thenReturn(securityMgr)
  when(master.conf).thenReturn(conf)
  when(master.rpcEnv).thenReturn(rpcEnv)
  when(master.self).thenReturn(masterEndpointRef)
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

  test("/app/kill POST method is not allowed") {
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/app/kill/"
    val body = convPostDataToString(Map(("id", "1"), ("terminate", "true")))
    assert(sendHttpRequest(url, "POST", body).getResponseCode === SC_METHOD_NOT_ALLOWED)
  }

  test("/driver/kill POST method is not allowed") {
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/driver/kill/"
    val body = convPostDataToString(Map(("id", "driver-0"), ("terminate", "true")))
    assert(sendHttpRequest(url, "POST", body).getResponseCode === SC_METHOD_NOT_ALLOWED)
  }

  test("/workers/kill POST method is not allowed") {
    val hostnames = Seq(s"${Utils.localHostNameForURI()}")
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/workers/kill/"
    val body = convPostDataToString(hostnames.map(("host", _)))
    assert(sendHttpRequest(url, "POST", body).getResponseCode === SC_METHOD_NOT_ALLOWED)
  }

  test("SPARK-47894: /environment") {
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/environment"
    val conn = sendHttpRequest(url, "GET", "")
    assert(conn.getResponseCode === SC_OK)
    val result = Source.fromInputStream(conn.getInputStream).mkString
    assert(result.contains("Runtime Information"))
    assert(result.contains("Spark Properties"))
    assert(result.contains("Hadoop Properties"))
  }

  test("SPARK-49206: Add 'Environment Variables' table to Master 'EnvironmentPage'") {
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/environment/"
    val conn = sendHttpRequest(url, "GET", "")
    assert(conn.getResponseCode === SC_OK)
    val result = Source.fromInputStream(conn.getInputStream).mkString
    assert(result.contains("Environment Variables"))
    assert(result.contains("<tr><td>SPARK_SCALA_VERSION</td><td>2.1"))
  }
}
