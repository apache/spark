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

import jakarta.servlet.{Filter, FilterChain, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletRequestWrapper}
import org.mockito.Mockito.{mock, never, verify, when}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.DeployMessages.DecommissionWorkersOnHosts
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.UI._
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.util.Utils

/**
 * Tests the modify ACL enforcement of the Master Web UI's `/workers/kill` endpoint.
 * A [[FakeAuthFilter]] injects the remote user so the modify permission can be exercised.
 */
class MasterWebUIAclSuite extends SparkFunSuite {
  import MasterWebUISuite._

  val conf = new SparkConf()
    .set(DECOMMISSION_ENABLED, true)
    .set(UI_FILTERS, Seq(classOf[FakeAuthFilter].getName))
    .set(ACLS_ENABLE, true)
    .set(UI_VIEW_ACLS, Seq("*"))
    .set(MODIFY_ACLS, Seq("alice"))
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

  private def killWorkers(hostnames: Seq[String], user: String): Unit = {
    val url = s"http://${Utils.localHostNameForURI()}:${masterWebUI.boundPort}/workers/kill/"
    val body = convPostDataToString(hostnames.map(("host", _)))
    val headers = Seq(FakeAuthFilter.FAKE_HTTP_USER -> user)
    val conn = sendHttpRequest(url, "POST", body, headers)
    // The master is mocked here, so cannot assert on the response code.
    conn.getResponseCode
  }

  test("Allow the worker kill request with the modify permission") {
    val hostnames = Seq("allowed")
    killWorkers(hostnames, "alice")
    verify(masterEndpointRef).askSync[Integer](DecommissionWorkersOnHosts(hostnames))
  }

  test("Reject the worker kill request without the modify permission") {
    val hostnames = Seq("denied")
    killWorkers(hostnames, "nobody")
    verify(masterEndpointRef, never()).askSync[Integer](DecommissionWorkersOnHosts(hostnames))
  }
}

/** Test filter that sets the remote user from the request's HTTP_USER header. */
class FakeAuthFilter extends Filter {
  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]
    val wrapped = new HttpServletRequestWrapper(hreq) {
      override def getRemoteUser(): String = hreq.getHeader(FakeAuthFilter.FAKE_HTTP_USER)
    }
    chain.doFilter(wrapped, res)
  }
}

object FakeAuthFilter {
  val FAKE_HTTP_USER = "HTTP_USER"
}
