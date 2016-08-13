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

import scala.collection.mutable.HashMap

import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._

/**
 * Web UI server for the standalone master.
 */
private[master]
class MasterWebUI(
    val master: Master,
    requestedPort: Int)
  extends WebUI(master.securityMgr, master.securityMgr.getSSLOptions("standalone"),
    requestedPort, master.conf, name = "MasterUI") with Logging {

  val masterEndpointRef = master.self
  val killEnabled = master.conf.getBoolean("spark.ui.killEnabled", true)
  private val proxyHandlers = new HashMap[String, ServletContextHandler]

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    val masterPage = new MasterPage(this)
    attachPage(new ApplicationPage(this))
    attachPage(masterPage)
    attachHandler(createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler(
      "/app/kill", "/", masterPage.handleAppKillRequest, httpMethods = Set("POST")))
    attachHandler(createRedirectHandler(
      "/driver/kill", "/", masterPage.handleDriverKillRequest, httpMethods = Set("POST")))
  }

  def addProxyTargets(id: String, target: String): Unit = {
    var endTarget: String = target
    if (endTarget.endsWith("/")) {
      endTarget = endTarget.substring(0, endTarget.length()-1)
    }
    val handler = createProxyHandler("/proxy/" + id, endTarget)
    attachHandler(handler)
    proxyHandlers(id) = handler
  }

  def removeProxyTargets(id: String): Unit = {
    detachHandler(proxyHandlers.get(id).get)
    proxyHandlers -= id
  }
}

private[master] object MasterWebUI {
  private val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
