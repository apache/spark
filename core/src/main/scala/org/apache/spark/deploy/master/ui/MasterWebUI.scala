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

import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkUIContainer
import org.apache.spark.deploy.master.Master
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val master: Master, requestedPort: Int)
  extends SparkUIContainer("MasterWebUI") with Logging {

  val masterActorRef = master.self
  val timeout = AkkaUtils.askTimeout(master.conf)

  private val host = Utils.localHostName()
  private val port = requestedPort
  private val applicationPage = new ApplicationPage(this)
  private val indexPage = new IndexPage(this)

  private val handlers: Seq[ServletContextHandler] = {
    master.masterMetricsSystem.getServletHandlers ++
    master.applicationMetricsSystem.getServletHandlers ++
    Seq[ServletContextHandler](
      createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR, "/static"),
      createServletHandler("/app/json",
        (request: HttpServletRequest) => applicationPage.renderJson(request), master.securityMgr),
      createServletHandler("/app",
        (request: HttpServletRequest) => applicationPage.render(request), master.securityMgr),
      createServletHandler("/json",
        (request: HttpServletRequest) => indexPage.renderJson(request), master.securityMgr),
      createServletHandler("/",
        (request: HttpServletRequest) => indexPage.render(request), master.securityMgr)
    )
  }

  def conf = master.conf

  /** Bind to the HTTP server behind this web interface. */
  override def bind() {
    try {
      serverInfo = Some(startJettyServer("0.0.0.0", port, handlers, master.conf))
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Master web UI", e)
        System.exit(1)
    }
  }

}

private[spark] object MasterWebUI {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
