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

import org.apache.spark.Logging
import org.apache.spark.deploy.master.Master
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val master: Master, requestedPort: Int)
  extends WebUI(master.securityMgr) with Logging {

  private val host = Utils.localHostName()
  private val port = requestedPort
  val masterActorRef = master.self
  val timeout = AkkaUtils.askTimeout(master.conf)

  /** Initialize all components of the server. */
  def start() {
    attachPage(new ApplicationPage(this))
    attachPage(new IndexPage(this))
    attachHandler(createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR, "/static"))
    master.masterMetricsSystem.getServletHandlers.foreach(attachHandler)
    master.applicationMetricsSystem.getServletHandlers.foreach(attachHandler)
  }

  /** Bind to the HTTP server behind this web interface. */
  def bind() {
    try {
      serverInfo = Some(startJettyServer("0.0.0.0", port, handlers, master.conf))
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Master web UI", e)
        System.exit(1)
    }
  }

  /** Attach a reconstructed UI to this Master UI. Only valid after bind(). */
  def attachUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "Master UI must be bound to a server before attaching SparkUIs")
    ui.getHandlers.foreach(attachHandler)
  }

  /** Detach a reconstructed UI from this Master UI. Only valid after bind(). */
  def detachUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "Master UI must be bound to a server before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
  }
}

private[spark] object MasterWebUI {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
