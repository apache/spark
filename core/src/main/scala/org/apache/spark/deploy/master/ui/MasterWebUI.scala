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
import org.eclipse.jetty.server.{Handler, Server}

import org.apache.spark.Logging
import org.apache.spark.deploy.master.Master
import org.apache.spark.ui.JettyUtils
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val master: Master, requestedPort: Int) extends Logging {
  val timeout = AkkaUtils.askTimeout(master.conf)
  val host = Utils.localHostName()
  val port = requestedPort

  val masterActorRef = master.self

  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val applicationPage = new ApplicationPage(this)
  val indexPage = new IndexPage(this)

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer(host, port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort.get))
    } catch {
      case e: Exception =>
        logError("Failed to create Master JettyUtils", e)
        System.exit(1)
    }
  }

  val metricsHandlers = master.masterMetricsSystem.getServletHandlers ++
    master.applicationMetricsSystem.getServletHandlers

  val handlers = metricsHandlers ++ Array[(String, Handler)](
    ("/static", createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)),
    ("/app/json", (request: HttpServletRequest) => applicationPage.renderJson(request)),
    ("/app", (request: HttpServletRequest) => applicationPage.render(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def stop() {
    server.foreach(_.stop())
  }
}

private[spark] object MasterWebUI {
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
}
