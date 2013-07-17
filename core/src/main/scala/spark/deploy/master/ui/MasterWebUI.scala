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

package spark.deploy.master.ui

import akka.actor.ActorRef
import akka.util.Duration

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import spark.{Logging, Utils}
import spark.ui.JettyUtils
import spark.ui.JettyUtils._

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val master: ActorRef, requestedPort: Option[Int] = None) extends Logging {
  implicit val timeout = Duration.create(
    System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
  val host = Utils.localHostName()
  val port = requestedPort.getOrElse(
    System.getProperty("master.ui.port", MasterWebUI.DEFAULT_PORT).toInt)

  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val applicationPage = new ApplicationPage(this)
  val indexPage = new IndexPage(this)

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort.get))
    } catch {
      case e: Exception =>
        logError("Failed to create Master JettyUtils", e)
        System.exit(1)
    }
  }

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)),
    ("/app/json", (request: HttpServletRequest) => applicationPage.renderJson(request)),
    ("/app", (request: HttpServletRequest) => applicationPage.render(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def stop() {
    server.foreach(_.stop())
  }
}

private[spark] object MasterWebUI {
  val STATIC_RESOURCE_DIR = "spark/ui/static"
  val DEFAULT_PORT = "8080"
}
