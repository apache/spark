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

import java.net.{InetAddress, NetworkInterface, SocketException}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.apache.spark.deploy.DeployMessages.{DecommissionWorkersOnHosts, MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.MASTER_UI_DECOMMISSION_ALLOW_MODE
import org.apache.spark.internal.config.UI.UI_KILL_ENABLED
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
  val killEnabled = master.conf.get(UI_KILL_ENABLED)
  val decommissionAllowMode = master.conf.get(MASTER_UI_DECOMMISSION_ALLOW_MODE)

  initialize()

  /** Initialize all components of the server. */
  def initialize(): Unit = {
    val masterPage = new MasterPage(this)
    attachPage(new ApplicationPage(this))
    attachPage(masterPage)
    addStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)
    attachHandler(createRedirectHandler(
      "/app/kill", "/", masterPage.handleAppKillRequest, httpMethods = Set("POST")))
    attachHandler(createRedirectHandler(
      "/driver/kill", "/", masterPage.handleDriverKillRequest, httpMethods = Set("POST")))
    attachHandler(createServletHandler("/workers/kill", new HttpServlet {
      override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        val hostnames: Seq[String] = Option(req.getParameterValues("host"))
          .getOrElse(Array[String]()).toSeq
        if (!isDecommissioningRequestAllowed(req)) {
          resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        } else {
          val removedWorkers = masterEndpointRef.askSync[Integer](
            DecommissionWorkersOnHosts(hostnames))
          logInfo(s"Decommissioning of hosts $hostnames decommissioned $removedWorkers workers")
          if (removedWorkers > 0) {
            resp.setStatus(HttpServletResponse.SC_OK)
          } else if (removedWorkers == 0) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND)
          } else {
            // We shouldn't even see this case.
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
          }
        }
      }
    }, ""))
  }

  def addProxy(): Unit = {
    val handler = createProxyHandler(idToUiAddress)
    attachHandler(handler)
  }

  def idToUiAddress(id: String): Option[String] = {
    val state = masterEndpointRef.askSync[MasterStateResponse](RequestMasterState)
    val maybeWorkerUiAddress = state.workers.find(_.id == id).map(_.webUiAddress)
    val maybeAppUiAddress = state.activeApps.find(_.id == id).map(_.desc.appUiUrl)

    maybeWorkerUiAddress.orElse(maybeAppUiAddress)
  }

  private def isLocal(address: InetAddress): Boolean = {
    if (address.isAnyLocalAddress || address.isLoopbackAddress) {
      return true
    }
    try {
      NetworkInterface.getByInetAddress(address) != null
    } catch {
      case _: SocketException => false
    }
  }

  private def isDecommissioningRequestAllowed(req: HttpServletRequest): Boolean = {
    decommissionAllowMode match {
      case "ALLOW" => true
      case "LOCAL" => isLocal(InetAddress.getByName(req.getRemoteAddr))
      case _ => false
    }
  }

}

private[master] object MasterWebUI {
  private val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
