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

import java.net.URL
import java.util.regex.Pattern
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.Logging
import org.apache.spark.deploy.master.Master
import org.apache.spark.status.api.v1.{ApplicationsListResource, ApplicationInfo}
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._

/**
 * Web UI server for the standalone master.
 */
private[master]
class MasterWebUI(
    val master: Master,
    requestedPort: Int,
    customMasterPage: Option[MasterPage] = None)
  extends WebUI(master.securityMgr, master.securityMgr.getSSLOptions("standalone"),
    requestedPort, master.conf, name = "MasterUI") with Logging {

  val masterEndpointRef = master.self
  val killEnabled = master.conf.getBoolean("spark.ui.killEnabled", true)

  val masterPage = customMasterPage.getOrElse(new MasterPage(this))

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
    attachHandler(createApiRootHandler())
  }

  def createApiRootHandler(): ServletContextHandler = {

    val servlet = new HttpServlet {
      private lazy val appIdPattern = Pattern.compile("\\/api\\/v\\d+\\/applications\\/([^\\/]+).*")

      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        doRequest(request, response)
      }
      override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        doRequest(request, response)
      }
      private def doRequest(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        val requestURI = request.getRequestURI

        // requesting an application info list
        if (requestURI == "applications") {
          // TODO - Should send ApplicationList response ???
        } else {
          // forward request to app if it is active, otherwise send error
          getAppId(request) match {
            case Some(appId) =>
              val state = masterPage.getMasterState
              state.activeApps.find(appInfo => appInfo.id == appId) match {
                case Some(appInfo) =>
                  val prefixedDestPath = appInfo.desc.appUiUrl + requestURI
                  val newUrl = new URL(new URL(request.getRequestURL.toString), prefixedDestPath).toString
                  response.sendRedirect(newUrl)
                  request.getPathInfo
                case None =>
                  response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE)
              }
            case None =>
              response.sendError(HttpServletResponse.SC_BAD_REQUEST)
          }
        }
      }

      private def getAppId(request: HttpServletRequest): Option[String] = {
        val m = appIdPattern.matcher(request.getRequestURI)
        if (m.matches) Some(m.group(1)) else None
      }

      // SPARK-5983 ensure TRACE is not supported
      protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }

    createServletHandler("/api", servlet, "")
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    val state = masterPage.getMasterState
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    activeApps.iterator.map { ApplicationsListResource.convertApplicationInfo(_, false) } ++
      completedApps.iterator.map { ApplicationsListResource.convertApplicationInfo(_, true) }
  }
}

private[master] object MasterWebUI {
  private val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
