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
import org.apache.spark.status.api.v1.{ApplicationsListResource, ApplicationInfo, JsonRootResource, UIRoot}
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.AkkaUtils

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val master: Master, requestedPort: Int)
  extends WebUI(master.securityMgr, requestedPort, master.conf, name = "MasterUI") with Logging
  with UIRoot {

  val masterActorRef = master.self
  val timeout = AkkaUtils.askTimeout(master.conf)
  val killEnabled = master.conf.getBoolean("spark.ui.killEnabled", true)

  val masterPage = new MasterPage(this)

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    val masterPage = new MasterPage(this)
    attachPage(new ApplicationPage(this))
    attachPage(new HistoryNotFoundPage(this))
    attachPage(masterPage)
    attachHandler(createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(JsonRootResource.getJsonServlet(this))
    attachHandler(
      createRedirectHandler("/app/kill", "/", masterPage.handleAppKillRequest))
    attachHandler(
      createRedirectHandler("/driver/kill", "/", masterPage.handleDriverKillRequest))
  }

  /** Attach a reconstructed UI to this Master UI. Only valid after bind(). */
  def attachSparkUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "Master UI must be bound to a server before attaching SparkUIs")
    ui.getHandlers.foreach(attachHandler)
  }

  /** Detach a reconstructed UI from this Master UI. Only valid after bind(). */
  def detachSparkUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "Master UI must be bound to a server before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
  }

  def getApplicationInfoList: Seq[ApplicationInfo] = {
    val state = masterPage.getMasterState
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    activeApps.map { ApplicationsListResource.convertApplicationInfo(_, false) } ++
      completedApps.map { ApplicationsListResource.convertApplicationInfo(_, true) }
  }

  def getSparkUI(appId: String): Option[SparkUI] = {
    val state = masterPage.getMasterState
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    (activeApps ++ completedApps).find { _.id == appId }.flatMap {
      master.rebuildSparkUI
    }
  }
}

private[spark] object MasterWebUI {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
