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

package org.apache.spark.deploy.worker.ui

import java.io.File
import javax.servlet.http.HttpServletRequest

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.deploy.worker.ui.WorkerWebUI._
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.AkkaUtils

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(
    val worker: Worker,
    val workDir: File,
    port: Option[Int] = None)
  extends WebUI(worker.securityMgr, getUIPort(port, worker.conf), worker.conf, name = "WorkerUI")
  with Logging {

  val timeout = AkkaUtils.askTimeout(worker.conf)

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    val logPage = new LogPage(this)
    attachPage(logPage)
    attachPage(new WorkerPage(this))
    attachHandler(createStaticHandler(WorkerWebUI.STATIC_RESOURCE_BASE, "/static"))
    attachHandler(createServletHandler("/log",
      (request: HttpServletRequest) => logPage.renderLog(request), worker.securityMgr))
    worker.metricsSystem.getServletHandlers.foreach(attachHandler)
  }
}

private[spark] object WorkerWebUI {
  val DEFAULT_PORT = 8081
  val STATIC_RESOURCE_BASE = SparkUI.STATIC_RESOURCE_DIR

  def getUIPort(requestedPort: Option[Int], conf: SparkConf): Int = {
    requestedPort.getOrElse(conf.getInt("spark.worker.ui.port", WorkerWebUI.DEFAULT_PORT))
  }
}
