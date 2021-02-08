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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI._
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors")
  with Logging {

  init()

  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)

    attachPage(new ExecutorsPage(this, threadDumpEnabled))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
  }

  def handleKillExecutorRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      // stripXSS is called first to remove suspicious characters used in XSS attacks
      Option(request.getParameter("executorId")).foreach { exeId =>
        // note: To reduce interaction with driver we may NOT check the executor is active,
        // the killAndReplaceExecutor will do nothing when it's dead.
        sc.map(_.killAndReplaceExecutor(exeId)).foreach { r =>
          if (r) {
            logInfo(s"Killed the executorId $exeId via the Web UI.")
          } else {
            logWarning(s"Could NOT kill the executorId $exeId via the Web UI.")
          }
        }
        // Do a quick pause here to give Spark time to kill the executorId so it shows up as
        // killed after the refresh. Note that this will block the serving thread so the
        // time should be limited in duration.
        Thread.sleep(100)
      }
    }
  }
}

private[ui] class ExecutorsPage(
    parent: SparkUITab,
    threadDumpEnabled: Boolean)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      {
        <div id="active-executors"></div> ++
        <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
        <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
        <script>setThreadDumpEnabled({threadDumpEnabled})</script>
        <script>setKillExecutorEnabled({parent.killEnabled})</script>
      }

    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
}
