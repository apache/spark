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

import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {

  init()

  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

    attachPage(new ExecutorsPage(this, threadDumpEnabled))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
  }

}

private[ui] class ExecutorsPage(
    parent: SparkUITab,
    threadDumpEnabled: Boolean)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div>
        {
          <div id="active-executors" class="row-fluid"></div> ++
          <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
          <script>setThreadDumpEnabled({threadDumpEnabled})</script>
        }
      </div>

    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
}
