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

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.config.UI._
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {

  init()

  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)
    val heapHistogramEnabled =
      parent.sc.isDefined && parent.conf.get(UI_HEAP_HISTOGRAM_ENABLED)

    attachPage(new ExecutorsPage(this, threadDumpEnabled, heapHistogramEnabled))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
    if (heapHistogramEnabled) {
      attachPage(new ExecutorHeapHistogramPage(this, parent.sc))
    }
  }

}

private[ui] class ExecutorsPage(
    parent: SparkUITab,
    threadDumpEnabled: Boolean,
    heapHistogramEnabled: Boolean)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val imported = UIUtils.formatImportJavaScript(
      request,
      "/static/executorspage.js",
      "setThreadDumpEnabled",
      "setHeapHistogramEnabled")
    val js =
      s"""
         |$imported
         |
         |setThreadDumpEnabled($threadDumpEnabled);
         |setHeapHistogramEnabled($heapHistogramEnabled)
         |""".stripMargin
    val content =
      {
        <div id="active-executors"></div> ++
        <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
        <script type="module"
                src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
        <script type="module">{Unparsed(js)}</script>
      }

    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
}
