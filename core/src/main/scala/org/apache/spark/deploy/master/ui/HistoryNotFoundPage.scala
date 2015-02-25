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

import java.net.URLDecoder
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[spark] class HistoryNotFoundPage(parent: MasterWebUI)
  extends WebUIPage("history/not-found") {

  /**
   * Render a page that conveys failure in loading application history.
   *
   * This accepts 3 HTTP parameters:
   *   msg = message to display to the user
   *   title = title of the page
   *   exception = detailed description of the exception in loading application history (if any)
   *
   * Parameters "msg" and "exception" are assumed to be UTF-8 encoded.
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val titleParam = request.getParameter("title")
    val msgParam = request.getParameter("msg")
    val exceptionParam = request.getParameter("exception")

    // If no parameters are specified, assume the user did not enable event logging
    val defaultTitle = "Event logging is not enabled"
    val defaultContent =
      <div class="row-fluid">
        <div class="span12" style="font-size:14px">
          No event logs were found for this application! To
          <a href="http://spark.apache.org/docs/latest/monitoring.html">enable event logging</a>,
          set <span style="font-style:italic">spark.eventLog.enabled</span> to true and
          <span style="font-style:italic">spark.eventLog.dir</span> to the directory to which your
          event logs are written.
        </div>
      </div>

    val title = Option(titleParam).getOrElse(defaultTitle)
    val content = Option(msgParam)
      .map { msg => URLDecoder.decode(msg, "UTF-8") }
      .map { msg =>
        <div class="row-fluid">
          <div class="span12" style="font-size:14px">{msg}</div>
        </div> ++
        Option(exceptionParam)
          .map { e => URLDecoder.decode(e, "UTF-8") }
          .map { e => <pre>{e}</pre> }
          .getOrElse(Seq.empty)
      }.getOrElse(defaultContent)

    UIUtils.basicSparkPage(content, title)
  }
}
