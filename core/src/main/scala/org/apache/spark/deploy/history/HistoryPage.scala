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

package org.apache.spark.deploy.history

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[history] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedIncomplete = Option(request.getParameter("showIncomplete"))
      .getOrElse("false").toBoolean

    val displayApplications = parent.getApplicationList()
      .exists(isApplicationCompleted(_) != requestedIncomplete)
    val eventLogsUnderProcessCount = parent.getEventLogsUnderProcess()
    val lastUpdatedTime = parent.getLastUpdatedTime()
    val providerConfig = parent.getProviderConfig()
    val content =
      <script src={UIUtils.prependBaseUri(request, "/static/historypage-common.js")}></script> ++
      <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script>
      <div>
          <div class="container-fluid">
            <ul class="list-unstyled">
              {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
            </ul>
            {
            if (eventLogsUnderProcessCount > 0) {
              <p>There are {eventLogsUnderProcessCount} event log(s) currently being
                processed which may result in additional applications getting listed on this page.
                Refresh the page to view updates. </p>
            }
            }

            {
            if (lastUpdatedTime > 0) {
              <p>Last updated: <span id="last-updated">{lastUpdatedTime}</span></p>
            }
            }

            {
            <p>Client local time zone: <span id="time-zone"></span></p>
            }

            {
            if (displayApplications) {
              <script src={UIUtils.prependBaseUri(
                request, "/static/dataTables.rowsGroup.js")}></script> ++
                <div id="history-summary"></div> ++
                <script src={UIUtils.prependBaseUri(request, "/static/historypage.js")}></script> ++
                <script>setAppLimit({parent.maxApplications})</script>
            } else if (requestedIncomplete) {
              <h4>No incomplete applications found!</h4>
            } else if (eventLogsUnderProcessCount > 0) {
              <h4>No completed applications found!</h4>
            } else {
              <h4>No completed applications found!</h4> ++ parent.emptyListingHtml
            }
            }

            <a href={makePageLink(request, !requestedIncomplete)}>
              {
              if (requestedIncomplete) {
                "Back to completed applications"
              } else {
                "Show incomplete applications"
              }
              }
            </a>
          </div>
      </div>
    UIUtils.basicSparkPage(request, content, "History Server", true)
  }

  private def makePageLink(request: HttpServletRequest, showIncomplete: Boolean): String = {
    UIUtils.prependBaseUri(request, "/?" + "showIncomplete=" + showIncomplete)
  }

  private def isApplicationCompleted(appInfo: ApplicationInfo): Boolean = {
    appInfo.attempts.nonEmpty && appInfo.attempts.head.completed
  }
}
