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

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[history] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedIncomplete =
      Option(request.getParameter("showIncomplete")).getOrElse("false").toBoolean

    val allApps = parent.getApplicationList()
      .filter(_.completed != requestedIncomplete)
    val allAppsSize = allApps.size

    val providerConfig = parent.getProviderConfig()
    val content =
      <div>
          <div class="span12">
            <ul class="unstyled">
              {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
            </ul>
            {
            if (allAppsSize > 0) {
              <script src={UIUtils.prependBaseUri("/static/dataTables.rowsGroup.js")}></script> ++
                <div id="history-summary" class="span12 pagination"></div> ++
                <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
                <script src={UIUtils.prependBaseUri("/static/historypage.js")}></script> ++
                <script>setAppLimit({parent.maxApplications})</script>
            } else if (requestedIncomplete) {
              <h4>No incomplete applications found!</h4>
            } else {
              <h4>No completed applications found!</h4> ++
                <p>Did you specify the correct logging directory?
                  Please verify your setting of <span style="font-style:italic">
                  spark.history.fs.logDirectory</span> and whether you have the permissions to
                  access it.<br /> It is also possible that your application did not run to
                  completion or did not stop the SparkContext.
                </p>
            }
            }

            <a href={makePageLink(!requestedIncomplete)}>
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
    UIUtils.basicSparkPage(content, "History Server", true)
  }

  private def makePageLink(showIncomplete: Boolean): String = {
    UIUtils.prependBaseUri("/?" + "showIncomplete=" + showIncomplete)
  }
}
