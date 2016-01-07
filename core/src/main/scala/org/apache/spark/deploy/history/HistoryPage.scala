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

  private val pageSize = 20
  private val plusOrMinus = 2

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedPage = Option(request.getParameter("page")).getOrElse("1").toInt
    val requestedFirst = (requestedPage - 1) * pageSize
    val requestedIncomplete =
      Option(request.getParameter("showIncomplete")).getOrElse("false").toBoolean

    val allApps = parent.getApplicationList()
      .filter(_.attempts.head.completed != requestedIncomplete)
    val allAppsSize = allApps.size

    val actualFirst = if (requestedFirst < allAppsSize) requestedFirst else 0
    val actualPage = (actualFirst / pageSize) + 1
    val last = Math.min(actualFirst + pageSize, allAppsSize) - 1
    val pageCount = allAppsSize / pageSize + (if (allAppsSize % pageSize > 0) 1 else 0)

    val secondPageFromLeft = 2
    val secondPageFromRight = pageCount - 1

    val providerConfig = parent.getProviderConfig()
    val content =
      <div class="row-fluid">
        <script src={UIUtils.prependBaseUri("/static/dataTables.rowsGroup.js")}></script>
          <div class="span12">
            <ul class="unstyled">
              {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
            </ul>
            {
            if (allAppsSize > 0) {
              val leftSideIndices =
                rangeIndices(actualPage - plusOrMinus until actualPage, 1 < _, requestedIncomplete)
              val rightSideIndices =
                rangeIndices(actualPage + 1 to actualPage + plusOrMinus, _ < pageCount,
                  requestedIncomplete)

              <h4>
                Showing {actualFirst + 1}-{last + 1} of {allAppsSize}
                {if (requestedIncomplete) "(Incomplete applications)"}
                <span style="float: right">
                  {
                  if (actualPage > 1) {
                    <a href={makePageLink(actualPage - 1, requestedIncomplete)}>&lt; </a>
                      <a href={makePageLink(1, requestedIncomplete)}>1</a>
                  }
                  }
                  {if (actualPage - plusOrMinus > secondPageFromLeft) " ... "}
                  {leftSideIndices}
                  {actualPage}
                  {rightSideIndices}
                  {if (actualPage + plusOrMinus < secondPageFromRight) " ... "}
                  {
                  if (actualPage < pageCount) {
                    <a href={makePageLink(pageCount, requestedIncomplete)}>{pageCount}</a>
                      <a href={makePageLink(actualPage + 1, requestedIncomplete)}> &gt;</a>
                  }
                  }
                </span>
              </h4> ++
              <div id="history-summary"></div>
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

            <a href={makePageLink(actualPage, !requestedIncomplete)}>
              {
              if (requestedIncomplete) {
                "Back to completed applications"
              } else {
                "Show incomplete applications"
              }
              }
            </a>
          </div> 
        <script src={UIUtils.prependBaseUri("/static/historypage.js")}> </script>

      </div>
    UIUtils.basicSparkPage(content, "History Server")
  }

  private def rangeIndices(
      range: Seq[Int],
      condition: Int => Boolean,
      showIncomplete: Boolean): Seq[Node] = {
    range.filter(condition).map(nextPage =>
      <a href={makePageLink(nextPage, showIncomplete)}> {nextPage} </a>)
  }

  private def makePageLink(linkPage: Int, showIncomplete: Boolean): String = {
    UIUtils.prependBaseUri("/?" + Array(
      "page=" + linkPage,
      "showIncomplete=" + showIncomplete
      ).mkString("&"))
  }
}
