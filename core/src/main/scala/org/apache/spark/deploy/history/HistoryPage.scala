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

import org.apache.spark.ui.{WebUIPage, UIUtils}

private[spark] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  private val pageSize = 20

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedPage = Option(request.getParameter("page")).getOrElse("1").toInt
    val requestedFirst = (requestedPage - 1) * pageSize

    val requestedIncludeIncomplete = Option(request.getParameter("includeIncomplete")).getOrElse("false").toBoolean

    val allApps = parent.getApplicationList().filter( app =>
      requestedIncludeIncomplete || !app.incomplete
    )
    val actualFirst = if (requestedFirst < allApps.size) requestedFirst else 0
    val apps = allApps.slice(actualFirst, Math.min(actualFirst + pageSize, allApps.size))

    val actualPage = (actualFirst / pageSize) + 1
    val last = Math.min(actualFirst + pageSize, allApps.size) - 1
    val pageCount = allApps.size / pageSize + (if (allApps.size % pageSize > 0) 1 else 0)

    val appTable = UIUtils.listingTable(appHeader, appRow, apps)
    val providerConfig = parent.getProviderConfig()
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            { providerConfig.map(e => <li><strong>{e._1}:</strong> {e._2}</li>) }
          </ul>
          <a href={createPageLink(actualPage, !requestedIncludeIncomplete)}>
            {if (requestedIncludeIncomplete) {"Hide incomplete apps" } else { "Include incomplete apps" }}
          </a>
          {
            if (allApps.size > 0) {
              <h4>
                Showing {actualFirst + 1}-{last + 1} of {allApps.size}
                <span style="float: right">
                  {if (actualPage > 1)
                    <a href={createPageLink(actualPage - 1, requestedIncludeIncomplete)}>&lt;</a>
                  }
                  {if (actualPage < pageCount)
                    <a href={createPageLink(actualPage + 1, requestedIncludeIncomplete)}>&gt;</a>
                  }
                </span>
              </h4> ++
              appTable
            } else {
              <h4>No Completed Applications Found</h4>
            }
          }
        </div>
      </div>
    UIUtils.basicSparkPage(content, "History Server")
  }

  private val appHeader = Seq(
    "App Name",
    "Started",
    "Completed",
    "Duration",
    "Spark User",
    "Last Updated")

  private def appRow(info: ApplicationHistoryInfo): Seq[Node] = {
    val uiAddress = "/history/" + info.id
    val startTime = UIUtils.formatDate(info.startTime)
    val endTime = if(!info.incomplete) UIUtils.formatDate(info.endTime) else "-"
    val duration = if(!info.incomplete) UIUtils.formatDuration(info.endTime - info.startTime) else "-"
    val lastUpdated = UIUtils.formatDate(info.lastUpdated)
    <tr>
      <td><a href={uiAddress}>{info.name}</a></td>
      <td>{startTime}</td>
      <td>{endTime}</td>
      <td>{duration}</td>
      <td>{info.sparkUser}</td>
      <td>{lastUpdated}</td>
    </tr>
  }

  private def createPageLink(linkPage: Int, includeIncomplete: Boolean): String = {
    "/?" + Array(
      "page=" + linkPage,
      "includeIncomplete=" + includeIncomplete
    ).mkString("&")
  }
}
