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

  def render(request: HttpServletRequest): Seq[Node] = {
    val appRows = parent.appIdToInfo.values.toSeq.sortBy { app => -app.lastUpdated }
    val appTable = UIUtils.listingTable(appHeader, appRow, appRows)
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Event Log Location: </strong> {parent.baseLogDir}</li>
          </ul>
          {
            if (parent.appIdToInfo.size > 0) {
              <h4>
                Showing {parent.appIdToInfo.size}/{parent.getNumApplications}
                Completed Application{if (parent.getNumApplications > 1) "s" else ""}
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
    "Log Directory",
    "Last Updated")

  private def appRow(info: ApplicationHistoryInfo): Seq[Node] = {
    val appName = if (info.started) info.name else info.logDirPath.getName
    val uiAddress = parent.getAddress + info.ui.basePath
    val startTime = if (info.started) UIUtils.formatDate(info.startTime) else "Not started"
    val endTime = if (info.completed) UIUtils.formatDate(info.endTime) else "Not completed"
    val difference = if (info.started && info.completed) info.endTime - info.startTime else -1L
    val duration = if (difference > 0) UIUtils.formatDuration(difference) else "---"
    val sparkUser = if (info.started) info.sparkUser else "Unknown user"
    val logDirectory = info.logDirPath.getName
    val lastUpdated = UIUtils.formatDate(info.lastUpdated)
    <tr>
      <td><a href={uiAddress}>{appName}</a></td>
      <td>{startTime}</td>
      <td>{endTime}</td>
      <td>{duration}</td>
      <td>{sparkUser}</td>
      <td>{logDirectory}</td>
      <td>{lastUpdated}</td>
    </tr>
  }
}
