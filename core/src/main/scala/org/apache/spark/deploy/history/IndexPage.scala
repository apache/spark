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

import java.text.SimpleDateFormat
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{SparkUI, UIUtils}

private[spark] class IndexPage(parent: HistoryServer) {
  val dateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  def render(request: HttpServletRequest): Seq[Node] = {
    // Check if logs have been updated
    parent.checkForLogs()

    // Populate app table, with most recently modified first
    val appRows = parent.logPathToLastUpdated.toSeq
      .sortBy { case (path, lastUpdated) => -lastUpdated }
      .map { case (path, lastUpdated) =>
        // (appName, lastUpdated, UI)
        (parent.getAppName(path), lastUpdated, parent.logPathToUI(path))
      }
    val appTable = UIUtils.listingTable(appHeader, appRow, appRows)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Event Log Location: </strong> {parent.baseLogDir}</li>
            <h4>Applications</h4> {appTable}
          </ul>
        </div>
      </div>

    UIUtils.basicSparkPage(content, "History Server")
  }

  private val appHeader = Seq[String]("App Name", "Last Updated")

  private def appRow(info: (String, Long, SparkUI)): Seq[Node] = {
    info match { case (appName, lastUpdated, ui) =>
      <tr>
        <td><a href={parent.getAddress + ui.basePath}>{appName}</a></td>
        <td>{dateFmt.format(new Date(lastUpdated))}</td>
      </tr>
    }
  }
}
