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

package org.apache.spark.sql.connect.ui

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.Logging
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

/** Page for Spark UI that contains information pertaining to a single Spark Connect session */
private[ui] class SparkConnectServerSessionPage(parent: SparkConnectServerTab)
    extends WebUIPage("session")
    with Logging {

  val store = parent.store
  private val startTime = parent.startTime

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val sessionId = request.getParameter("id")
    require(sessionId != null && sessionId.nonEmpty, "Missing id parameter")

    val content = store.synchronized { // make sure all parts in this page are consistent
      store
        .getSession(sessionId)
        .map { sessionStat =>
          generateBasicStats() ++
            <br/> ++
            <h4>
            User
            {sessionStat.userId}
            ,
            Session created at
            {formatDate(sessionStat.startTimestamp)}
            ,
            Total run
            {sessionStat.totalExecution}
            Request(s)
          </h4> ++
            generateSQLStatsTable(request, sessionStat.sessionId)
        }
        .getOrElse(<div>No information to display for session {sessionId}</div>)
    }
    UIUtils.headerSparkPage(request, "Spark Connect Session", content, parent)
  }

  /** Generate basic stats of the Spark Connect Server */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime.getTime
    <ul class ="list-unstyled">
      <li>
        <strong>Started at: </strong> {formatDate(startTime)}
      </li>
      <li>
        <strong>Time since start: </strong>{formatDurationVerbose(timeSinceStart)}
      </li>
    </ul>
  }

  /** Generate stats of batch statements of the Spark Connect server */
  private def generateSQLStatsTable(request: HttpServletRequest, sessionID: String): Seq[Node] = {
    val executionList = store.getExecutionList
      .filter(_.sessionId == sessionID)
    val numStatement = executionList.size
    val table = if (numStatement > 0) {

      val sqlTableTag = "sqlsessionstat"

      val sqlTablePage =
        Option(request.getParameter(s"$sqlTableTag.page")).map(_.toInt).getOrElse(1)

      try {
        Some(
          new SqlStatsPagedTable(
            request,
            parent,
            executionList,
            "connect/session",
            UIUtils.prependBaseUri(request, parent.basePath),
            sqlTableTag,
            showSessionLink = false).table(sqlTablePage))
      } catch {
        case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
          Some(<div class="alert alert-error">
            <p>Error while rendering job table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>)
      }
    } else {
      None
    }
    val content =
      <span id="sqlsessionstat" class="collapse-aggregated-sqlsessionstat collapse-table"
            onClick="collapseTable('collapse-aggregated-sqlsessionstat',
                'aggregated-sqlsessionstat')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Request Statistics</a>
        </h4>
      </span> ++
        <div class="aggregated-sqlsessionstat collapsible-table">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>

    content
  }
}
