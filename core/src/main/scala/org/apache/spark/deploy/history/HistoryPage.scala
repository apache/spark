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
    val appsToShow = allApps.slice(actualFirst, actualFirst + pageSize)

    val actualPage = (actualFirst / pageSize) + 1
    val last = Math.min(actualFirst + pageSize, allAppsSize) - 1
    val pageCount = allAppsSize / pageSize + (if (allAppsSize % pageSize > 0) 1 else 0)

    val secondPageFromLeft = 2
    val secondPageFromRight = pageCount - 1

    val hasMultipleAttempts = appsToShow.exists(_.attempts.size > 1)
    val appTable =
      if (hasMultipleAttempts) {
        // Sorting is disable here as table sort on rowspan has issues.
        // ref. SPARK-10172
        UIUtils.listingTable(appWithAttemptHeader, appWithAttemptRow,
          appsToShow, sortable = false)
      } else {
        UIUtils.listingTable(appHeader, appRow, appsToShow)
      }

    val providerConfig = parent.getProviderConfig()
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
          </ul>
          {
            // This displays the indices of pages that are within `plusOrMinus` pages of
            // the current page. Regardless of where the current page is, this also links
            // to the first and last page. If the current page +/- `plusOrMinus` is greater
            // than the 2nd page from the first page or less than the 2nd page from the last
            // page, `...` will be displayed.
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
              appTable
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
      </div>
    UIUtils.basicSparkPage(content, "History Server")
  }

  private val appHeader = Seq(
    "App ID",
    "App Name",
    "Started",
    "Completed",
    "Duration",
    "Spark User",
    "Last Updated")

  private val appWithAttemptHeader = Seq(
    "App ID",
    "App Name",
    "Attempt ID",
    "Started",
    "Completed",
    "Duration",
    "Spark User",
    "Last Updated")

  private def rangeIndices(
      range: Seq[Int],
      condition: Int => Boolean,
      showIncomplete: Boolean): Seq[Node] = {
    range.filter(condition).map(nextPage =>
      <a href={makePageLink(nextPage, showIncomplete)}> {nextPage} </a>)
  }

  private def attemptRow(
      renderAttemptIdColumn: Boolean,
      info: ApplicationHistoryInfo,
      attempt: ApplicationAttemptInfo,
      isFirst: Boolean): Seq[Node] = {
    val uiAddress = UIUtils.prependBaseUri(HistoryServer.getAttemptURI(info.id, attempt.attemptId))
    val startTime = UIUtils.formatDate(attempt.startTime)
    val endTime = if (attempt.endTime > 0) UIUtils.formatDate(attempt.endTime) else "-"
    val duration =
      if (attempt.endTime > 0) {
        UIUtils.formatDuration(attempt.endTime - attempt.startTime)
      } else {
        "-"
      }
    val lastUpdated = UIUtils.formatDate(attempt.lastUpdated)
    <tr>
      {
        if (isFirst) {
          if (info.attempts.size > 1 || renderAttemptIdColumn) {
            <td rowspan={info.attempts.size.toString} style="background-color: #ffffff">
              <a href={uiAddress}>{info.id}</a></td>
            <td rowspan={info.attempts.size.toString} style="background-color: #ffffff">
              {info.name}</td>
          } else {
            <td><a href={uiAddress}>{info.id}</a></td>
            <td>{info.name}</td>
          }
        } else {
          Nil
        }
      }
      {
        if (renderAttemptIdColumn) {
          if (info.attempts.size > 1 && attempt.attemptId.isDefined) {
            <td><a href={uiAddress}>{attempt.attemptId.get}</a></td>
          } else {
            <td>&nbsp;</td>
          }
        } else {
          Nil
        }
      }
      <td sorttable_customkey={attempt.startTime.toString}>{startTime}</td>
      <td sorttable_customkey={attempt.endTime.toString}>{endTime}</td>
      <td sorttable_customkey={(attempt.endTime - attempt.startTime).toString}>
        {duration}</td>
      <td>{attempt.sparkUser}</td>
      <td sorttable_customkey={attempt.lastUpdated.toString}>{lastUpdated}</td>
    </tr>
  }

  private def appRow(info: ApplicationHistoryInfo): Seq[Node] = {
    attemptRow(false, info, info.attempts.head, true)
  }

  private def appWithAttemptRow(info: ApplicationHistoryInfo): Seq[Node] = {
    attemptRow(true, info, info.attempts.head, true) ++
      info.attempts.drop(1).flatMap(attemptRow(true, info, _, false))
  }

  private def makePageLink(linkPage: Int, showIncomplete: Boolean): String = {
    UIUtils.prependBaseUri("/?" + Array(
      "page=" + linkPage,
      "showIncomplete=" + showIncomplete
      ).mkString("&"))
  }
}
