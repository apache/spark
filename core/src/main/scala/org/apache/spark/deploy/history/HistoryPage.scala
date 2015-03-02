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
import scala.collection.immutable.ListMap
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

private[spark] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  private val pageSize = 20
  private val plusOrMinus = 2

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedPage = Option(request.getParameter("page")).getOrElse("1").toInt
    val requestedFirst = (requestedPage - 1) * pageSize
    val requestedIncomplete =
      Option(request.getParameter("showIncomplete")).getOrElse("false").toBoolean

    val allCompletedAppsNAttempts = 
        parent.getApplicationList().filter(_.completed != requestedIncomplete)
    val (hasAttemptInfo, appToAttemptMap)  = getApplicationLevelList(allCompletedAppsNAttempts)
    
    val allAppsSize = allCompletedAppsNAttempts.size
    
    val actualFirst = if (requestedFirst < allAppsSize) requestedFirst else 0
    val apps = 
        allCompletedAppsNAttempts.slice(actualFirst, Math.min(actualFirst + pageSize, allAppsSize))
    val appWithAttemptsDisplayList = 
        appToAttemptMap.slice(actualFirst, Math.min(actualFirst + pageSize, allAppsSize))
        
    val actualPage = (actualFirst / pageSize) + 1
    val last = Math.min(actualFirst + pageSize, allAppsSize) - 1
    val pageCount = allAppsSize / pageSize + (if (allAppsSize % pageSize > 0) 1 else 0)

    val secondPageFromLeft = 2
    val secondPageFromRight = pageCount - 1

    val appTable = 
      if (hasAttemptInfo) {
        UIUtils.listingTable(appWithAttemptHeader, appWithAttemptRow, appWithAttemptsDisplayList)
      } else {
        UIUtils.listingTable(appHeader, appRow, apps) 
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
  
  private def getApplicationLevelList (appNattemptList: Iterable[ApplicationHistoryInfo])  ={
    // Create HashMap as per the multiple attempts for one application. 
    // If there is no attempt specific stuff, then
    // do return false, to indicate the same, so that previous UI gets displayed.
    var hasAttemptInfo = false
    val appToAttemptInfo = new HashMap[String, ArrayBuffer[ApplicationHistoryInfo]]
    for( appAttempt <- appNattemptList) {
      if(!appAttempt.appAttemptId.equals("")){
        hasAttemptInfo = true
        val attemptId = appAttempt.appAttemptId.toInt
         if(appToAttemptInfo.contains(appAttempt.id)){
           val currentAttempts = appToAttemptInfo.get(appAttempt.id).get
           currentAttempts += appAttempt
           appToAttemptInfo.put( appAttempt.id, currentAttempts) 
         } else {
           val currentAttempts = new ArrayBuffer[ApplicationHistoryInfo]()
           currentAttempts += appAttempt
           appToAttemptInfo.put( appAttempt.id, currentAttempts )
         }
      }else {
        val currentAttempts = new ArrayBuffer[ApplicationHistoryInfo]()
           currentAttempts += appAttempt
        appToAttemptInfo.put(appAttempt.id, currentAttempts)
      }
    } 
    val sortedMap = ListMap(appToAttemptInfo.toSeq.sortWith(_._1 > _._1):_*)
    (hasAttemptInfo, sortedMap)
  } 
  

  private val appHeader = Seq(
    "App ID",
    "App Name",
    "Started",
    "Completed",
    "Duration",
    "Spark User",
    "Last Updated")

  private def rangeIndices(range: Seq[Int], condition: Int => Boolean, showIncomplete: Boolean):
  Seq[Node] = {
    range.filter(condition).map(nextPage =>
      <a href={makePageLink(nextPage, showIncomplete)}> {nextPage} </a>)
  }
    
  private val appWithAttemptHeader = Seq(
    "App ID",
    "App Name",
    "Attempt ID",
    "Started",
    "Completed",
    "Duration",
    "Spark User",
    "Last Updated")

  private def appRow(info: ApplicationHistoryInfo): Seq[Node] = {
    val uiAddress = HistoryServer.UI_PATH_PREFIX + s"/${info.id}"
    val startTime = UIUtils.formatDate(info.startTime)
    val endTime = if (info.endTime > 0) UIUtils.formatDate(info.endTime) else "-"
    val duration =
      if (info.endTime > 0) UIUtils.formatDuration(info.endTime - info.startTime) else "-"
    val lastUpdated = UIUtils.formatDate(info.lastUpdated)
    <tr>
      <td><a href={uiAddress}>{info.id}</a></td>
      <td>{info.name}</td>
      <td sorttable_customkey={info.startTime.toString}>{startTime}</td>
      <td sorttable_customkey={info.endTime.toString}>{endTime}</td>
      <td sorttable_customkey={(info.endTime - info.startTime).toString}>{duration}</td>
      <td>{info.sparkUser}</td>
      <td sorttable_customkey={info.lastUpdated.toString}>{lastUpdated}</td>
    </tr>
  }
  
  private def getAttemptURI(attemptInfo: ApplicationHistoryInfo, 
                            returnEmptyIfAttemptInfoNull: Boolean = true ) = {
    if (attemptInfo.appAttemptId.equals("")) { 
      if(returnEmptyIfAttemptInfoNull) {
        attemptInfo.appAttemptId
      } else {
        HistoryServer.UI_PATH_PREFIX + s"/${attemptInfo.id}"
      }
   } else {
     HistoryServer.UI_PATH_PREFIX + s"/${attemptInfo.id}" + "_" +  s"${attemptInfo.appAttemptId}"
     }
  }
  
  private def firstAttemptRow(attemptInfo : ApplicationHistoryInfo)  = {
    val uiAddress = 
      if (attemptInfo.appAttemptId.equals("")) {
       attemptInfo.appAttemptId
      } else {
       HistoryServer.UI_PATH_PREFIX + s"/${attemptInfo.id}" + "_" +  s"${attemptInfo.appAttemptId}"
      }
              
    val startTime = UIUtils.formatDate(attemptInfo.startTime)
    val endTime = UIUtils.formatDate(attemptInfo.endTime)
    val duration = UIUtils.formatDuration(attemptInfo.endTime - attemptInfo.startTime)
    val lastUpdated = UIUtils.formatDate(attemptInfo.lastUpdated)
    val attemptId = attemptInfo.appAttemptId
       <td><a href={uiAddress}>{attemptId}</a></td>
       <td sorttable_customkey={attemptInfo.startTime.toString}>{startTime}</td>
      <td sorttable_customkey={attemptInfo.endTime.toString}>{endTime}</td>
      <td sorttable_customkey={(attemptInfo.endTime - attemptInfo.startTime).toString}>
                  {duration}</td>
      <td>{attemptInfo.sparkUser}</td>
      <td sorttable_customkey={attemptInfo.lastUpdated.toString}>{lastUpdated}</td>
  }
  
  private def attemptRow(attemptInfo: ApplicationHistoryInfo)  = {
    <tr>
      {firstAttemptRow(attemptInfo)}
    </tr>
  }
    
  private def appWithAttemptRow(
      appAttemptsInfo: (String,ArrayBuffer[ApplicationHistoryInfo])): Seq[Node] = {
    val applicationId = appAttemptsInfo._1
    val info  = appAttemptsInfo._2
    val rowSpan = info.length
    val rowSpanString = rowSpan.toString
    val applicatioName = info(0).name
    val lastAttemptURI = getAttemptURI(info(0), false)
    val ttAttempts = info.slice(1, rowSpan -1)
    val x = new xml.NodeBuffer
    x += 
    <tr>
      <td rowspan={rowSpanString}><a href={lastAttemptURI}>{applicationId}</a></td>
      <td rowspan={rowSpanString}>{applicatioName}</td>
      { firstAttemptRow(info(0)) }
    </tr>;
    for( i <- 1 until rowSpan ){
      x += attemptRow(info(i))
    }
      x
  }

  private def makePageLink(linkPage: Int, showIncomplete: Boolean): String = {
    "/?" + Array(
      "page=" + linkPage,
      "showIncomplete=" + showIncomplete
    ).mkString("&")
  }
}
