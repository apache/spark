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

private[spark] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  private val pageSize = 20

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedPage = Option(request.getParameter("page")).getOrElse("1").toInt
    val requestedFirst = (requestedPage - 1) * pageSize

    val applicationNattemptsList = parent.getApplicationList()
    val (hasAttemptInfo, appToAttemptMap)  = getApplicationLevelList(applicationNattemptsList)
    val allAppsSize = if(hasAttemptInfo) appToAttemptMap.size else applicationNattemptsList.size
    val actualFirst = if (requestedFirst < allAppsSize) requestedFirst else 0
    val apps = applicationNattemptsList.slice(actualFirst, 
                                              Math.min(actualFirst + pageSize,
                                              allAppsSize))
    val appWithAttemptsDisplayList = appToAttemptMap.slice(actualFirst, 
                                                           Math.min(actualFirst + pageSize,
                                                           allAppsSize))
    val actualPage = (actualFirst / pageSize) + 1
    val last = Math.min(actualFirst + pageSize, allAppsSize) - 1
    val pageCount = allAppsSize / pageSize + (if (allAppsSize % pageSize > 0) 1 else 0)
 
     val appTable = if(hasAttemptInfo ) UIUtils.listingTable(appWithAttemptHeader,
                                         appWithAttemptRow,
                                         appWithAttemptsDisplayList)
                   else UIUtils.listingTable(appHeader, appRow, apps) 

    val providerConfig = parent.getProviderConfig()
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
          </ul>
          {
            if (allAppsSize > 0) {
              <h4>
                Showing {actualFirst + 1}-{last + 1} of {allAppsSize}
                <span style="float: right">
                  {if (actualPage > 1) <a href={"/?page=" + (actualPage - 1)}>&lt;</a>}
                  {if (actualPage < pageCount) <a href={"/?page=" + (actualPage + 1)}>&gt;</a>}
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
        </div>
      </div>
    UIUtils.basicSparkPage(content, "History Server")
  }

  private def getApplicationLevelList (appNattemptList: Iterable[ApplicationHistoryInfo])  ={
    // Create HashMap as per the multiple attempts for one application. 
    // If there is no attempt specific stuff, then
    // do return false, to indicate the same, so that previous UI gets displayed.
    var hasAttemptInfo = false
    val appToAttemptInfo = new scala.collection.mutable.HashMap[String, 
                                      scala.collection.mutable.ArrayBuffer[ApplicationHistoryInfo]]
    for( appAttempt <- appNattemptList) {
      if(appAttempt.id.contains("_attemptid_")){
        hasAttemptInfo = true
        val applicationId = appAttempt.id.substring(0, appAttempt.id.indexOf("_attemptid_"))
        val attemptId = getAttemptId(appAttempt.id)._1
         if(appToAttemptInfo.contains(applicationId)){
           val currentAttempts = appToAttemptInfo.get(applicationId).get
           currentAttempts += appAttempt
           appToAttemptInfo.put( applicationId, currentAttempts) 
         } else {
           val currentAttempts = new scala.collection.mutable.ArrayBuffer[ApplicationHistoryInfo]()
           currentAttempts += appAttempt
           appToAttemptInfo.put( applicationId, currentAttempts )
         }
      }else {
        val currentAttempts = new scala.collection.mutable.ArrayBuffer[ApplicationHistoryInfo]()
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

  private def appRow(info: ApplicationHistoryInfo): Seq[Node] = {
    val uiAddress = HistoryServer.UI_PATH_PREFIX + s"/${info.id}"
    val startTime = UIUtils.formatDate(info.startTime)
    val endTime = UIUtils.formatDate(info.endTime)
    val duration = UIUtils.formatDuration(info.endTime - info.startTime)
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

    
   private val appWithAttemptHeader = Seq(
    "App ID",
    "App Name",
    "Attempt ID",
    "Started",
    "Completed",
    "Duration",
    "Spark User",
    "Last Updated")
    
  
  private def firstAttemptRow(attemptInfo : ApplicationHistoryInfo)  = {
    val uiAddress = HistoryServer.UI_PATH_PREFIX + s"/${attemptInfo.id}"
    val startTime = UIUtils.formatDate(attemptInfo.startTime)
    val endTime = UIUtils.formatDate(attemptInfo.endTime)
    val duration = UIUtils.formatDuration(attemptInfo.endTime - attemptInfo.startTime)
    val lastUpdated = UIUtils.formatDate(attemptInfo.lastUpdated)
    val attemptId = getAttemptId(attemptInfo.id)._1
       <td><a href={uiAddress}>{attemptId}</a></td>
       <td sorttable_customkey={attemptInfo.startTime.toString}>{startTime}</td>
      <td sorttable_customkey={attemptInfo.endTime.toString}>{endTime}</td>
      <td sorttable_customkey={(attemptInfo.endTime - attemptInfo.startTime).toString}>
                  {duration}</td>
      <td>{attemptInfo.sparkUser}</td>
      <td sorttable_customkey={attemptInfo.lastUpdated.toString}>{lastUpdated}</td>

  }
  private def attemptRow(attemptInfo : ApplicationHistoryInfo)  = {
    <tr>
      {firstAttemptRow(attemptInfo)}
    </tr>
  }

    
  private def getAttemptId(value : String) = {
    if(value.contains("_attemptid_")) {
       (value.substring(value.indexOf("_attemptid_") + "_attemptid_".length), true )
    } else {
      // Instead of showing NA, show the application itself, in case of no attempt specific logging.
      // One can check the second value, to see if it has any attempt specific value or not
      (value, false )
    }  
  }
 
  private def appWithAttemptRow(appAttemptsInfo: (String, 
        scala.collection.mutable.ArrayBuffer[ApplicationHistoryInfo])): Seq[Node] = {
    val applicationId = appAttemptsInfo._1
    val info  = appAttemptsInfo._2
    val rowSpan = info.length
    val rowSpanString = rowSpan.toString
    val applicatioName = info(0).name
    val ttAttempts = info.slice(1, rowSpan -1)
    val x = new xml.NodeBuffer
    x += 
    <tr>
      <td rowspan={rowSpanString}>{applicationId}</td>
      <td rowspan={rowSpanString}>{applicatioName}</td>
      { firstAttemptRow(info(0)) }
    </tr>;
    for( i <- 1 until rowSpan ){
      x += attemptRow(info(i))
    }
      x
  }

}
