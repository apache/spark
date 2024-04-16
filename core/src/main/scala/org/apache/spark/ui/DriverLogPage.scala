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
package org.apache.spark.ui

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{LOG_TYPE, PATH}
import org.apache.spark.internal.config.DRIVER_LOG_LOCAL_DIR
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.DriverLogger.DRIVER_LOG_FILE
import org.apache.spark.util.logging.RollingFileAppender

/**
 * Live Spark Driver Log UI Page.
 *
 * This is similar with Spark worker's LogPage class.
 */
private[ui] class DriverLogPage(
    parent: DriverLogTab,
    conf: SparkConf)
  extends WebUIPage("") with Logging {
  require(conf.get(DRIVER_LOG_LOCAL_DIR).nonEmpty, s"Please specify ${DRIVER_LOG_LOCAL_DIR.key}.")

  private val supportedLogTypes = Set(DRIVER_LOG_FILE, "stderr", "stdout")
  private val defaultBytes = 100 * 1024
  private val logDir = conf.get(DRIVER_LOG_LOCAL_DIR).get

  def render(request: HttpServletRequest): Seq[Node] = {
    val logType = Option(request.getParameter("logType")).getOrElse(DRIVER_LOG_FILE)
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)
    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val curLogLength = endByte - startByte
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    val moreButton =
      <button type="button" onclick={"loadMore()"} class="log-more-btn btn btn-secondary">
        Load More
      </button>

    val newButton =
      <button type="button" onclick={"loadNew()"} class="log-new-btn btn btn-secondary">
        Load New
      </button>

    val alert =
      <div class="no-new-alert alert alert-info" style="display: none;">
        End of Log
      </div>

    val logParams = "/?logType=%s".format(logType)
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        Logs at {logDir}
        {range}
        <div class="log-content" style="height:80vh; overflow:auto; padding:5px;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    UIUtils.headerSparkPage(request, "Logs", content, parent)
  }

  def renderLog(request: HttpServletRequest): String = {
    val logType = Option(request.getParameter("logType")).getOrElse(DRIVER_LOG_FILE)
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  /** Get the part of the log files given the offset and desired length of bytes */
  private def getLog(
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int
    ): (String, Long, Long, Long) = {

    if (!supportedLogTypes.contains(logType)) {
      return ("Error: Log type must be one of " + supportedLogTypes.mkString(", "), 0, 0, 0)
    }

    try {
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, logType)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, conf))
      val totalLength = fileLengths.sum
      val offset = offsetOption.getOrElse(totalLength - byteLength)
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      val endIndex = math.min(startIndex + byteLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      val logText = Utils.offsetBytes(files, fileLengths, startIndex, endIndex)
      logDebug(s"Got log of length ${logText.length} bytes")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(log"Error getting ${MDC(LOG_TYPE, logType)} logs from directory " +
          log"${MDC(PATH, logDirectory)}", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}

/**
 * Live Spark Driver Log UI Tab.
 */
private[ui] class DriverLogTab(parent: SparkUI) extends SparkUITab(parent, "logs") {
  private val page = new DriverLogPage(this, parent.conf)
  attachPage(page)

  def getPage: DriverLogPage = page
}
