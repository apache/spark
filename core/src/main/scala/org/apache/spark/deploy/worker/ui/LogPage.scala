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

package org.apache.spark.deploy.worker.ui

import java.io.File

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{LOG_TYPE, PATH}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.RollingFileAppender

private[ui] class LogPage(parent: WorkerWebUI) extends WebUIPage("logPage") with Logging {
  private val worker = parent.worker
  private val workDir = new File(parent.workDir.toURI.normalize().getPath)
  private val supportedLogTypes = Set("stderr", "stdout", "out")
  private val defaultBytes = 100 * 1024

  def renderLog(request: HttpServletRequest): String = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val self = Option(request.getParameter("self"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

    val logDir = (appId, executorId, driverId, self) match {
      case (Some(a), Some(e), None, None) =>
        s"${workDir.getPath}/$a/$e/"
      case (None, None, Some(d), None) =>
        s"${workDir.getPath}/$d/"
      case (None, None, None, Some(_)) =>
        s"${sys.env.getOrElse("SPARK_LOG_DIR", workDir.getPath)}/"
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val self = Option(request.getParameter("self"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

    val (logDir, params, pageName) = (appId, executorId, driverId, self) match {
      case (Some(a), Some(e), None, None) =>
        (s"${workDir.getPath}/$a/$e/", s"appId=$a&executorId=$e", s"$a/$e")
      case (None, None, Some(d), None) =>
        (s"${workDir.getPath}/$d/", s"driverId=$d", d)
      case (None, None, None, Some(_)) =>
        (s"${sys.env.getOrElse("SPARK_LOG_DIR", workDir.getPath)}/", "self", "worker")
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val linkToMaster = <p><a href={worker.activeMasterWebUiUrl}>Back to Master</a></p>
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

    val logParams = "?%s&logType=%s".format(params, logType)
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        {linkToMaster}
        {range}
        <div class="log-content" style="height:80vh; overflow:auto; padding:5px;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    UIUtils.basicSparkPage(request, content, logType + " log page for " + pageName)
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

    // Verify that the normalized path of the log directory is in the working directory
    val normalizedUri = new File(logDirectory).toURI.normalize()
    val normalizedLogDir = new File(normalizedUri.getPath)
    if (!Utils.isInDirectory(workDir, normalizedLogDir)) {
      return ("Error: invalid log directory " + logDirectory, 0, 0, 0)
    }

    try {
      // Find a log file name
      val fileName = if (logType.equals("out")) {
        normalizedLogDir.listFiles.map(_.getName).filter(_.endsWith(".out"))
          .headOption.getOrElse(logType)
      } else {
        logType
      }
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, fileName)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, worker.conf))
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
        logError(log"Error getting ${MDC(LOG_TYPE, logType)} logs from " +
          log"directory ${MDC(PATH, logDirectory)}", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}
