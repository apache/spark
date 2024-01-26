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

package org.apache.spark.deploy.master.ui

import java.io.File
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.RollingFileAppender

private[ui] class LogPage(parent: MasterWebUI) extends WebUIPage("logPage") with Logging {
  private val defaultBytes = 100 * 1024

  def render(request: HttpServletRequest): Seq[Node] = {
    val logDir = sys.env.getOrElse("SPARK_LOG_DIR", "logs/")
    val logType = request.getParameter("logType")
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

    val logParams = "?self&logType=%s".format(logType)
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        <p><a href="/">Back to Master</a></p>
        {range}
        <div class="log-content" style="height:80vh; overflow:auto; padding:5px;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    UIUtils.basicSparkPage(request, content, logType + " log page for master")
  }

  /** Get the part of the log files given the offset and desired length of bytes */
  private def getLog(
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int
    ): (String, Long, Long, Long) = {
    try {
      // Find a log file name
      val fileName = if (logType.equals("out")) {
        val normalizedUri = new File(logDirectory).toURI.normalize()
        val normalizedLogDir = new File(normalizedUri.getPath)
        normalizedLogDir.listFiles.map(_.getName).filter(_.endsWith(".out"))
          .headOption.getOrElse(logType)
      } else {
        logType
      }
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, fileName)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, parent.master.conf))
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
        logError(s"Error getting $logType logs from directory $logDirectory", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}
