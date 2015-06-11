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

package org.apache.spark.ui.exec

import java.io.DataInputStream
import javax.servlet.http.HttpServletRequest

import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey
import org.apache.hadoop.yarn.logaggregation.{AggregatedLogFormat, LogAggregationUtils}
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

import scala.xml.Node

private[ui] class LogPage(
    parent: ExecutorsTab)
  extends WebUIPage("logPage") with Logging {

  def render(request: HttpServletRequest): Seq[Node] = {
    val defaultBytes = 100 * 1024
    val appId = parent.appId
    val containerId = Option(request.getParameter("containerId"))
    val nodeAddress = Option(request.getParameter("nodeAddress"))
    val appOwner = Option(request.getParameter("appOwner"))
    val logType = Option(request.getParameter("logType"))
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)

    if (!(containerId.isDefined && nodeAddress.isDefined && appOwner.isDefined &&
      logType.isDefined)) {
      throw new Exception("Request must specify appId, containerId, appOwner and logType!")
    }

    val (logText, startByte, endByte, logLength) = getLog(
      appId, containerId.get, nodeAddress.get, appOwner.get, logType.get, offset, byteLength)

    val range = <span>Bytes {startByte.toString} - {endByte.toString} of {logLength}</span>

    val backButton =
      if (startByte > 0) {
        <a href={"?containerId=%s&nodeAddress=%s&appOwner=%s&logType=%s&offset=%s&byteLength=%s"
          .format(containerId.get, nodeAddress.get, appOwner.get, logType.get,
            math.max(startByte - byteLength, 0), byteLength)}>
          <button type="button" class="btn btn-default">
            Previous {Utils.bytesToString(math.min(byteLength, startByte))}
          </button>
        </a>
      } else {
        <button type="button" class="btn btn-default" disabled="disabled">
          Previous 0 B
        </button>
      }

    val nextButton =
      if (endByte < logLength) {
        <a href={"?containerId=%s&nodeAddress=%s&appOwner=%s&logType=%s&offset=%s&byteLength=%s"
          .format(containerId.get, nodeAddress.get, appOwner.get, logType.get,
            endByte, byteLength)}>
          <button type="button" class="btn btn-default">
            Next {Utils.bytesToString(math.min(byteLength, logLength - endByte))}
          </button>
        </a>
      } else {
        <button type="button" class="btn btn-default" disabled="disabled">
          Next 0 B
        </button>
      }

    val content =
      <html>
        <body>
          <div>
            <div style="float:left; margin-right:10px">{backButton}</div>
            <div style="float:left;">{range}</div>
            <div style="float:right; margin-left:10px">{nextButton}</div>
          </div>
          <br />
          <div style="height:500px; overflow:auto; padding:5px;">
            <pre>{logText}</pre>
          </div>
        </body>
      </html>
    UIUtils.basicSparkPage(content, "Log page for " + appId)
  }

  /** Get the part of the aggregated log file given the offset and desired length of bytes */
  private def getLog(
      appId: String,
      containerId: String,
      nodeAddress: String,
      appOwner: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int
    ): (String, Long, Long, Long) = {
    val yarnConf = new YarnConfiguration(
      SparkHadoopUtil.get.newConfiguration(parent.conf))
    var reader: AggregatedLogFormat.LogReader = null

    try {
      val filePath = getFilePath(yarnConf, appId, containerId, nodeAddress, appOwner)
      reader = new AggregatedLogFormat.LogReader(yarnConf, filePath)
      var key = new LogKey()
      var inputStream = reader.next(key)
      while (inputStream != null && !key.toString.equals(containerId)) {
        key = new LogKey()
        inputStream = reader.next(key)
      }

      var fileType = inputStream.readUTF()
      var fileLen = inputStream.readUTF().toLong
      while (fileType != logType) {
        skip(inputStream, fileLen)
        fileType = inputStream.readUTF()
        fileLen = inputStream.readUTF().toLong
      }

      val offset = offsetOption.getOrElse(fileLen - byteLength)
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > fileLen) {
          fileLen
        } else {
          offset
        }
      }
      val endIndex = math.min(startIndex + byteLength, fileLen)
      logDebug(s"Getting log from $startIndex to $endIndex")

      if (skip(inputStream, startIndex) == startIndex) {
        val buf = new Array[Byte]((endIndex - startIndex).toInt)
        inputStream.readFully(buf)
        val logText = new String(buf)
        logDebug(s"Got log of length ${logText.length} bytes")
        (logText, startIndex, endIndex, fileLen)
      } else {
        (s"Failed to read the log from offset $startIndex.", 0, 0, 0)
      }
    } catch {
      case e: Exception =>
        logError("Error getting log ", e)
        ("The log does not exist. Please make sure log aggregation is enabled and finished", 0, 0, 0)
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  private def skip(
      inputStream: DataInputStream,
      n: Long
    ) : Long = {
    var totalSkiped = 0L
    while (totalSkiped < n) {
      val skipped = inputStream.skip(n - totalSkiped)
      if (skipped <= 0) {
        return totalSkiped
      }
      totalSkiped += skipped
    }
    totalSkiped
  }

  private def getFilePath(
      yarnConf: YarnConfiguration,
      appId: String,
      containerId: String,
      nodeAddress: String,
      appOwner: String
    ): Path = {
    val remoteRootLogDir = new Path(yarnConf.get(
      YarnConfiguration.NM_REMOTE_APP_LOG_DIR, YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR))
    val suffix = yarnConf.get(
      YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX)

    val remoteLogDir = new Path(remoteRootLogDir, appOwner)
    val remoteLogSuffixedDir = if (suffix.isEmpty) remoteLogDir else new Path(remoteLogDir, suffix)
    val remoteAppLogDir = new Path(remoteLogSuffixedDir, appId)
    val qualifiedLogDir = FileContext.getFileContext(yarnConf).makeQualified(remoteAppLogDir)

    val files = FileContext.getFileContext(qualifiedLogDir.toUri, yarnConf)
      .listStatus(remoteAppLogDir)

    while (files.hasNext) {
      val thisFile = files.next
      val fileName = thisFile.getPath.getName
      val nodeString = nodeAddress.replace(":", "_")
      if (fileName.contains(nodeString) && !fileName.endsWith(".tmp")) {
        return thisFile.getPath
      }
    }
    null
  }
}
