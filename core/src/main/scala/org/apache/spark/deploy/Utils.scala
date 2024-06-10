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

package org.apache.spark.deploy

import java.io.File

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{LOG_TYPE, PATH}
import org.apache.spark.ui.JettyUtils.createServletHandler
import org.apache.spark.ui.WebUI
import org.apache.spark.util.Utils.{getFileLength, offsetBytes}
import org.apache.spark.util.logging.RollingFileAppender

/**
 * An object to provide utility methods for Spark deploy module.
 */
private[deploy] object Utils extends Logging {
  val DEFAULT_BYTES = 100 * 1024
  val SUPPORTED_LOG_TYPES = Set("stderr", "stdout", "out")

  def addRenderLogHandler(page: WebUI, conf: SparkConf): Unit = {
    page.attachHandler(createServletHandler("/log",
      (request: HttpServletRequest) => renderLog(request, conf),
      conf))
  }

  private def renderLog(request: HttpServletRequest, conf: SparkConf): String = {
    val logDir = sys.env.getOrElse("SPARK_LOG_DIR", "logs/")
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(DEFAULT_BYTES)

    val (logText, startByte, endByte, logLength) = getLog(conf, logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  /** Get the part of the log files given the offset and desired length of bytes */
  def getLog(
      conf: SparkConf,
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int): (String, Long, Long, Long) = {
    if (!SUPPORTED_LOG_TYPES.contains(logType)) {
      return ("Error: Log type must be one of " + SUPPORTED_LOG_TYPES.mkString(", "), 0, 0, 0)
    }
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

      val fileLengths: Seq[Long] = files.map(getFileLength(_, conf))
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
      val logText = offsetBytes(files, fileLengths, startIndex, endIndex)
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
