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

import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.server.{Handler, Server}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.ui.{JettyUtils, UIUtils}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(val worker: Worker, val workDir: File, requestedPort: Option[Int] = None)
  extends Logging {
  val timeout = AkkaUtils.askTimeout(worker.conf)
  val host = Utils.localHostName()
  val port = requestedPort.getOrElse(
    worker.conf.get("worker.ui.port",  WorkerWebUI.DEFAULT_PORT).toInt)

  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val indexPage = new IndexPage(this)

  val metricsHandlers = worker.metricsSystem.getServletHandlers

  val handlers = metricsHandlers ++ Array[(String, Handler)](
    ("/static", createStaticHandler(WorkerWebUI.STATIC_RESOURCE_DIR)),
    ("/log", (request: HttpServletRequest) => log(request)),
    ("/logPage", (request: HttpServletRequest) => logPage(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Worker web UI at http://%s:%d".format(host, bPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Worker JettyUtils", e)
        System.exit(1)
    }
  }

  def log(request: HttpServletRequest): String = {
    val defaultBytes = 100 * 1024

    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)

    val path = (appId, executorId, driverId) match {
      case (Some(a), Some(e), None) =>
        s"${workDir.getPath}/$appId/$executorId/$logType"
      case (None, None, Some(d)) =>
        s"${workDir.getPath}/$driverId/$logType"
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (startByte, endByte) = getByteRange(path, offset, byteLength)
    val file = new File(path)
    val logLength = file.length

    val pre = s"==== Bytes $startByte-$endByte of $logLength of $path ====\n"
    pre + Utils.offsetBytes(path, startByte, endByte)
  }

  def logPage(request: HttpServletRequest): Seq[scala.xml.Node] = {
    val defaultBytes = 100 * 1024
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)

    val (path, params) = (appId, executorId, driverId) match {
      case (Some(a), Some(e), None) =>
        (s"${workDir.getPath}/$a/$e/$logType", s"appId=$a&executorId=$e")
      case (None, None, Some(d)) =>
        (s"${workDir.getPath}/$d/$logType", s"driverId=$d")
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (startByte, endByte) = getByteRange(path, offset, byteLength)
    val file = new File(path)
    val logLength = file.length

    val logText = <node>{Utils.offsetBytes(path, startByte, endByte)}</node>

    val linkToMaster = <p><a href={worker.activeMasterWebUiUrl}>Back to Master</a></p>

    val range = <span>Bytes {startByte.toString} - {endByte.toString} of {logLength}</span>

    val backButton =
      if (startByte > 0) {
        <a href={"?%s&logType=%s&offset=%s&byteLength=%s"
          .format(params, logType, math.max(startByte-byteLength, 0), byteLength)}>
          <button type="button" class="btn btn-default">
            Previous {Utils.bytesToString(math.min(byteLength, startByte))}
          </button>
        </a>
      }
      else {
        <button type="button" class="btn btn-default" disabled="disabled">
          Previous 0 B
        </button>
      }

    val nextButton =
      if (endByte < logLength) {
        <a href={"?%s&logType=%s&offset=%s&byteLength=%s".
          format(params, logType, endByte, byteLength)}>
          <button type="button" class="btn btn-default">
            Next {Utils.bytesToString(math.min(byteLength, logLength-endByte))}
          </button>
        </a>
      }
      else {
        <button type="button" class="btn btn-default" disabled="disabled">
          Next 0 B
        </button>
      }

    val content =
      <html>
        <body>
          {linkToMaster}
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
    UIUtils.basicSparkPage(content, logType + " log page for " + appId)
  }

  /** Determine the byte range for a log or log page. */
  def getByteRange(path: String, offset: Option[Long], byteLength: Int)
  : (Long, Long) = {
    val defaultBytes = 100 * 1024
    val maxBytes = 1024 * 1024

    val file = new File(path)
    val logLength = file.length()
    val getOffset = offset.getOrElse(logLength-defaultBytes)

    val startByte =
      if (getOffset < 0) 0L
      else if (getOffset > logLength) logLength
      else getOffset

    val logPageLength = math.min(byteLength, maxBytes)

    val endByte = math.min(startByte+logPageLength, logLength)

    (startByte, endByte)
  }

  def stop() {
    server.foreach(_.stop())
  }
}

private[spark] object WorkerWebUI {
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_PORT="8081"
}
