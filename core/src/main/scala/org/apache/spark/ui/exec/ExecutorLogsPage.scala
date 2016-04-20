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

import java.io.{IOException, FileNotFoundException}
import javax.servlet.http.HttpServletRequest

import org.apache.hadoop.fs.{FileContext, RemoteIterator, FileStatus, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, NodeId, ContainerId}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.logaggregation.{AggregatedLogFormat, LogAggregationUtils}
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager
import org.apache.hadoop.yarn.util.{Times, ConverterUtils}
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.ui.{UIUtils, WebUIPage}
import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

class ExecutorLogsPage(parent: ExecutorsTab) extends WebUIPage("executorLogs") with Logging {
  private val conf = parent.conf
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  hadoopConf.addResource("yarn-site.xml")

  case class LogLimits(var start: Long, var end: Long)

  override def render(request: HttpServletRequest): Seq[Node] = {

    val warnContent: ArrayBuffer[Node] = ArrayBuffer.empty
    val containerId = verifyAndGetContainerId(request, warnContent)
    val nodeId = verifyAndGetNodeId(request, warnContent)
    val appOwner = verifyAndGetAppOwner(request, warnContent)
    val logLimits = verifyAndGetLogLimits(request, warnContent)

    if (containerId.isEmpty ||
      nodeId.isEmpty || appOwner.isEmpty || logLimits.isEmpty) {
      return UIUtils.basicSparkPage(warnContent, "")
    }

    val applicationId = containerId.get.getApplicationAttemptId.getApplicationId

    val logEntity = containerId.map(_.getContainerId.toString)

    if (!hadoopConf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
      YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      val warnContent =
        <div><h2>{s"Aggregation is not enabled. Try the nodemanager at $nodeId"}</h2></div>
      return UIUtils.basicSparkPage(warnContent, "")
    }

    val remoteRootLogDir: Path = new Path(hadoopConf.get(
      YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR))
    val remoteAppDir: Path = LogAggregationUtils.getRemoteAppLogDir(
      remoteRootLogDir,
      applicationId,
      appOwner.get,
      LogAggregationUtils.getRemoteNodeLogDirSuffix(hadoopConf))
    var nodeFiles: RemoteIterator[FileStatus] = null

    try {
      val qualifiedLogDir: Path =
        FileContext.getFileContext(hadoopConf).makeQualified(remoteAppDir)
      nodeFiles = FileContext.getFileContext(
        qualifiedLogDir.toUri, hadoopConf).listStatus(remoteAppDir)
    } catch {
      case e: FileNotFoundException =>
        warnContent += <div><h2>{ s"Logs not available for  ${logEntity.get} " +
          s". Aggregation may not be complete, " +
          s"Check back later or try the nodemanager at ${nodeId.get};" }</h2></div>
        return UIUtils.basicSparkPage(warnContent, "")
      case e: Exception =>
        warnContent += <div><h2>{s"Error getting logs at ${nodeId.get}"}</h2></div>
        return UIUtils.basicSparkPage(warnContent, "")
    }


    val desiredLogType: Option[String] = getParameter(request, "logtype")
    try {
      while (nodeFiles.hasNext) {
        val thisNodeFile = nodeFiles.next()
        if (thisNodeFile.getPath.getName.contains(
          LogAggregationUtils.getNodeString(nodeId.get).split("_").head)
          && !thisNodeFile.getPath.getName.endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
          var reader: AggregatedLogFormat.LogReader = null
          try {
            val logUploadedTime = thisNodeFile.getModificationTime
            reader = new AggregatedLogFormat.LogReader(hadoopConf, thisNodeFile.getPath)
            val appAcls: java.util.Map[ApplicationAccessType, String] = reader.getApplicationAcls
            val owner: String = reader.getApplicationOwner
            val aclsManager = new ApplicationACLsManager(hadoopConf)
            aclsManager.addApplication(applicationId, appAcls)
            var remoteUser = request.getRemoteUser
            if (remoteUser == null) {
              remoteUser = UserGroupInformation.getCurrentUser.getShortUserName
            }
            var callerUGI: UserGroupInformation = null
            if (remoteUser != null) {
              callerUGI = UserGroupInformation.createRemoteUser(remoteUser)
            }

            if (!(callerUGI != null && !aclsManager.checkAccess(callerUGI,
              ApplicationAccessType.VIEW_APP, owner, applicationId))) {
              val logReader = reader.getContainerLogsReader(containerId.get)
              if (logReader != null) {
                val content = readContainerLogs(request, logReader, logLimits.get,
                  desiredLogType, logUploadedTime)
                return UIUtils.basicSparkPage(content, "")
              }
            }
          } catch {
            case e: IOException =>
              logError(s"Error getting logs for $logEntity, path:  ${thisNodeFile.getPath}", e)
          } finally {
            if (reader != null) {
              try {
                reader.close()
              } catch {
                case e: IOException =>
                  logError(s"Error close reader for this node File: ${thisNodeFile.getPath}", e)
              }
            }
          }
        }
      }

      if (desiredLogType.isEmpty) {
          warnContent +=
            <div><h1>{s"No logs available for container ${containerId.toString}"}</h1></div>
      } else {
        warnContent +=
          <div><h1>{
            s"Unable to locate $desiredLogType log for container ${containerId.toString}"
            }</h1></div>
      }
    } catch {
      case e: IOException =>
        warnContent += <div><h1>{s"Error getting logs for $logEntity"}</h1></div>
        logError(s"Error getting logs for $logEntity", e)
    }

    UIUtils.basicSparkPage(warnContent, "")
  }

  private def execRow(
      request: HttpServletRequest,
      logType: String,
      logReader: AggregatedLogFormat.ContainerLogsReader,
      logLimits: LogLimits,
      logUpLoadTime: Long): Seq[Node] = {
    val bufferSize: Int = 65536
    val cbuf: Array[Char] = new Array[Char](bufferSize)
    val logLength: Long = logReader.getCurrentLogLength
    var toRead: Long = -1
    var start: Long = -1
    <div><pre>
      Log Type: {logType}
      Log Upload Time: {Times.format(logUpLoadTime)}
      Log Length: {logLength.toString}
      {
        start = if (logLimits.start < 0) logLength + logLimits.start else logLimits.start
        start = math.min(math.max(start, 0), logLength)
        var end = if (logLimits.end < 0) logLength + logLimits.end else logLimits.end
        end = math.max(math.min(math.max(end, 0), logLength), start)
        toRead = end - start
        if (toRead < logLength) {
          val fullUrl = s"executorLogs/?${request.getQueryString}&logType=$logType&start=0"
          <span>Showing {toRead} bytes of {logLength
          } total. Click <a href={fullUrl}>here</a> for the full log.</span>
        } else {
          <span></span>
        }
      }
      {
        var totalSkipped: Long = 0
        while (totalSkipped < start) {
          var ret: Long = logReader.skip(start - totalSkipped)
          totalSkipped += ret
        }

        var currentToRead: Int = if (toRead > bufferSize) bufferSize else toRead.toInt

        Iterator.continually {
          logReader.read(cbuf, 0, currentToRead)
        }.takeWhile(toRead > 0 && _ > 0).map {
          case len =>
            toRead = toRead - len
            currentToRead = if (toRead > bufferSize) bufferSize else toRead.toInt
            <span>{new String(cbuf, 0, len)}</span>
        }
      }</pre></div>
  }

  private def readContainerLogs(
    request: HttpServletRequest,
    logReader: AggregatedLogFormat.ContainerLogsReader,
    logLimits: LogLimits,
    desiredLogType: Option[String],
    logUpLoadTime: Long): Seq[Node] = {

    Iterator.continually{
      logReader.nextLog()
    }.takeWhile { case logType =>
    logType != null && (desiredLogType.isEmpty || (desiredLogType.get == logType)) }.map {
      case logType =>
      execRow(request, logType, logReader, logLimits, logUpLoadTime)
    }.reduce(_ ++ _)
  }

  private def verifyAndGetContainerId(
      request: HttpServletRequest,
      content: ArrayBuffer[Node]): Option[ContainerId] = {
    val containerIdStr: Option[String] =
      Option(request.getParameter("containerId")).map(UIUtils.decodeURLParameter)
    if (containerIdStr.isEmpty) {
      content += <h1>"Cannot get container logs without a ContainerId"</h1>
      None
    } else {
      try {
        Some(ConverterUtils.toContainerId(containerIdStr.get))
      } catch {
        case e: IllegalArgumentException => {
          content +=
            <h1>"Cannot get container logs for invalid containerId: " + {containerIdStr}</h1>
          None
        }
      }
    }
  }

  private def verifyAndGetNodeId(
      request: HttpServletRequest,
      content: ArrayBuffer[Node]): Option[NodeId] = {
    val nodeIdStr: Option[String] = getParameter(request, "hostPort")
    if (nodeIdStr.isEmpty) {
      content += <h1>"Cannot get container logs without a NodeId"</h1>
      None
    } else {
      try {
        Some(ConverterUtils.toNodeId(nodeIdStr.get))
      } catch {
        case e: IllegalArgumentException => {
          content += <h1>"Cannot get container logs. Invalid nodeId: " {nodeIdStr.get}</h1>
          None
        }
      }
    }
  }

  private def verifyAndGetAppOwner(
      request: HttpServletRequest,
      content: ArrayBuffer[Node]): Option[String] = {
    val appOwner: Option[String] = getParameter(request, "user").map(UIUtils.decodeURLParameter)
    if (appOwner.isEmpty) {
      content +=
       <div>
         <h2>"Cannot get container logs without an app owner"</h2>
       </div>
    }
    appOwner
  }

  private def getParameter(request: HttpServletRequest, key: String): Option[String] = {
    val value = request.getParameter(key)
    if (value == null || value.isEmpty) {
      None
    } else {
      Some(value)
    }
  }

  private def verifyAndGetLogLimits(
      request: HttpServletRequest,
      content: ArrayBuffer[Node]): Option[LogLimits] = {
    var start: Long = -4096
    var end: Long = Long.MaxValue
    var isValid: Boolean = true
    val startStr: Option[String] = getParameter(request, "start").map(UIUtils.decodeURLParameter)
    if (startStr.isDefined) {
      try {
        start = startStr.get.toLong
      }
      catch {
        case e: NumberFormatException => {
          isValid = false
          content += <div><h2>"Invalid log start value: " + {startStr.get}</h2></div>
        }
      }
    }
    val endStr: Option[String] = getParameter(request, "end").map(UIUtils.decodeURLParameter)
    if (endStr.isDefined) {
      try {
        end = endStr.get.toLong
      }
      catch {
        case e: NumberFormatException => {
          isValid = false
          content += <div><h1>"Invalid log end value: " + {endStr.get} </h1></div>
        }
      }
    }

    if (!isValid) {
      None
    } else {
      Some(LogLimits(start, end))
    }
  }
}
