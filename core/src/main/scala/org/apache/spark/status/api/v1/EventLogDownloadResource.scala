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
package org.apache.spark.status.api.v1

import java.io.{File, FileOutputStream, OutputStream}
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.util.Utils

@Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
private[v1] class EventLogDownloadResource(
    val uIRoot: UIRoot,
    val appId: String,
    val attemptId: Option[String]) extends Logging {
  val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf)

  @GET
  def getEventLogs(): Response = {
    uIRoot match {
      case hs: HistoryServer =>
        var logsNotFound = false
        val fileName = {
          attemptId match {
            case Some(id) => s"eventLogs-$appId-$id.zip"
            case None => s"eventLogs-$appId.zip"
          }
        }
        val stream = new StreamingOutput {
          override def write(output: OutputStream): Unit = {
            val eventLogs = hs.getEventLogPaths(appId, attemptId)
            if (eventLogs.isEmpty) logsNotFound = true
            else zipLogFiles(eventLogs, output)
          }
        }
        if (logsNotFound) {
          Response.serverError()
            .entity(s"Event logs are not available for app: $appId.")
            .status(Response.Status.SERVICE_UNAVAILABLE)
            .build()
        } else {
          Response.ok(stream)
            .header("Content-Disposition", s"attachment; filename=$fileName")
            .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
            .build()
        }
      case _ =>
        Response.serverError()
          .entity("Event logs are only available through the history server.")
          .status(Response.Status.SERVICE_UNAVAILABLE)
          .build()
    }
  }

  private def zipLogFiles(eventLogs: Seq[Path], output: OutputStream): Unit = {
    val areLegacyLogs = eventLogs.headOption.exists { path =>
      path.getFileSystem(conf).isDirectory(path)
    }
    val pathsToZip = if (areLegacyLogs) {
      new ArrayBuffer[Path]()
    } else {
      eventLogs
    }
    var tempDir: File = null
    try {
      if (areLegacyLogs) {
        tempDir = Utils.createTempDir()
        Utils.chmod700(tempDir)
        eventLogs.foreach { logPath =>
          // If the event logs are directories (legacy), then create a zip file for each
          // one and write each of these files to the eventual output.
          val fs = logPath.getFileSystem(conf)
          val logFiles = fs.listFiles(logPath, true)
          val zipFile = new File(tempDir, logPath.getName + ".zip")
          pathsToZip.asInstanceOf[ArrayBuffer[Path]] += new Path(zipFile.toURI)
          val outputStream = new FileOutputStream(zipFile)
          val paths = new ArrayBuffer[Path]()
          while (logFiles.hasNext) {
            paths += logFiles.next().getPath
          }
          Utils.zipFilesToStream(paths, conf, outputStream)
        }
      }
      Utils.zipFilesToStream(pathsToZip, conf, output)
    } finally {
      if (tempDir != null) Utils.deleteRecursively(tempDir)
    }
  }
}
