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

import java.io.{BufferedInputStream, FileInputStream, OutputStream}
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.{Response, StreamingOutput, MediaType}

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
        val fileName: String = {
          attemptId match {
            case Some(id) => s"eventLogs-$appId-$id.zip"
            case None => s"eventLogs-$appId.zip"
          }
        }
        val stream = new StreamingOutput {
          override def write(output: OutputStream): Unit = {
            attemptId match {
              case Some(id) =>
                Utils.zipFilesToStream(hs.getEventLogPaths(appId, id), conf, output)
              case None =>
                val appInfo = hs.getApplicationInfoList.find(_.id == appId)
                appInfo match {
                  case Some(info) =>
                    val attempts = info.attempts
                    val files = new ArrayBuffer[Path]
                    attempts.foreach { attempt =>
                      attempt.attemptId.foreach { attemptId =>
                        logInfo(s"Attempt found: ${attemptId}")
                        files ++= hs.getEventLogPaths(appId, attemptId)
                      }
                    }
                    if (files.nonEmpty) {
                      Utils.zipFilesToStream(files, conf, output)
                    }
                  case None => logsNotFound = true
                }
            }
            output.flush()
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
          .entity("History Server is not running - cannot return event logs.")
          .status(Response.Status.SERVICE_UNAVAILABLE)
          .build()
    }
  }
}

private[v1] object EventLogDownloadResource {

  def unapply(resource: EventLogDownloadResource): Option[(UIRoot, String)] = {
    Some((resource.uIRoot, resource.appId))
  }
}
