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

import java.io.OutputStream
import java.util.zip.ZipOutputStream
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}

import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil

@Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
private[v1] class EventLogDownloadResource(
    val uIRoot: UIRoot,
    val appId: String,
    val attemptId: Option[String]) extends Logging {
  val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf)

  @GET
  def getEventLogs(): Response = {
    try {
      val fileName = {
        attemptId match {
          case Some(id) => s"eventLogs-$appId-$id.zip"
          case None => s"eventLogs-$appId.zip"
        }
      }

      val stream = new StreamingOutput {
        override def write(output: OutputStream): Unit = {
          val zipStream = new ZipOutputStream(output)
          try {
            uIRoot.writeEventLogs(appId, attemptId, zipStream)
          } finally {
            zipStream.close()
          }

        }
      }

      Response.ok(stream)
        .header("Content-Disposition", s"attachment; filename=$fileName")
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
        .build()
    } catch {
      case NonFatal(e) =>
        Response.serverError()
          .entity(s"Event logs are not available for app: $appId.")
          .status(Response.Status.SERVICE_UNAVAILABLE)
          .build()
    }
  }
}
