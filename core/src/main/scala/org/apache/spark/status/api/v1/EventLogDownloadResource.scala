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

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.util.Utils

@Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
private[v1] class EventLogDownloadResource(val uIRoot: UIRoot, val appId: String) {

  @GET
  def getEventLogs(): Response = {
    uIRoot match {
      case hs: HistoryServer =>
        val dir = Utils.createTempDir()
        Utils.chmod700(dir)
        hs.copyEventLogsToDirectory(appId, dir)
        dir.listFiles().headOption.foreach { file =>
          val stream  = new StreamingOutput {
            override def write(output: OutputStream): Unit = {
              try {
                val inStream = new BufferedInputStream(new FileInputStream(file))
                val buffer = new Array[Byte](1024 * 1024)
                var dataRemains = true
                while (dataRemains) {
                  val read = inStream.read(buffer)
                  if (read > 0) {
                    output.write(buffer, 0, read)
                  } else {
                    dataRemains = false
                  }
                }
                output.flush()
              } finally {
                Utils.deleteRecursively(dir)
              }
            }
          }
          return Response.ok(stream)
            .header("Content-Length", file.length().toString)
            .header("Content-Disposition", s"attachment; filename=${file.getName}")
            .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
            .build()
        }
        Response.serverError()
          .entity(s"Event logs for $appId not found.")
          .status(Response.Status.NOT_FOUND)
          .build()
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
