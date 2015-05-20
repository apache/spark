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

import java.io.{BufferedInputStream, FileInputStream, OutputStream, File, InputStream}
import javax.ws.rs.ext.Provider
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.{StreamingOutput, MultivaluedMap, MediaType}

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.util.Utils


private[v1] class EventLogDownloadResource(val uIRoot: UIRoot, val appId: String) {

  private def getErrorOutput(err: String): StreamingOutput = {
    new StreamingOutput {
      override def write(outputStream: OutputStream): Unit = {
        outputStream.write(
          s"File download not available for application : $appId due to $err".getBytes("utf-8"))
      }
    }
  }

  @GET
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def getEventLogs(): StreamingOutput = {
    uIRoot match {
      case hs: HistoryServer =>
        val dir = Utils.createTempDir()
        Utils.chmod700(dir)
        hs.copyEventLogsToDirectory(appId, dir)
        dir.listFiles().headOption.foreach { file =>
          return new StreamingOutput {
            override def write(output: OutputStream): Unit = {
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
            }
          }
        }
        getErrorOutput("No files in dir.")
      case _ => getErrorOutput("hs not history server")
    }
  }
}

private[v1] object EventLogDownloadResource {

  def unapply(resource: EventLogDownloadResource): Option[(UIRoot, String)] = {
    Some((resource.uIRoot, resource.appId))
  }
}
