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

import java.io.{FileInputStream, OutputStream, File, InputStream}
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.{MultivaluedMap, MediaType}

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.util.Utils

@Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
private[v1] class EventLogDownloadResource(val uIRoot: UIRoot, val appId: String) {

  @GET
  def getEventLogs(headers: MultivaluedMap[String, AnyRef], outputStream: OutputStream): Unit = {
    uIRoot match {
      case hs: HistoryServer =>
        val dir = Utils.createTempDir()
        Utils.chmod700(dir)
        hs.copyEventLogsToDirectory(appId, dir)
        dir.listFiles().headOption.foreach { zipFile =>
          headers.add("Content-Length", zipFile.length().toString)
          headers.add("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
          headers.add("Content-Disposition", s"attachment; filename=${zipFile.getName}")
          var inputStream: InputStream = null
          try {
            inputStream = new FileInputStream(zipFile)
            val buffer = new Array[Byte](1024 * 1024)
            var remaining = true
            while (remaining) {
              val read = inputStream.read(buffer)
              if (read != -1) {
                outputStream.write(buffer, 0, read)
              } else {
                remaining = false
              }
            }
            outputStream.flush()
          } finally {
            inputStream.close()
            Utils.deleteRecursively(dir)
          }
        }
      case _ => outputStream.write(
        s"File download not available for application : $appId".getBytes("utf-8"))
    }
  }
}

private[v1] object EventLogDownloadResource {

  def unapply(resource: EventLogDownloadResource): Option[(UIRoot, String)] = {
    Some((resource.uIRoot, resource.appId))
  }
}
