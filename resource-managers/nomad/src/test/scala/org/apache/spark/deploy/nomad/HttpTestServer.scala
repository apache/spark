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

package org.apache.spark.deploy.nomad

import java.io.File
import java.nio.charset.StandardCharsets
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.{FileUtils, IOUtils}
import org.eclipse.jetty.server.{AbstractNetworkConnector, Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler

import org.apache.spark.util.Utils

class HttpTestServer(hostnameInUrls: String) {
  import HttpTestServer._

  private var fileAndChecksumMappings = Map.empty[String, (File, String)]
  private var putMappings = Map.empty[String, Array[Byte]]

  private val server = new Server(0)
  server.setHandler(new AbstractHandler {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest,
        response: HttpServletResponse): Unit = {

      baseRequest.getMethod match {
        case "GET" =>
          putMappings.get(baseRequest.getRequestURI) match {
            case Some(bytes) =>
              response.setStatus(200)
              IOUtils.write(bytes, response.getOutputStream)
              baseRequest.setHandled(true)
            case None =>
              fileAndChecksumMappings.get(baseRequest.getRequestURI)
                .foreach { case (file, _) =>
                  response.setStatus(200)
                  FileUtils.copyFile(file, response.getOutputStream)
                  baseRequest.setHandled(true)
                }
          }
        case "PUT" =>
          response.setStatus(201)
          putMappings += baseRequest.getRequestURI -> IOUtils.toByteArray(request.getInputStream)
          baseRequest.setHandled(true)
        case method =>
          response.setStatus(405)
          baseRequest.setHandled(true)
      }
    }
  })
  server.start()

  def port: Int =
    server.getConnectors.head.asInstanceOf[AbstractNetworkConnector].getLocalPort

  def addMapping(path: String, file: File): String = {
    require(file.isFile, s"$file must be a file, but isn't")
    fileAndChecksumMappings += path -> (file, checksum(file))
    url(path)
  }

  def url(path: String): String = {
    val query = fileAndChecksumMappings.get(path) match {
      case None => ""
      case Some((_, checksum)) => s"?checksum=$checksum"
    }
    s"http://$hostnameInUrls:$port$path$query"
  }

  def valuePutToPath(path: String): Option[String] =
    putMappings.get(path)
      .map(new String(_, StandardCharsets.UTF_8))

  def close(): Unit =
    server.stop()

}

object HttpTestServer {

  def apply(hostnameInUrls: Option[String]): HttpTestServer =
    new HttpTestServer(hostnameInUrls.getOrElse(Utils.localHostNameForURI()))

  private def checksum(file: File): String = {
    "md5:" + Utils.tryWithResource(FileUtils.openInputStream(file))(DigestUtils.md5Hex)
  }

}
