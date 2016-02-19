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
package org.apache.spark.rpc.netty

import java.io.File

import org.apache.spark.{HttpFileServer, SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEnvFileServer

private[netty] class HttpBasedFileServer(conf: SparkConf, securityManager: SecurityManager)
  extends RpcEnvFileServer {

  @volatile private var httpFileServer: HttpFileServer = _

  override def addFile(file: File): String = {
    getFileServer().addFile(file)
  }

  override def addJar(file: File): String = {
    getFileServer().addJar(file)
  }

  def shutdown(): Unit = {
    if (httpFileServer != null) {
      httpFileServer.stop()
    }
  }

  private def getFileServer(): HttpFileServer = {
    if (httpFileServer == null) synchronized {
      if (httpFileServer == null) {
        httpFileServer = startFileServer()
      }
    }
    httpFileServer
  }

  private def startFileServer(): HttpFileServer = {
    val fileServerPort = conf.getInt("spark.fileserver.port", 0)
    val server = new HttpFileServer(conf, securityManager, fileServerPort)
    server.initialize()
    server
  }

}
