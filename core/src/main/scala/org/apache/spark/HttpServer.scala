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

package org.apache.spark

import java.io.File
import java.net.InetAddress

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.bio.SocketConnector
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.apache.spark.util.Utils

/**
 * Exception type thrown by HttpServer when it is in the wrong state for an operation.
 */
private[spark] class ServerStateException(message: String) extends Exception(message)

/**
 * An HTTP server for static content used to allow worker nodes to access JARs added to SparkContext
 * as well as classes created by the interpreter when the user types in code. This is just a wrapper
 * around a Jetty server.
 */
private[spark] class HttpServer(resourceBase: File) extends Logging {
  private var server: Server = null
  private var port: Int = -1

  def start() {
    if (server != null) {
      throw new ServerStateException("Server is already started")
    } else {
      server = new Server()
      val connector = new SocketConnector
      connector.setMaxIdleTime(60*1000)
      connector.setSoLingerTime(-1)
      connector.setPort(0)
      server.addConnector(connector)

      val threadPool = new QueuedThreadPool
      threadPool.setDaemon(true)
      server.setThreadPool(threadPool)
      val resHandler = new ResourceHandler
      resHandler.setResourceBase(resourceBase.getAbsolutePath)
      val handlerList = new HandlerList
      handlerList.setHandlers(Array(resHandler, new DefaultHandler))
      server.setHandler(handlerList)
      server.start()
      port = server.getConnectors()(0).getLocalPort()
    }
  }

  def stop() {
    if (server == null) {
      throw new ServerStateException("Server is already stopped")
    } else {
      server.stop()
      port = -1
      server = null
    }
  }

  /**
   * Get the URI of this HTTP server (http://host:port)
   */
  def uri: String = {
    if (server == null) {
      throw new ServerStateException("Server is not started")
    } else {
      return "http://" + Utils.localIpAddress + ":" + port
    }
  }
}
