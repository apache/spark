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
package org.apache.spark.deploy.kubernetes.driverlauncher

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.Files
import org.apache.commons.codec.binary.Base64
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

/**
 * REST server responsible for helping a Kubernetes-backed Spark application
 * bootstrap its dependencies. This is expected to be run in a Kubernetes pod
 * and have environment variables bootstrapped to it.
*/
private[spark] class KubernetesDriverLauncherServer {

  private val port = System.getenv("SPARK_DRIVER_LAUNCHER_SERVER_PORT")
  assert(port != null, "SPARK_DRIVER_LAUNCHER_SERVER_PORT is not set.")
  private val secretFile = System.getenv("SPARK_DRIVER_LAUNCHER_APP_SECRET_LOCATION")
  assert(secretFile != null, "SPARK_DRIVER_LAUNCHER_APP_SECRET_LOCATION is not set.")
  private var jettyServer: Server = null

  // TODO(mcheah) use SSL
  private def startServer(): Server = {
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    jettyServer = new Server(threadPool)
    val connector = new ServerConnector(jettyServer)
    connector.setPort(port.toInt)
    jettyServer.addConnector(connector)
    val mainHandler = new ServletContextHandler
    val resourceConfig = new ResourceConfig()
    val applicationSecret = Base64.encodeBase64String(Files.toByteArray(new File(secretFile)))
    resourceConfig.registerInstances(
      new KubernetesDriverLauncherServiceImpl(applicationSecret, this)
          .asInstanceOf[KubernetesDriverLauncherService],
      new JacksonJsonProvider(new ObjectMapper().registerModule(new DefaultScalaModule)))
    mainHandler.setContextPath("/")
    val servletHolder = new ServletHolder("main", new ServletContainer(resourceConfig))
    mainHandler.addServlet(servletHolder, "/api/*")
    jettyServer.setHandler(mainHandler)
    jettyServer.start()
    jettyServer
  }

  def stop(): Unit = {
    jettyServer.stop()
  }
}

private[spark] object KubernetesDriverLauncherServer {

  def main(argStrings: Array[String]): Unit = {
    val server = new KubernetesDriverLauncherServer().startServer()
    server.join()
  }

}
