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
package org.apache.spark.deploy.rest.kubernetes.v2

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.media.multipart.MultiPartFeature
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

private[spark] class ResourceStagingServer(
    port: Int,
    serviceInstance: ResourceStagingService) {

  private var jettyServer: Option[Server] = None

  def start(): Unit = synchronized {
    val threadPool = new QueuedThreadPool
    val contextHandler = new ServletContextHandler()
    val jsonProvider = new JacksonJaxbJsonProvider()
    jsonProvider.setMapper(new ObjectMapper().registerModule(new DefaultScalaModule))
    val resourceConfig = new ResourceConfig().registerInstances(
      serviceInstance,
      jsonProvider,
      new MultiPartFeature)
    val servletHolder = new ServletHolder("main", new ServletContainer(resourceConfig))
    contextHandler.setContextPath("/api/")
    contextHandler.addServlet(servletHolder, "/*")
    threadPool.setDaemon(true)
    val server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(port)
    server.addConnector(connector)
    server.setHandler(contextHandler)
    server.start()
    jettyServer = Some(server)
  }

  def stop(): Unit = synchronized {
    jettyServer.foreach(_.stop())
    jettyServer = None
  }
}
