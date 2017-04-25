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

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.eclipse.jetty.http.HttpVersion
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector, SslConnectionFactory}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.glassfish.jersey.media.multipart.MultiPartFeature
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class ResourceStagingServer(
    port: Int,
    serviceInstance: ResourceStagingService,
    sslOptionsProvider: ResourceStagingServerSslOptionsProvider) extends Logging {

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
    val resolvedConnectionFactories = sslOptionsProvider.getSslOptions
      .createJettySslContextFactory()
      .map(sslFactory => {
        val sslConnectionFactory = new SslConnectionFactory(
          sslFactory, HttpVersion.HTTP_1_1.asString())
        val rawHttpConfiguration = new HttpConfiguration()
        rawHttpConfiguration.setSecureScheme("https")
        rawHttpConfiguration.setSecurePort(port)
        val rawHttpConnectionFactory = new HttpConnectionFactory(rawHttpConfiguration)
        Array(sslConnectionFactory, rawHttpConnectionFactory)
      }).getOrElse(Array(new HttpConnectionFactory()))
    val server = new Server(threadPool)
    val connector = new ServerConnector(
      server,
      null,
      // Call this full constructor to set this, which forces daemon threads:
      new ScheduledExecutorScheduler("DependencyServer-Executor", true),
      null,
      -1,
      -1,
      resolvedConnectionFactories: _*)
    connector.setPort(port)
    server.addConnector(connector)
    server.setHandler(contextHandler)
    server.start()
    jettyServer = Some(server)
    logInfo(s"Resource staging server started on port $port.")
  }

  def join(): Unit = jettyServer.foreach(_.join())

  def stop(): Unit = synchronized {
    jettyServer.foreach(_.stop())
    jettyServer = None
  }
}

object ResourceStagingServer {
  def main(args: Array[String]): Unit = {
    val sparkConf = if (args.nonEmpty) {
      SparkConfPropertiesParser.getSparkConfFromPropertiesFile(new File(args(0)))
    } else {
      new SparkConf(true)
    }
    val dependenciesRootDir = Utils.createTempDir(namePrefix = "local-application-dependencies")
    val serviceInstance = new ResourceStagingServiceImpl(dependenciesRootDir)
    val sslOptionsProvider = new ResourceStagingServerSslOptionsProviderImpl(sparkConf)
    val server = new ResourceStagingServer(
      port = sparkConf.get(RESOURCE_STAGING_SERVER_PORT),
      serviceInstance = serviceInstance,
      sslOptionsProvider = sslOptionsProvider)
    server.start()
    try {
      server.join()
    } finally {
      server.stop()
    }
  }
}
