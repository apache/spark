/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli.thrift

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.Shell
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}
import org.apache.thrift.server.TServlet
import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.{ExecutorThreadPool, ScheduledExecutorScheduler}

import org.apache.spark.service.cli.thrift.TCLIService
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.CLIService
import org.apache.spark.sql.hive.thriftserver.server.NamedThreadFactory


class ThriftHttpCLIService(cliService: CLIService)
  extends ThriftCLIService(cliService, classOf[ThriftHttpCLIService].getSimpleName) {
  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target URL to differ,
   * e.g. http://gateway:port/hive2/servlets/thrifths2/
   */
  override def run(): Unit = {
    try { // Server thread pool
      // Start with minWorkerThreads, expand till maxWorkerThreads and reject subsequent requests
      val threadPoolName: String = "HiveServer2-HttpHandler-Pool"
      val executorService: ThreadPoolExecutor =
        new ThreadPoolExecutor(minWorkerThreads,
          maxWorkerThreads,
          workerKeepAliveTime,
          TimeUnit.SECONDS,
          new SynchronousQueue[Runnable],
          new NamedThreadFactory(threadPoolName))
      val threadPool: ExecutorThreadPool = new ExecutorThreadPool(executorService)
      // HTTP Server
      httpServer = new org.eclipse.jetty.server.Server(threadPool)
      // Connector configs
      var connectionFactories: Array[ConnectionFactory] = null
      val useSsl: Boolean = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)
      val schemeName: String = if (useSsl) {
        "https"
      } else {
        "http"
      }
      // Change connector if SSL is used
      if (useSsl) {
        val keyStorePath: String = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim
        val keyStorePassword: String =
          ShimLoader.getHadoopShims
            .getPassword(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname)
        if (keyStorePath.isEmpty) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname +
            " Not configured for SSL connection")
        }
        val sslContextFactory: SslContextFactory = new SslContextFactory.Server
        val excludedProtocols: Array[String] =
          hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")
        logInfo("HTTP Server SSL: adding excluded protocols: " + excludedProtocols.mkString(","))
        sslContextFactory.addExcludeProtocols(excludedProtocols: _*)
        logInfo("HTTP Server SSL: SslContextFactory.getExcludeProtocols = "
          + sslContextFactory.getExcludeProtocols.mkString(","))
        sslContextFactory.setKeyStorePath(keyStorePath)
        sslContextFactory.setKeyStorePassword(keyStorePassword)
        connectionFactories =
          AbstractConnectionFactory.getFactories(sslContextFactory, new HttpConnectionFactory)
      } else {
        connectionFactories = Array[ConnectionFactory](new HttpConnectionFactory)
      }
      val connector: ServerConnector =
        new ServerConnector(
          httpServer,
          null,
          new ScheduledExecutorScheduler("SparkThriftServer-HttpHandler-JettyScheduler", true),
          null,
          -1,
          -1,
          connectionFactories: _*)
      connector.setPort(portNum)
      // Linux:yes, Windows:no
      connector.setReuseAddress(!Shell.WINDOWS)
      val maxIdleTime: Int = hiveConf.getTimeVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME,
        TimeUnit.MILLISECONDS).toInt
      connector.setIdleTimeout(maxIdleTime)
      httpServer.addConnector(connector)
      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf)
      val processor: TProcessor = new TCLIService.Processor[TCLIService.Iface](this)
      val protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory
      // Set during the init phase of HiveServer2 if auth mode is kerberos
      // UGI for the hive/_HOST (kerberos) principal
      val serviceUGI: UserGroupInformation = cliService.getServiceUGI
      // UGI for the http/_HOST (SPNego) principal
      val httpUGI: UserGroupInformation = cliService.getHttpUGI
      val authType: String = hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
      val thriftHttpServlet: TServlet =
        new ThriftHttpServlet(processor, protocolFactory, authType, serviceUGI, httpUGI)
      // Context handler
      val context: ServletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS)
      context.setContextPath("/")
      val httpPath: String =
        getHttpPath(hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH))
      httpServer.setHandler(context)
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath)
      // TODO: check defaults: maxTimeout, keepalive, maxBodySize, bodyRecieveDuration, etc.
      // Finally, start the server
      httpServer.start()
      val msg: String = "Started " + classOf[ThriftHttpCLIService].getSimpleName +
        " in " + schemeName + " mode on port " + connector.getLocalPort +
        " path=" + httpPath + " with " + minWorkerThreads + "..." +
        maxWorkerThreads + " worker threads"
      logInfo(msg)
      httpServer.join()
    } catch {
      case t: Throwable =>
        logError("Error starting SparkThriftServer: could not start " +
          classOf[ThriftHttpCLIService].getSimpleName, t)
        System.exit(-1)
    }
  }

  private def getHttpPath(path: String): String = {
    var httpPath = path
    if (httpPath == null || httpPath == "") {
      httpPath = "/*"
    }
    else {
      if (!httpPath.startsWith("/")) {
        httpPath = "/" + httpPath
      }
      if (httpPath.endsWith("/")) {
        httpPath = httpPath + "*"
      }
      if (!httpPath.endsWith("/*")) {
        httpPath = httpPath + "/*"
      }
    }
    httpPath
  }

}