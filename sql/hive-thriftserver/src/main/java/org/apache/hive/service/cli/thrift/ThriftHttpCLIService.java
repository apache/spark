/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.thrift;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;


public class ThriftHttpCLIService extends ThriftCLIService {

  public ThriftHttpCLIService(CLIService cliService) {
    super(cliService, ThriftHttpCLIService.class.getSimpleName());
  }

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target URL to differ,
   * e.g. http://gateway:port/hive2/servlets/thrifths2/
   */
  @Override
  public void run() {
    try {
      // Server thread pool
      // Start with minWorkerThreads, expand till maxWorkerThreads and reject subsequent requests
      String threadPoolName = "HiveServer2-HttpHandler-Pool";
      ExecutorService executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
          workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new ThreadFactoryWithGarbageCleanup(threadPoolName));
      ExecutorThreadPool threadPool = new ExecutorThreadPool(executorService);

      // HTTP Server
      httpServer = new org.eclipse.jetty.server.Server(threadPool);

      // Connector configs
      ServerConnector connector = new ServerConnector(httpServer);
      boolean useSsl = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL);
      String schemeName = useSsl ? "https" : "http";
      // Change connector if SSL is used
      if (useSsl) {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname
              + " Not configured for SSL connection");
        }
        SslContextFactory sslContextFactory = new SslContextFactory();
        String[] excludedProtocols = hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",");
        LOG.info("HTTP Server SSL: adding excluded protocols: " + Arrays.toString(excludedProtocols));
        sslContextFactory.addExcludeProtocols(excludedProtocols);
        LOG.info("HTTP Server SSL: SslContextFactory.getExcludeProtocols = " +
          Arrays.toString(sslContextFactory.getExcludeProtocols()));
        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        connector = new ServerConnector(httpServer, sslContextFactory);
      }
      connector.setPort(portNum);
      // Linux:yes, Windows:no
      connector.setReuseAddress(!Shell.WINDOWS);
      int maxIdleTime = (int) hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME,
          TimeUnit.MILLISECONDS);
      connector.setIdleTimeout(maxIdleTime);

      httpServer.addConnector(connector);

      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf);
      TProcessor processor = new TCLIService.Processor<Iface>(this);
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      // Set during the init phase of HiveServer2 if auth mode is kerberos
      // UGI for the hive/_HOST (kerberos) principal
      UserGroupInformation serviceUGI = cliService.getServiceUGI();
      // UGI for the http/_HOST (SPNego) principal
      UserGroupInformation httpUGI = cliService.getHttpUGI();
      String authType = hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);
      TServlet thriftHttpServlet = new ThriftHttpServlet(processor, protocolFactory, authType,
          serviceUGI, httpUGI);

      // Context handler
      final ServletContextHandler context = new ServletContextHandler(
          ServletContextHandler.SESSIONS);
      context.setContextPath("/");
      String httpPath = getHttpPath(hiveConf
          .getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));
      httpServer.setHandler(context);
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);

      // TODO: check defaults: maxTimeout, keepalive, maxBodySize, bodyRecieveDuration, etc.
      // Finally, start the server
      httpServer.start();
      String msg = "Started " + ThriftHttpCLIService.class.getSimpleName() + " in " + schemeName
          + " mode on port " + portNum + " path=" + httpPath + " with " + minWorkerThreads + "..."
          + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      httpServer.join();
    } catch (Throwable t) {
      LOG.fatal(
          "Error starting HiveServer2: could not start "
              + ThriftHttpCLIService.class.getSimpleName(), t);
      System.exit(-1);
    }
  }

  /**
   * The config parameter can be like "path", "/path", "/path/", "path/*", "/path1/path2/*" and so on.
   * httpPath should end up as "/*", "/path/*" or "/path1/../pathN/*"
   * @param httpPath
   * @return
   */
  private String getHttpPath(String httpPath) {
    if(httpPath == null || httpPath.equals("")) {
      httpPath = "/*";
    }
    else {
      if(!httpPath.startsWith("/")) {
        httpPath = "/" + httpPath;
      }
      if(httpPath.endsWith("/")) {
        httpPath = httpPath + "*";
      }
      if(!httpPath.endsWith("/*")) {
        httpPath = httpPath + "/*";
      }
    }
    return httpPath;
  }
}
