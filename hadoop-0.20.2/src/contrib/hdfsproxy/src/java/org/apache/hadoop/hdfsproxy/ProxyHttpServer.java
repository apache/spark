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

package org.apache.hadoop.hdfsproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;

/**
 * Create a Jetty embedded server to answer http/https requests.
 */
public class ProxyHttpServer extends HttpServer {
  public static final Log LOG = LogFactory.getLog(ProxyHttpServer.class);

  public ProxyHttpServer(InetSocketAddress addr, Configuration conf)
      throws IOException {
    super("", addr.getHostName(), addr.getPort(), 0 <= addr.getPort(), conf);
  }

  /** {@inheritDoc} */
  public Connector createBaseListener(Configuration conf)
      throws IOException {
    final String sAddr;
    if (null == (sAddr = conf.get("proxy.http.test.listener.addr"))) {
      SslSocketConnector sslListener = new SslSocketConnector();
      sslListener.setKeystore(conf.get("ssl.server.keystore.location"));
      sslListener.setPassword(conf.get("ssl.server.keystore.password", ""));
      sslListener.setKeyPassword(conf.get("ssl.server.keystore.keypassword", ""));
      sslListener.setKeystoreType(conf.get("ssl.server.keystore.type", "jks"));
      sslListener.setNeedClientAuth(true);
      System.setProperty("javax.net.ssl.trustStore",
          conf.get("ssl.server.truststore.location", ""));
      System.setProperty("javax.net.ssl.trustStorePassword",
          conf.get("ssl.server.truststore.password", ""));
      System.setProperty("javax.net.ssl.trustStoreType",
          conf.get("ssl.server.truststore.type", "jks"));
      return sslListener;
    }
    // unit test
    InetSocketAddress proxyAddr = NetUtils.createSocketAddr(sAddr);
    SelectChannelConnector testlistener = new SelectChannelConnector();
    testlistener.setUseDirectBuffers(false);
    testlistener.setHost(proxyAddr.getHostName());
    testlistener.setPort(proxyAddr.getPort());
    return testlistener;
  }

}
