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

import org.apache.hadoop.hdfs.server.namenode.JspHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A HTTPS/SSL proxy to HDFS, implementing certificate based access control.
 */
public class HdfsProxy {
  public static final Log LOG = LogFactory.getLog(HdfsProxy.class);

  private ProxyHttpServer server;
  private InetSocketAddress sslAddr;

  /** Construct a proxy from the given configuration */
  public HdfsProxy(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (IOException e) {
      this.stop();
      throw e;
    }
  }

  private void initialize(Configuration conf) throws IOException {
    sslAddr = getSslAddr(conf);
    String nn = conf.get("hdfsproxy.dfs.namenode.address");
    if (nn == null)
      throw new IOException("HDFS NameNode address is not specified");
    InetSocketAddress nnAddr = NetUtils.createSocketAddr(nn);
    LOG.info("HDFS NameNode is at: " + nnAddr.getHostName() + ":" + nnAddr.getPort());

    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get("hdfsproxy.https.server.keystore.resource",
        "ssl-server.xml"));
    // unit testing
    sslConf.set("proxy.http.test.listener.addr",
                conf.get("proxy.http.test.listener.addr"));

    this.server = new ProxyHttpServer(sslAddr, sslConf);
    this.server.setAttribute("proxy.https.port", server.getPort());
    this.server.setAttribute("name.node.address", nnAddr);
    this.server.setAttribute(JspHelper.CURRENT_CONF, new Configuration());
    this.server.addGlobalFilter("ProxyFilter", ProxyFilter.class.getName(), null);
    this.server.addServlet("listPaths", "/listPaths/*", ProxyListPathsServlet.class);
    this.server.addServlet("data", "/data/*", ProxyFileDataServlet.class);
    this.server.addServlet("streamFile", "/streamFile/*", ProxyStreamFile.class);
  }

  /** return the http port if any, only for testing purposes */
  int getPort() throws IOException {
    return server.getPort();
  }

  /**
   * Start the server.
   */
  public void start() throws IOException {
    this.server.start();
    LOG.info("HdfsProxy server up at: " + sslAddr.getHostName() + ":"
        + sslAddr.getPort());
  }

  /**
   * Stop all server threads and wait for all to finish.
   */
  public void stop() {
    try {
      if (server != null) {
        server.stop();
        server.join();
      }
    } catch (Exception e) {
      LOG.warn("Got exception shutting down proxy", e);
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      this.server.join();
    } catch (InterruptedException ie) {
    }
  }

  static InetSocketAddress getSslAddr(Configuration conf) throws IOException {
    String addr = conf.get("hdfsproxy.https.address");
    if (addr == null)
      throw new IOException("HdfsProxy address is not specified");
    return NetUtils.createSocketAddr(addr);
  }


  public static HdfsProxy createHdfsProxy(String argv[], Configuration conf)
      throws IOException {
    if (argv.length > 0) {
      System.err.println("Usage: HdfsProxy");
      return null;
    }
    if (conf == null) {
      conf = new Configuration(false);
      conf.addResource("hdfsproxy-default.xml");
    }

    StringUtils.startupShutdownMessage(HdfsProxy.class, argv, LOG);
    HdfsProxy proxy = new HdfsProxy(conf);
    proxy.start();
    return proxy;
  }

  public static void main(String[] argv) throws Exception {
    try {
      HdfsProxy proxy = createHdfsProxy(argv, null);
      if (proxy != null)
        proxy.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
