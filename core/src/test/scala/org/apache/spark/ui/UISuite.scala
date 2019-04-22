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

package org.apache.spark.ui

import java.net.{BindException, ServerSocket}
import java.net.{URI, URL}
import java.util.Locale
import javax.servlet._
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.util.Utils

class UISuite extends SparkFunSuite {

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
   */
  private def newSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(UI_ENABLED, true)
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }

  private def sslDisabledConf(): (SparkConf, SecurityManager, SSLOptions) = {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    (conf, securityMgr, securityMgr.getSSLOptions("ui"))
  }

  private def sslEnabledConf(sslPort: Option[Int] = None):
      (SparkConf, SecurityManager, SSLOptions) = {
    val keyStoreFilePath = getTestResourcePath("spark.keystore")
    val conf = new SparkConf()
      .set("spark.ssl.ui.enabled", "true")
      .set("spark.ssl.ui.keyStore", keyStoreFilePath)
      .set("spark.ssl.ui.keyStorePassword", "123456")
      .set("spark.ssl.ui.keyPassword", "123456")
    sslPort.foreach { p =>
      conf.set("spark.ssl.ui.port", p.toString)
    }
    val securityMgr = new SecurityManager(conf)
    (conf, securityMgr, securityMgr.getSSLOptions("ui"))
  }

  ignore("basic ui visibility") {
    withSpark(newSparkContext()) { sc =>
      // test if the ui is visible, and all the expected tabs are visible
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        val html = Source.fromURL(sc.ui.get.webUrl).mkString
        assert(!html.contains("random data that should not be present"))
        assert(html.toLowerCase(Locale.ROOT).contains("stages"))
        assert(html.toLowerCase(Locale.ROOT).contains("storage"))
        assert(html.toLowerCase(Locale.ROOT).contains("environment"))
        assert(html.toLowerCase(Locale.ROOT).contains("executors"))
      }
    }
  }

  ignore("visibility at localhost:4040") {
    withSpark(newSparkContext()) { sc =>
      // test if visible from http://localhost:4040
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        val html = Source.fromURL("http://localhost:4040").mkString
        assert(html.toLowerCase(Locale.ROOT).contains("stages"))
      }
    }
  }

  test("jetty selects different port under contention") {
    var server: ServerSocket = null
    var serverInfo1: ServerInfo = null
    var serverInfo2: ServerInfo = null
    val (conf, _, sslOptions) = sslDisabledConf()
    try {
      server = new ServerSocket(0)
      val startPort = server.getLocalPort
      serverInfo1 = JettyUtils.startJettyServer("0.0.0.0", startPort, sslOptions, conf)
      serverInfo2 = JettyUtils.startJettyServer("0.0.0.0", startPort, sslOptions, conf)
      // Allow some wiggle room in case ports on the machine are under contention
      val boundPort1 = serverInfo1.boundPort
      val boundPort2 = serverInfo2.boundPort
      assert(boundPort1 != startPort)
      assert(boundPort2 != startPort)
      assert(boundPort1 != boundPort2)
    } finally {
      stopServer(serverInfo1)
      stopServer(serverInfo2)
      closeSocket(server)
    }
  }

  test("jetty with https selects different port under contention") {
    var server: ServerSocket = null
    var serverInfo1: ServerInfo = null
    var serverInfo2: ServerInfo = null
    try {
      server = new ServerSocket(0)
      val startPort = server.getLocalPort
      val (conf, _, sslOptions) = sslEnabledConf()
      serverInfo1 = JettyUtils.startJettyServer("0.0.0.0", startPort, sslOptions, conf, "server1")
      serverInfo2 = JettyUtils.startJettyServer("0.0.0.0", startPort, sslOptions, conf, "server2")
      // Allow some wiggle room in case ports on the machine are under contention
      val boundPort1 = serverInfo1.boundPort
      val boundPort2 = serverInfo2.boundPort
      assert(boundPort1 != startPort)
      assert(boundPort2 != startPort)
      assert(boundPort1 != boundPort2)
    } finally {
      stopServer(serverInfo1)
      stopServer(serverInfo2)
      closeSocket(server)
    }
  }

  test("jetty binds to port 0 correctly") {
    var socket: ServerSocket = null
    var serverInfo: ServerInfo = null
    val (conf, _, sslOptions) = sslDisabledConf()
    try {
      serverInfo = JettyUtils.startJettyServer("0.0.0.0", 0, sslOptions, conf)
      val server = serverInfo.server
      val boundPort = serverInfo.boundPort
      assert(server.getState === "STARTED")
      assert(boundPort != 0)
      intercept[BindException] {
        socket = new ServerSocket(boundPort)
      }
    } finally {
      stopServer(serverInfo)
      closeSocket(socket)
    }
  }

  test("jetty with https binds to port 0 correctly") {
    var socket: ServerSocket = null
    var serverInfo: ServerInfo = null
    try {
      val (conf, _, sslOptions) = sslEnabledConf()
      serverInfo = JettyUtils.startJettyServer("0.0.0.0", 0, sslOptions, conf)
      val server = serverInfo.server
      val boundPort = serverInfo.boundPort
      assert(server.getState === "STARTED")
      assert(boundPort != 0)
      assert(serverInfo.securePort.isDefined)
      intercept[BindException] {
        socket = new ServerSocket(boundPort)
      }
    } finally {
      stopServer(serverInfo)
      closeSocket(socket)
    }
  }

  test("verify webUrl contains the scheme") {
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val uiAddress = ui.webUrl
      assert(uiAddress.startsWith("http://") || uiAddress.startsWith("https://"))
    }
  }

  test("verify webUrl contains the port") {
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val splitUIAddress = ui.webUrl.split(':')
      val boundPort = ui.boundPort
      assert(splitUIAddress(2).toInt == boundPort)
    }
  }

  test("verify proxy rewrittenURI") {
    val prefix = "/worker-id"
    val target = "http://localhost:8081"
    val path = "/worker-id/json"
    var rewrittenURI = JettyUtils.createProxyURI(prefix, target, path, null)
    assert(rewrittenURI.toString() === "http://localhost:8081/json")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, path, "test=done")
    assert(rewrittenURI.toString() === "http://localhost:8081/json?test=done")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/worker-id", null)
    assert(rewrittenURI.toString() === "http://localhost:8081")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/worker-id/test%2F", null)
    assert(rewrittenURI.toString() === "http://localhost:8081/test%2F")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/worker-id/%F0%9F%98%84", null)
    assert(rewrittenURI.toString() === "http://localhost:8081/%F0%9F%98%84")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/worker-noid/json", null)
    assert(rewrittenURI === null)
  }

  test("verify rewriting location header for reverse proxy") {
    val clientRequest = mock(classOf[HttpServletRequest])
    var headerValue = "http://localhost:4040/jobs"
    val targetUri = URI.create("http://localhost:4040")
    when(clientRequest.getScheme()).thenReturn("http")
    when(clientRequest.getHeader("host")).thenReturn("localhost:8080")
    when(clientRequest.getPathInfo()).thenReturn("/proxy/worker-id/jobs")
    var newHeader = JettyUtils.createProxyLocationHeader(headerValue, clientRequest, targetUri)
    assert(newHeader.toString() === "http://localhost:8080/proxy/worker-id/jobs")
    headerValue = "http://localhost:4041/jobs"
    newHeader = JettyUtils.createProxyLocationHeader(headerValue, clientRequest, targetUri)
    assert(newHeader === null)
  }

  test("add and remove handlers with custom user filter") {
    val (conf, securityMgr, sslOptions) = sslDisabledConf()
    conf.set("spark.ui.filters", classOf[TestFilter].getName())
    conf.set(s"spark.${classOf[TestFilter].getName()}.param.responseCode",
      HttpServletResponse.SC_NOT_ACCEPTABLE.toString)

    val serverInfo = JettyUtils.startJettyServer("0.0.0.0", 0, sslOptions, conf)
    try {
      val path = "/test"
      val url = new URL(s"http://localhost:${serverInfo.boundPort}$path/root")

      assert(TestUtils.httpResponseCode(url) === HttpServletResponse.SC_NOT_FOUND)

      val (servlet, ctx) = newContext(path)
      serverInfo.addHandler(ctx, securityMgr)
      assert(TestUtils.httpResponseCode(url) === HttpServletResponse.SC_NOT_ACCEPTABLE)

      // Try a request with bad content in a parameter to make sure the security filter
      // is being added to new handlers.
      val badRequest = new URL(
        s"http://localhost:${serverInfo.boundPort}$path/root?bypass&invalid<=foo")
      assert(TestUtils.httpResponseCode(badRequest) === HttpServletResponse.SC_OK)
      assert(servlet.lastRequest.getParameter("invalid<") === null)
      assert(servlet.lastRequest.getParameter("invalid&lt;") !== null)

      serverInfo.removeHandler(ctx)
      assert(TestUtils.httpResponseCode(url) === HttpServletResponse.SC_NOT_FOUND)
    } finally {
      stopServer(serverInfo)
    }
  }

  test("http -> https redirect applies to all URIs") {
    val (conf, securityMgr, sslOptions) = sslEnabledConf()
    val serverInfo = JettyUtils.startJettyServer("0.0.0.0", 0, sslOptions, conf)
    try {
      Seq(newContext("/"), newContext("/test1")).foreach { case (_, ctx) =>
        serverInfo.addHandler(ctx, securityMgr)
      }
      assert(serverInfo.server.getState === "STARTED")

      val (_, testContext) = newContext("/test2")
      serverInfo.addHandler(testContext, securityMgr)
      testContext.start()

      val httpPort = serverInfo.boundPort

      val tests = Seq(
        ("http", serverInfo.boundPort, HttpServletResponse.SC_FOUND),
        ("https", serverInfo.securePort.get, HttpServletResponse.SC_OK))

      tests.foreach { case (scheme, port, expected) =>
        val urls = Seq(
          s"$scheme://localhost:$port/root",
          s"$scheme://localhost:$port/test1/root",
          s"$scheme://localhost:$port/test2/root")
        urls.foreach { url =>
          val rc = TestUtils.httpResponseCode(new URL(url))
          assert(rc === expected, s"Unexpected status $rc for $url")
        }
      }
    } finally {
      stopServer(serverInfo)
    }
  }

  test("specify both http and https ports separately") {
    var socket: ServerSocket = null
    var serverInfo: ServerInfo = null
    try {
      socket = new ServerSocket(0)

      // Make sure the SSL port lies way outside the "http + 400" range used as the default.
      val baseSslPort = Utils.userPort(socket.getLocalPort(), 10000)
      val (conf, _, sslOptions) = sslEnabledConf(sslPort = Some(baseSslPort))

      serverInfo = JettyUtils.startJettyServer("0.0.0.0", socket.getLocalPort() + 1,
        sslOptions, conf, serverName = "server1")

      val notAllowed = Utils.userPort(serverInfo.boundPort, 400)
      assert(serverInfo.securePort.isDefined)
      assert(serverInfo.securePort.get != Utils.userPort(serverInfo.boundPort, 400))
    } finally {
      stopServer(serverInfo)
      closeSocket(socket)
    }
  }

  /**
   * Create a new context handler for the given path, with a single servlet that responds to
   * requests in `$path/root`.
   */
  private def newContext(path: String): (CapturingServlet, ServletContextHandler) = {
    val servlet = new CapturingServlet()
    val ctx = new ServletContextHandler()
    ctx.setContextPath(path)
    ctx.addServlet(new ServletHolder(servlet), "/root")
    (servlet, ctx)
  }

  def stopServer(info: ServerInfo): Unit = {
    if (info != null) info.stop()
  }

  def closeSocket(socket: ServerSocket): Unit = {
    if (socket != null) socket.close
  }

  /** Test servlet that exposes the last request object for GET calls. */
  private class CapturingServlet extends HttpServlet {

    @volatile var lastRequest: HttpServletRequest = _

    override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      lastRequest = req
      res.sendError(HttpServletResponse.SC_OK)
    }

  }

}

// Filter for testing; returns a configurable code for every request.
private[spark] class TestFilter extends Filter {

  private var rc: Int = HttpServletResponse.SC_OK

  override def destroy(): Unit = { }

  override def init(config: FilterConfig): Unit = {
    if (config.getInitParameter("responseCode") != null) {
      rc = config.getInitParameter("responseCode").toInt
    }
  }

  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    if (req.getParameter("bypass") == null) {
      res.asInstanceOf[HttpServletResponse].sendError(rc, "Test.")
    } else {
      chain.doFilter(req, res)
    }
  }

}
