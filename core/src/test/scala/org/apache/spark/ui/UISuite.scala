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
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
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
      .set("spark.ui.enabled", "true")
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }

  private def sslDisabledConf(): (SparkConf, SSLOptions) = {
    val conf = new SparkConf
    (conf, new SecurityManager(conf).getSSLOptions("ui"))
  }

  private def sslEnabledConf(sslPort: Option[Int] = None): (SparkConf, SSLOptions) = {
    val keyStoreFilePath = getTestResourcePath("spark.keystore")
    val conf = new SparkConf()
      .set("spark.ssl.ui.enabled", "true")
      .set("spark.ssl.ui.keyStore", keyStoreFilePath)
      .set("spark.ssl.ui.keyStorePassword", "123456")
      .set("spark.ssl.ui.keyPassword", "123456")
    sslPort.foreach { p =>
      conf.set("spark.ssl.ui.port", p.toString)
    }
    (conf, new SecurityManager(conf).getSSLOptions("ui"))
  }

  ignore("basic ui visibility") {
    withSpark(newSparkContext()) { sc =>
      // test if the ui is visible, and all the expected tabs are visible
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
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
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL("http://localhost:4040").mkString
        assert(html.toLowerCase(Locale.ROOT).contains("stages"))
      }
    }
  }

  test("jetty selects different port under contention") {
    var server: ServerSocket = null
    var serverInfo1: ServerInfo = null
    var serverInfo2: ServerInfo = null
    val (conf, sslOptions) = sslDisabledConf()
    try {
      server = new ServerSocket(0)
      val startPort = server.getLocalPort
      serverInfo1 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, sslOptions, Seq[ServletContextHandler](), conf)
      serverInfo2 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, sslOptions, Seq[ServletContextHandler](), conf)
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
      val (conf, sslOptions) = sslEnabledConf()
      serverInfo1 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, sslOptions, Seq[ServletContextHandler](), conf, "server1")
      serverInfo2 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, sslOptions, Seq[ServletContextHandler](), conf, "server2")
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
    val (conf, sslOptions) = sslDisabledConf()
    try {
      serverInfo = JettyUtils.startJettyServer(
        "0.0.0.0", 0, sslOptions, Seq[ServletContextHandler](), conf)
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
      val (conf, sslOptions) = sslEnabledConf()
      serverInfo = JettyUtils.startJettyServer(
        "0.0.0.0", 0, sslOptions, Seq[ServletContextHandler](), conf)
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
    val prefix = "/proxy/worker-id"
    val target = "http://localhost:8081"
    val path = "/proxy/worker-id/json"
    var rewrittenURI = JettyUtils.createProxyURI(prefix, target, path, null)
    assert(rewrittenURI.toString() === "http://localhost:8081/json")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, path, "test=done")
    assert(rewrittenURI.toString() === "http://localhost:8081/json?test=done")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/proxy/worker-id", null)
    assert(rewrittenURI.toString() === "http://localhost:8081")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/proxy/worker-id/test%2F", null)
    assert(rewrittenURI.toString() === "http://localhost:8081/test%2F")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/proxy/worker-id/%F0%9F%98%84", null)
    assert(rewrittenURI.toString() === "http://localhost:8081/%F0%9F%98%84")
    rewrittenURI = JettyUtils.createProxyURI(prefix, target, "/proxy/worker-noid/json", null)
    assert(rewrittenURI === null)
  }

  test("verify rewriting location header for reverse proxy") {
    val clientRequest = mock(classOf[HttpServletRequest])
    var headerValue = "http://localhost:4040/jobs"
    val prefix = "/proxy/worker-id"
    val targetUri = URI.create("http://localhost:4040")
    when(clientRequest.getScheme()).thenReturn("http")
    when(clientRequest.getHeader("host")).thenReturn("localhost:8080")
    var newHeader = JettyUtils.createProxyLocationHeader(
      prefix, headerValue, clientRequest, targetUri)
    assert(newHeader.toString() === "http://localhost:8080/proxy/worker-id/jobs")
    headerValue = "http://localhost:4041/jobs"
    newHeader = JettyUtils.createProxyLocationHeader(
      prefix, headerValue, clientRequest, targetUri)
    assert(newHeader === null)
  }

  test("http -> https redirect applies to all URIs") {
    var serverInfo: ServerInfo = null
    try {
      val servlet = new HttpServlet() {
        override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
          res.sendError(HttpServletResponse.SC_OK)
        }
      }

      def newContext(path: String): ServletContextHandler = {
        val ctx = new ServletContextHandler()
        ctx.setContextPath(path)
        ctx.addServlet(new ServletHolder(servlet), "/root")
        ctx
      }

      val (conf, sslOptions) = sslEnabledConf()
      serverInfo = JettyUtils.startJettyServer("0.0.0.0", 0, sslOptions,
        Seq[ServletContextHandler](newContext("/"), newContext("/test1")),
        conf)
      assert(serverInfo.server.getState === "STARTED")

      val testContext = newContext("/test2")
      serverInfo.addHandler(testContext)
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
      val (conf, sslOptions) = sslEnabledConf(sslPort = Some(baseSslPort))

      serverInfo = JettyUtils.startJettyServer("0.0.0.0", socket.getLocalPort() + 1,
        sslOptions, Seq[ServletContextHandler](), conf, "server1")

      val notAllowed = Utils.userPort(serverInfo.boundPort, 400)
      assert(serverInfo.securePort.isDefined)
      assert(serverInfo.securePort.get != Utils.userPort(serverInfo.boundPort, 400))
    } finally {
      stopServer(serverInfo)
      closeSocket(socket)
    }
  }

  def stopServer(info: ServerInfo): Unit = {
    if (info != null) info.stop()
  }

  def closeSocket(socket: ServerSocket): Unit = {
    if (socket != null) socket.close
  }
}
