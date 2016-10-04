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

import java.io.File
import java.net.{BindException, ServerSocket}
import java.net.URI
import javax.servlet.http.HttpServletRequest

import scala.io.Source

import org.eclipse.jetty.servlet.ServletContextHandler
import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._

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

  private def sslEnabledConf(): (SparkConf, SSLOptions) = {
    val keyStoreFile = getClass.getClassLoader.getResource("spark.keystore").getFile
    val keyStoreFilePath = new File(keyStoreFile).getCanonicalPath
    val conf = new SparkConf()
      .set("spark.ssl.ui.enabled", "true")
      .set("spark.ssl.ui.keyStore", keyStoreFilePath)
      .set("spark.ssl.ui.keyStorePassword", "123456")
      .set("spark.ssl.ui.keyPassword", "123456")
    (conf, new SecurityManager(conf).getSSLOptions("ui"))
  }

  ignore("basic ui visibility") {
    withSpark(newSparkContext()) { sc =>
      // test if the ui is visible, and all the expected tabs are visible
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL(sc.ui.get.appUIAddress).mkString
        assert(!html.contains("random data that should not be present"))
        assert(html.toLowerCase.contains("stages"))
        assert(html.toLowerCase.contains("storage"))
        assert(html.toLowerCase.contains("environment"))
        assert(html.toLowerCase.contains("executors"))
      }
    }
  }

  ignore("visibility at localhost:4040") {
    withSpark(newSparkContext()) { sc =>
      // test if visible from http://localhost:4040
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL("http://localhost:4040").mkString
        assert(html.toLowerCase.contains("stages"))
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
      intercept[BindException] {
        socket = new ServerSocket(boundPort)
      }
    } finally {
      stopServer(serverInfo)
      closeSocket(socket)
    }
  }

  test("verify appUIAddress contains the scheme") {
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val uiAddress = ui.appUIAddress
      val uiHostPort = ui.appUIHostPort
      assert(uiAddress.equals("http://" + uiHostPort))
    }
  }

  test("verify appUIAddress contains the port") {
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val splitUIAddress = ui.appUIAddress.split(':')
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

  def stopServer(info: ServerInfo): Unit = {
    if (info != null && info.server != null) info.server.stop
  }

  def closeSocket(socket: ServerSocket): Unit = {
    if (socket != null) socket.close
  }
}
