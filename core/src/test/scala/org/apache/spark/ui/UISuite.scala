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

import scala.io.Source

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.LocalSparkContext._
import org.apache.spark.{SecurityManager, SparkConf, SparkContext}

class UISuite extends FunSuite {

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
    val conf = new SparkConf
    val securityManager = new SecurityManager(conf)
    try {
      server = new ServerSocket(0)
      val startPort = server.getLocalPort
      serverInfo1 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, securityManager, Seq[ServletContextHandler](), conf)
      serverInfo2 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, securityManager, Seq[ServletContextHandler](), conf)
      // Allow some wiggle room in case ports on the machine are under contention
      val boundPort1 = serverInfo1.boundPort
      val boundPort2 = serverInfo2.boundPort
      assert(boundPort1 != startPort)
      assert(boundPort2 != startPort)
      assert(boundPort1 != boundPort2)
    } finally {
      UISuite.stopServer(serverInfo1.server)
      UISuite.stopServer(serverInfo2.server)
      UISuite.closeSocket(server)
    }
  }

  test("jetty with https selects different port under contention") {
    var server: ServerSocket = null
    var serverInfo1: ServerInfo = null
    var serverInfo2: ServerInfo = null
    try {
      server = new ServerSocket(0)
      val startPort = server.getLocalPort

      val sparkConf = new SparkConf()
        .set("spark.ssl.ui.enabled", "true")
        .set("spark.ssl.ui.keyStore", "./src/test/resources/spark.keystore")
        .set("spark.ssl.ui.keyStorePassword", "123456")
        .set("spark.ssl.ui.keyPassword", "123456")
      val securityManager = new SecurityManager(sparkConf)
      serverInfo1 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, securityManager, Seq[ServletContextHandler](), sparkConf, "server1")
      serverInfo2 = JettyUtils.startJettyServer(
        "0.0.0.0", startPort, securityManager, Seq[ServletContextHandler](), sparkConf, "server2")
      // Allow some wiggle room in case ports on the machine are under contention
      val boundPort1 = serverInfo1.boundPort
      val boundPort2 = serverInfo2.boundPort
      assert(boundPort1 != startPort)
      assert(boundPort2 != startPort)
      assert(boundPort1 != boundPort2)
    } finally {
      UISuite.stopServer(serverInfo1.server)
      UISuite.stopServer(serverInfo2.server)
      UISuite.closeSocket(server)
    }
  }

  test("jetty binds to port 0 correctly") {
    var socket: ServerSocket = null
    var serverInfo: ServerInfo = null
    val conf = new SparkConf
    val securityManager = new SecurityManager(conf)
    try {
      serverInfo = JettyUtils.startJettyServer(
        "0.0.0.0", 0, securityManager, Seq[ServletContextHandler](), conf)
      val server = serverInfo.server
      val boundPort = serverInfo.boundPort
      assert(server.getState === "STARTED")
      assert(boundPort != 0)
      intercept[BindException] {
        socket = new ServerSocket(boundPort)
      }
    } finally {
      UISuite.stopServer(serverInfo.server)
      UISuite.closeSocket(socket)
    }
  }

  test("jetty with https binds to port 0 correctly") {
    var socket: ServerSocket = null
    var serverInfo: ServerInfo = null
    try {
      val sparkConf = new SparkConf()
        .set("spark.ssl.ui.enabled", "false")
        .set("spark.ssl.ui.keyStore", "./src/test/resources/spark.keystore")
        .set("spark.ssl.ui.keyStorePassword", "123456")
        .set("spark.ssl.ui.keyPassword", "123456")
      val securityManager = new SecurityManager(sparkConf)
      serverInfo = JettyUtils.startJettyServer(
        "0.0.0.0", 0, securityManager, Seq[ServletContextHandler](), sparkConf)
      val server = serverInfo.server
      val boundPort = serverInfo.boundPort
      assert(server.getState === "STARTED")
      assert(boundPort != 0)
      intercept[BindException] {
        socket = new ServerSocket(boundPort)
      }
    } finally {
      UISuite.stopServer(serverInfo.server)
      UISuite.closeSocket(socket)
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
}

object UISuite {
  def stopServer(server: Server): Unit = {
    if (server != null) server.stop
  }

  def closeSocket(socket: ServerSocket): Unit = {
    if (socket != null) socket.close
  }
}
