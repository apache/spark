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

package org.apache.spark.deploy.history

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.openqa.selenium.WebDriver
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark._
import org.apache.spark.internal.config.{EVENT_LOG_STAGE_EXECUTOR_METRICS, EXECUTOR_PROCESS_TREE_METRICS_ENABLED}
import org.apache.spark.internal.config.History.{HISTORY_LOG_DIR, LOCAL_STORE_DIR, UPDATE_INTERVAL_S}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.util.{ResetSystemProperties, Utils}

/**
 * Tests for HistoryServer with real web browsers.
 */
abstract class RealBrowserUIHistoryServerSuite(val driverProp: String)
  extends SparkFunSuite with WebBrowser with Matchers with ResetSystemProperties {

  implicit var webDriver: WebDriver

  private val driverPropPrefix = "spark.test."
  private val logDir = getTestResourcePath("spark-events")
  private val storeDir = Utils.createTempDir(namePrefix = "history")

  private var provider: FsHistoryProvider = null
  private var server: HistoryServer = null
  private var port: Int = -1

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(
      sys.props(driverPropPrefix + driverProp) !== null,
      "System property " + driverPropPrefix + driverProp +
        " should be set to the corresponding driver path.")
    sys.props(driverProp) = sys.props(driverPropPrefix + driverProp)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (server == null) {
      init()
    }
  }

  override def afterAll(): Unit = {
    sys.props.remove(driverProp)
    super.afterAll()
  }

  def init(extraConf: (String, String)*): Unit = {
    Utils.deleteRecursively(storeDir)
    assert(storeDir.mkdir())
    val conf = new SparkConf()
      .set(HISTORY_LOG_DIR, logDir)
      .set(UPDATE_INTERVAL_S.key, "0")
      .set(IS_TESTING, true)
      .set(LOCAL_STORE_DIR, storeDir.getAbsolutePath())
      .set(EVENT_LOG_STAGE_EXECUTOR_METRICS, true)
      .set(EXECUTOR_PROCESS_TREE_METRICS_ENABLED, true)
    conf.setAll(extraConf)
    provider = new FsHistoryProvider(conf)
    provider.checkForLogs()
    val securityManager = HistoryServer.createSecurityManager(conf)

    server = new HistoryServer(conf, provider, securityManager, 18080)
    server.bind()
    provider.start()
    port = server.boundPort
  }

  def stop(): Unit = {
    server.stop()
    server = null
  }

  test("ajax rendered relative links are prefixed with uiRoot (spark.ui.proxyBase)") {
    val uiRoot = "/testwebproxybase"
    System.setProperty("spark.ui.proxyBase", uiRoot)

    stop()
    init()

    val port = server.boundPort

    val servlet = new ProxyServlet {
      override def rewriteTarget(request: HttpServletRequest): String = {
        // servlet acts like a proxy that redirects calls made on
        // spark.ui.proxyBase context path to the normal servlet handlers operating off "/"
        val sb = request.getRequestURL()

        if (request.getQueryString() != null) {
          sb.append(s"?${request.getQueryString()}")
        }

        val proxyidx = sb.indexOf(uiRoot)
        sb.delete(proxyidx, proxyidx + uiRoot.length).toString
      }
    }

    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(uiRoot)
    contextHandler.addServlet(holder, "/")
    server.attachHandler(contextHandler)

    try {
      val url = s"http://localhost:$port"

      go to s"$url$uiRoot"

      // expect the ajax call to finish in 5 seconds
      implicitlyWait(org.scalatest.time.Span(5, org.scalatest.time.Seconds))

      // once this findAll call returns, we know the ajax load of the table completed
      findAll(ClassNameQuery("odd"))

      val links = findAll(TagNameQuery("a"))
        .map(_.attribute("href"))
        .filter(_.isDefined)
        .map(_.get)
        .filter(_.startsWith(url)).toList

      // there are at least some URL links that were generated via javascript,
      // and they all contain the spark.ui.proxyBase (uiRoot)
      links.length should be > 4
      for (link <- links) {
        link should startWith(url + uiRoot)
      }
    } finally {
      contextHandler.stop()
      quit()
    }
  }
}
