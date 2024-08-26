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

package org.apache.spark.deploy.yarn

import java.io.{IOException, PrintWriter, StringWriter}
import java.net.HttpURLConnection
import java.util
import java.util.{Collections, Locale}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import jakarta.servlet.{FilterChain, FilterConfig, ServletContext, ServletException, ServletOutputStream, ServletRequest, ServletResponse}
import jakarta.servlet.http.{Cookie, HttpServlet, HttpServletRequest, HttpServletResponse}
import jakarta.ws.rs.core.MediaType
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite

// A port of org.apache.hadoop.yarn.server.webproxy.amfilter.TestAmFilter
class AmIpFilterSuite extends SparkFunSuite {

  private val proxyHost = "localhost"
  private val proxyUri = "http://bogus"

  class TestAmIpFilter extends AmIpFilter {
    override def getProxyAddresses: util.Set[String] = Set(proxyHost).asJava
  }

  class DummyFilterConfig (val map: util.Map[String, String]) extends FilterConfig {
    override def getFilterName: String = "dummy"

    override def getInitParameter(arg0: String): String = map.get(arg0)

    override def getInitParameterNames: util.Enumeration[String] =
      Collections.enumeration(map.keySet)

    override def getServletContext: ServletContext = null
  }

  test("filterNullCookies") {
    val request = mock(classOf[HttpServletRequest])

    when(request.getCookies).thenReturn(null)
    when(request.getRemoteAddr).thenReturn(proxyHost)

    val response = mock(classOf[HttpServletResponse])
    val invoked = new AtomicBoolean

    val chain = new FilterChain() {
      @throws[IOException]
      @throws[ServletException]
      override def doFilter(req: ServletRequest, resp: ServletResponse): Unit = {
        invoked.set(true)
      }
    }

    val params = new util.HashMap[String, String]
    params.put(AmIpFilter.PROXY_HOST, proxyHost)
    params.put(AmIpFilter.PROXY_URI_BASE, proxyUri)
    val conf = new DummyFilterConfig(params)
    val filter = new TestAmIpFilter
    filter.init(conf)
    filter.doFilter(request, response, chain)
    assert(invoked.get)
    filter.destroy()
  }

  test("testFindRedirectUrl") {
    class EchoServlet extends HttpServlet {
      @throws[IOException]
      @throws[ServletException]
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        response.setContentType(MediaType.TEXT_PLAIN + "; charset=utf-8")
        val out = response.getWriter
        request.getParameterNames.asScala.toSeq.sorted.foreach { key =>
          out.print(key)
          out.print(':')
          out.print(request.getParameter(key))
          out.print('\n')
        }
        out.close()
      }
    }

    def withHttpEchoServer(body: String => Unit): Unit = {
      val server = new Server(0)
      server.getThreadPool.asInstanceOf[QueuedThreadPool].setMaxThreads(20)
      val context = new ServletContextHandler
      context.setContextPath("/foo")
      server.setHandler(context)
      val servletPath = "/bar"
      context.addServlet(new ServletHolder(new EchoServlet), servletPath)
      server.getConnectors.head.asInstanceOf[ServerConnector].setHost("localhost")
      try {
        server.start()
        body(server.getURI.toString + servletPath)
      } finally {
        server.stop()
      }
    }

    // generate a valid URL
    withHttpEchoServer { rm1Url =>
      val rm1 = "rm1"
      val rm2 = "rm2"
      // invalid url
      val rm2Url = "host2:8088"

      val filter = new TestAmIpFilter
      // make sure findRedirectUrl() go to HA branch
      filter.proxyUriBases = Map(rm1 -> rm1Url, rm2 -> rm2Url).asJava
      filter.rmUrls = Array[String](rm1, rm2)

      assert(filter.findRedirectUrl === rm1Url)
    }
  }

  test("testProxyUpdate") {
    var params = new util.HashMap[String, String]
    params.put(AmIpFilter.PROXY_HOSTS, proxyHost)
    params.put(AmIpFilter.PROXY_URI_BASES, proxyUri)

    var conf = new DummyFilterConfig(params)
    val filter = new AmIpFilter
    val updateInterval = TimeUnit.SECONDS.toMillis(1)
    AmIpFilter.setUpdateInterval(updateInterval)
    filter.init(conf)

    // check that the configuration was applied
    assert(filter.getProxyAddresses.contains("127.0.0.1"))

    // change proxy configurations
    params = new util.HashMap[String, String]
    params.put(AmIpFilter.PROXY_HOSTS, "unknownhost")
    params.put(AmIpFilter.PROXY_URI_BASES, proxyUri)
    conf = new DummyFilterConfig(params)
    filter.init(conf)

    // configurations shouldn't be updated now
    assert(!filter.getProxyAddresses.isEmpty)
    // waiting for configuration update
    eventually(timeout(5.seconds), interval(500.millis)) {
      assertThrows[ServletException] {
        filter.getProxyAddresses.isEmpty
      }
    }
  }

  test("testFilter") {
    var doFilterRequest: String = null
    var servletWrapper: AmIpServletRequestWrapper = null

    val params = new util.HashMap[String, String]
    params.put(AmIpFilter.PROXY_HOST, proxyHost)
    params.put(AmIpFilter.PROXY_URI_BASE, proxyUri)
    val config = new DummyFilterConfig(params)

    // dummy filter
    val chain = new FilterChain() {
      @throws[IOException]
      @throws[ServletException]
      override def doFilter(req: ServletRequest, resp: ServletResponse): Unit = {
        doFilterRequest = req.getClass.getName
        req match {
          case wrapper: AmIpServletRequestWrapper => servletWrapper = wrapper
          case _ =>
        }
      }
    }
    val testFilter = new AmIpFilter
    testFilter.init(config)

    val response = new HttpServletResponseForTest

    // Test request should implements HttpServletRequest
    val failRequest = mock(classOf[ServletRequest])
    val throws = intercept[ServletException] {
      testFilter.doFilter(failRequest, response, chain)
    }
    assert(ProxyUtils.E_HTTP_HTTPS_ONLY === throws.getMessage)


    // request with HttpServletRequest
    val request = mock(classOf[HttpServletRequest])
    when(request.getRemoteAddr).thenReturn("nowhere")
    when(request.getRequestURI).thenReturn("/app/application_00_0")

    // address "redirect" is not in host list for non-proxy connection
    testFilter.doFilter(request, response, chain)
    assert(HttpURLConnection.HTTP_MOVED_TEMP === response.status)
    var redirect = response.getHeader(ProxyUtils.LOCATION)
    assert("http://bogus/app/application_00_0" === redirect)

    // address "redirect" is not in host list for proxy connection
    when(request.getRequestURI).thenReturn("/proxy/application_00_0")
    testFilter.doFilter(request, response, chain)
    assert(HttpURLConnection.HTTP_MOVED_TEMP === response.status)
    redirect = response.getHeader(ProxyUtils.LOCATION)
    assert("http://bogus/proxy/redirect/application_00_0" === redirect)

    // check for query parameters
    when(request.getRequestURI).thenReturn("/proxy/application_00_0")
    when(request.getQueryString).thenReturn("id=0")
    testFilter.doFilter(request, response, chain)
    assert(HttpURLConnection.HTTP_MOVED_TEMP === response.status)
    redirect = response.getHeader(ProxyUtils.LOCATION)
    assert("http://bogus/proxy/redirect/application_00_0?id=0" === redirect)

    // "127.0.0.1" contains in host list. Without cookie
    when(request.getRemoteAddr).thenReturn("127.0.0.1")
    testFilter.doFilter(request, response, chain)
    assert(doFilterRequest.contains("HttpServletRequest"))

    // cookie added
    val cookies = Array[Cookie](new Cookie(AmIpFilter.PROXY_USER_COOKIE_NAME, "user"))

    when(request.getCookies).thenReturn(cookies)
    testFilter.doFilter(request, response, chain)

    assert(doFilterRequest === classOf[AmIpServletRequestWrapper].getName)
    // request contains principal from cookie
    assert(servletWrapper.getUserPrincipal.getName === "user")
    assert(servletWrapper.getRemoteUser === "user")
    assert(!servletWrapper.isUserInRole(""))
  }

  private class HttpServletResponseForTest extends HttpServletResponse {
    private var redirectLocation = ""
    var status = 0
    private var contentType: String = _
    final private val headers = new util.HashMap[String, String](1)
    private var body: StringWriter = _

    def getRedirect: String = redirectLocation

    @throws[IOException]
    override def sendRedirect(location: String): Unit = redirectLocation = location

    override def setDateHeader(name: String, date: Long): Unit = {}

    override def addDateHeader(name: String, date: Long): Unit = {}

    override def addCookie(cookie: Cookie): Unit = {}

    override def containsHeader(name: String): Boolean = false

    override def encodeURL(url: String): String = null

    override def encodeRedirectURL(url: String): String = url

    override def encodeUrl(url: String): String = null

    override def encodeRedirectUrl(url: String): String = null

    @throws[IOException]
    override def sendError(sc: Int, msg: String): Unit = {}

    @throws[IOException]
    override def sendError(sc: Int): Unit = {}

    override def setStatus(status: Int): Unit = this.status = status

    override def setStatus(sc: Int, sm: String): Unit = {}

    override def getStatus: Int = 0

    override def setContentType(contentType: String): Unit = this.contentType = contentType

    override def setBufferSize(size: Int): Unit = {}

    override def getBufferSize: Int = 0

    @throws[IOException]
    override def flushBuffer(): Unit = {}

    override def resetBuffer(): Unit = {}

    override def isCommitted: Boolean = false

    override def reset(): Unit = {}

    override def setLocale(loc: Locale): Unit = {}

    override def getLocale: Locale = null

    override def setHeader(name: String, value: String): Unit = headers.put(name, value)

    override def addHeader(name: String, value: String): Unit = {}

    override def setIntHeader(name: String, value: Int): Unit = {}

    override def addIntHeader(name: String, value: Int): Unit = {}

    override def getHeader(name: String): String = headers.get(name)

    override def getHeaders(name: String): util.Collection[String] = null

    override def getHeaderNames: util.Collection[String] = null

    override def getCharacterEncoding: String = null

    override def getContentType: String = null

    @throws[IOException]
    override def getOutputStream: ServletOutputStream = null

    @throws[IOException]
    override def getWriter: PrintWriter = {
      body = new StringWriter
      new PrintWriter(body)
    }

    override def setCharacterEncoding(charset: String): Unit = {}

    override def setContentLength(len: Int): Unit = {}

    override def setContentLengthLong(len: Long): Unit = {}
  }

}
