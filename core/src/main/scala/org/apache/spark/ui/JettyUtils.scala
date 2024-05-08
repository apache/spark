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

import java.net.{URI, URL, URLDecoder}
import java.util.EnumSet

import scala.language.implicitConversions
import scala.util.Try
import scala.xml.Node

import jakarta.servlet.DispatcherType
import jakarta.servlet.http._
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.Response
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP
import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler._
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet._
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{pretty, render}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.UI._
import org.apache.spark.util.Utils

/**
 * Utilities for launching a web server using Jetty's HTTP Server class
 */
private[spark] object JettyUtils extends Logging {

  val SPARK_CONNECTOR_NAME = "Spark"
  val REDIRECT_CONNECTOR_NAME = "HttpsRedirect"

  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.
  type Responder[T] = HttpServletRequest => T

  class ServletParams[T <: AnyRef](val responder: Responder[T],
    val contentType: String,
    val extractFn: T => String = (in: Any) => in.toString) {}

  // Conversions from various types of Responder's to appropriate servlet parameters
  implicit def jsonResponderToServlet(responder: Responder[JValue]): ServletParams[JValue] =
    new ServletParams(responder, "text/json", (in: JValue) => pretty(render(in)))

  implicit def htmlResponderToServlet(responder: Responder[Seq[Node]]): ServletParams[Seq[Node]] =
    new ServletParams(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

  implicit def textResponderToServlet(responder: Responder[String]): ServletParams[String] =
    new ServletParams(responder, "text/plain")

  private def createServlet[T <: AnyRef](
      servletParams: ServletParams[T],
      conf: SparkConf): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        try {
          response.setContentType("%s;charset=utf-8".format(servletParams.contentType))
          response.setStatus(HttpServletResponse.SC_OK)
          val result = servletParams.responder(request)
          response.getWriter.print(servletParams.extractFn(result))
        } catch {
          case e: IllegalArgumentException =>
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage)
          case e: Exception =>
            logWarning(log"GET ${MDC(LogKeys.URI, request.getRequestURI)} failed: " +
              log"${MDC(ERROR, e)}", e)
            throw e
        }
      }
      // SPARK-5983 ensure TRACE is not supported
      protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
  }

  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler[T <: AnyRef](
      path: String,
      servletParams: ServletParams[T],
      conf: SparkConf,
      basePath: String = ""): ServletContextHandler = {
    createServletHandler(path, createServlet(servletParams, conf), basePath)
  }

  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler(
      path: String,
      servlet: HttpServlet,
      basePath: String): ServletContextHandler = {
    val prefixedPath = if (basePath == "" && path == "/") {
      path
    } else {
      (basePath + path).stripSuffix("/")
    }
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(prefixedPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /** Create a handler that always redirects the user to the given path */
  def createRedirectHandler(
      srcPath: String,
      destPath: String,
      beforeRedirect: HttpServletRequest => Unit = x => (),
      basePath: String = "",
      httpMethods: Set[String] = Set("GET")): ServletContextHandler = {
    val prefixedDestPath = basePath + destPath
    val servlet = new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        if (httpMethods.contains("GET")) {
          doRequest(request, response)
        } else {
          response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
      }
      override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        if (httpMethods.contains("POST")) {
          doRequest(request, response)
        } else {
          response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
      }
      private def doRequest(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        beforeRedirect(request)
        // Make sure we don't end up with "//" in the middle
        val newUrl = new URL(new URL(request.getRequestURL.toString), prefixedDestPath).toString
        response.sendRedirect(newUrl)
      }
      // SPARK-5983 ensure TRACE is not supported
      protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
    createServletHandler(srcPath, servlet, basePath)
  }

  /** Create a handler for serving files from a static directory */
  def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
    val staticHandler = new DefaultServlet
    val holder = new ServletHolder(staticHandler)
    Option(Utils.getSparkClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("resourceBase", res.toString)
      case None =>
        throw new Exception("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /** Create a handler for proxying request to Workers and Application Drivers */
  def createProxyHandler(idToUiAddress: String => Option[String]): ServletContextHandler = {
    val servlet = new ProxyServlet {
      override def rewriteTarget(request: HttpServletRequest): String = {
        val path = request.getPathInfo
        if (path == null) return null

        val prefixTrailingSlashIndex = path.indexOf('/', 1)
        val prefix = if (prefixTrailingSlashIndex == -1) {
          path
        } else {
          path.substring(0, prefixTrailingSlashIndex)
        }
        val id = prefix.drop(1)

        // Query master state for id's corresponding UI address
        // If that address exists, try to turn it into a valid, target URI string
        // Otherwise, return null
        idToUiAddress(id)
          .map(createProxyURI(prefix, _, path, request.getQueryString))
          .filter(uri => uri != null && validateDestination(uri.getHost, uri.getPort))
          .map(_.toString)
          .orNull
      }

      override def newHttpClient(): HttpClient = {
        // SPARK-21176: Use the Jetty logic to calculate the number of selector threads (#CPUs/2),
        // but limit it to 8 max.
        val numSelectors = math.max(1, math.min(8, Runtime.getRuntime().availableProcessors() / 2))
        new HttpClient(new HttpClientTransportOverHTTP(numSelectors))
      }

      override def filterServerResponseHeader(
          clientRequest: HttpServletRequest,
          serverResponse: Response,
          headerName: String,
          headerValue: String): String = {
        if (headerName.equalsIgnoreCase("location")) {
          val newHeader = createProxyLocationHeader(headerValue, clientRequest,
            serverResponse.getRequest().getURI())
          if (newHeader != null) {
            return newHeader
          }
        }
        super.filterServerResponseHeader(
          clientRequest, serverResponse, headerName, headerValue)
      }
    }

    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath("/proxy")
    contextHandler.addServlet(holder, "/*")
    contextHandler
  }

  /**
   * Attempt to start a Jetty server bound to the supplied hostName:port using the given
   * context handlers.
   *
   * If the desired port number is contended, continues incrementing ports until a free port is
   * found. Return the jetty Server object, the chosen port, and a mutable collection of handlers.
   */
  def startJettyServer(
      hostName: String,
      port: Int,
      sslOptions: SSLOptions,
      conf: SparkConf,
      serverName: String = "",
      poolSize: Int = 200): ServerInfo = {

    val stopTimeout = conf.get(UI_JETTY_STOP_TIMEOUT)
    logInfo(s"Start Jetty $hostName:$port for $serverName")
    // Start the server first, with no connectors.
    val pool = new QueuedThreadPool(poolSize)
    if (serverName.nonEmpty) {
      pool.setName(serverName)
    }
    pool.setDaemon(true)

    val server = new Server(pool)

    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val collection = new ContextHandlerCollection
    conf.get(PROXY_REDIRECT_URI) match {
      case Some(proxyUri) =>
        val proxyHandler = new ProxyRedirectHandler(proxyUri)
        proxyHandler.setHandler(collection)
        server.setHandler(proxyHandler)

      case _ =>
        server.setHandler(collection)
    }

    // Executor used to create daemon threads for the Jetty connectors.
    val serverExecutor = new ScheduledExecutorScheduler(s"$serverName-JettyScheduler", true)

    try {
      server.setStopTimeout(stopTimeout)
      server.start()

      // As each acceptor and each selector will use one thread, the number of threads should at
      // least be the number of acceptors and selectors plus 1. (See SPARK-13776)
      var minThreads = 1

      def newConnector(
          connectionFactories: Array[ConnectionFactory],
          port: Int): (ServerConnector, Int) = {
        val connector = new ServerConnector(
          server,
          null,
          serverExecutor,
          null,
          -1,
          -1,
          connectionFactories: _*)
        connector.setPort(port)
        connector.setHost(hostName)
        connector.setReuseAddress(!Utils.isWindows)
         // spark-45248: set the idle timeout to prevent slow DoS
        connector.setIdleTimeout(8000)

        // Currently we only use "SelectChannelConnector"
        // Limit the max acceptor number to 8 so that we don't waste a lot of threads
        connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

        connector.start()
        // The number of selectors always equals to the number of acceptors
        minThreads += connector.getAcceptors * 2

        (connector, connector.getLocalPort())
      }
      val httpConfig = new HttpConfiguration()
      val requestHeaderSize = conf.get(UI_REQUEST_HEADER_SIZE).toInt
      logDebug(s"Using requestHeaderSize: $requestHeaderSize")
      httpConfig.setRequestHeaderSize(requestHeaderSize)

      // Hide information.
      logDebug("Using setSendServerVersion: false")
      httpConfig.setSendServerVersion(false)
      logDebug("Using setSendXPoweredBy: false")
      httpConfig.setSendXPoweredBy(false)

      // If SSL is configured, create the secure connector first.
      val securePort = sslOptions.createJettySslContextFactoryServer().map { factory =>

        // SPARK-45522: SniHostCheck defaulted to true since Jetty 10,
        // this will affect the standalone deployment.
        val src = new SecureRequestCustomizer()
        src.setSniHostCheck(false)
        httpConfig.addCustomizer(src)

        val securePort = sslOptions.port.getOrElse(if (port > 0) Utils.userPort(port, 400) else 0)
        val secureServerName = if (serverName.nonEmpty) s"$serverName (HTTPS)" else serverName

        val connectionFactories = AbstractConnectionFactory.getFactories(factory,
          new HttpConnectionFactory(httpConfig))

        def sslConnect(currentPort: Int): (ServerConnector, Int) = {
          newConnector(connectionFactories, currentPort)
        }

        val (connector, boundPort) = Utils.startServiceOnPort[ServerConnector](securePort,
          sslConnect, conf, secureServerName)
        connector.setName(SPARK_CONNECTOR_NAME)
        server.addConnector(connector)
        boundPort
      }

      // Bind the HTTP port.
      def httpConnect(currentPort: Int): (ServerConnector, Int) = {
        newConnector(Array(new HttpConnectionFactory(httpConfig)), currentPort)
      }

      val (httpConnector, httpPort) = Utils.startServiceOnPort[ServerConnector](port, httpConnect,
        conf, serverName)

      // If SSL is configured, then configure redirection in the HTTP connector.
      securePort match {
        case Some(p) =>
          httpConnector.setName(REDIRECT_CONNECTOR_NAME)
          val redirector = createRedirectHttpsHandler(p, "https")
          collection.addHandler(redirector)
          redirector.start()

        case None =>
          httpConnector.setName(SPARK_CONNECTOR_NAME)
      }

      server.addConnector(httpConnector)
      pool.setMaxThreads(math.max(pool.getMaxThreads, minThreads))
      ServerInfo(server, httpPort, securePort, conf, collection)
    } catch {
      case e: Exception =>
        server.stop()
        if (serverExecutor.isStarted()) {
          serverExecutor.stop()
        }
        if (pool.isStarted()) {
          pool.stop()
        }
        throw e
    }
  }

  private def createRedirectHttpsHandler(securePort: Int, scheme: String): ContextHandler = {
    val redirectHandler: ContextHandler = new ContextHandler
    redirectHandler.setContextPath("/")
    redirectHandler.setVirtualHosts(toVirtualHosts(REDIRECT_CONNECTOR_NAME))
    redirectHandler.setHandler(new AbstractHandler {
      override def handle(
          target: String,
          baseRequest: Request,
          request: HttpServletRequest,
          response: HttpServletResponse): Unit = {
        if (baseRequest.isSecure) {
          return
        }
        val httpsURI = createRedirectURI(scheme, securePort, baseRequest)
        response.setContentLength(0)
        response.sendRedirect(response.encodeRedirectURL(httpsURI))
        baseRequest.setHandled(true)
      }
    })
    redirectHandler
  }

  def createProxyURI(prefix: String, target: String, path: String, query: String): URI = {
    if (!path.startsWith(prefix)) {
      return null
    }

    val uri = new StringBuilder(target)
    val rest = path.substring(prefix.length())

    if (!rest.isEmpty()) {
      if (!rest.startsWith("/") && !uri.endsWith("/")) {
        uri.append("/")
      }
      uri.append(rest)
    }

    val queryString = if (query == null) {
      ""
    } else {
      s"?$query"
    }
    // SPARK-33611: use method `URI.create` to avoid percent-encoding twice on the query string.
    URI.create(uri.toString() + queryString).normalize()
  }

  def createProxyLocationHeader(
      headerValue: String,
      clientRequest: HttpServletRequest,
      targetUri: URI): String = {
    val toReplace = targetUri.getScheme() + "://" + targetUri.getAuthority()
    if (headerValue.startsWith(toReplace)) {
      val id = clientRequest.getPathInfo.substring("/proxy/".length).takeWhile(_ != '/')
      val headerPath = headerValue.substring(toReplace.length)

      s"${clientRequest.getScheme}://${clientRequest.getHeader("host")}/proxy/$id$headerPath"
    } else {
      null
    }
  }

  def addFilter(
      handler: ServletContextHandler,
      filter: String,
      params: Map[String, String]): Unit = {
    val holder = new FilterHolder()
    holder.setClassName(filter)
    params.foreach { case (k, v) => holder.setInitParameter(k, v) }
    handler.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
  }

  private def decodeURL(url: String, encoding: String): String = {
    if (url == null) {
      null
    } else {
      URLDecoder.decode(url, encoding)
    }
  }

  // Create a new URI from the arguments, handling IPv6 host encoding and default ports.
  private def createRedirectURI(scheme: String, port: Int, request: Request): String = {
    val server = request.getServerName
    val redirectServer = if (server.contains(":") && !server.startsWith("[")) {
      s"[${server}]"
    } else {
      server
    }
    val authority = s"$redirectServer:$port"
    val queryEncoding = if (request.getQueryEncoding != null) {
      request.getQueryEncoding
    } else {
      // By default decoding the URI as "UTF-8" should be enough for SparkUI
      "UTF-8"
    }
    // The request URL can be raw or encoded here. To avoid the request URL being
    // encoded twice, let's decode it here.
    val requestURI = decodeURL(request.getRequestURI, queryEncoding)
    val queryString = decodeURL(request.getQueryString, queryEncoding)
    new URI(scheme, authority, requestURI, queryString, null).toString
  }

  def toVirtualHosts(connectors: String*): Array[String] = connectors.map("@" + _).toArray

}

private[spark] case class ServerInfo(
    server: Server,
    boundPort: Int,
    securePort: Option[Int],
    private val conf: SparkConf,
    private val rootHandler: ContextHandlerCollection) extends Logging {

  def addHandler(
      handler: ServletContextHandler,
      securityMgr: SecurityManager): Unit = synchronized {
    handler.setVirtualHosts(JettyUtils.toVirtualHosts(JettyUtils.SPARK_CONNECTOR_NAME))
    addFilters(handler, securityMgr)

    val gzipHandler = new GzipHandler()
    gzipHandler.setHandler(handler)
    rootHandler.addHandler(gzipHandler)

    if (!handler.isStarted()) {
      handler.start()
    }
    gzipHandler.start()
  }

  def removeHandler(handler: ServletContextHandler): Unit = synchronized {
    // Since addHandler() always adds a wrapping gzip handler, find the container handler
    // and remove it.
    rootHandler.getHandlers()
      .find { h =>
        h.isInstanceOf[GzipHandler] && h.asInstanceOf[GzipHandler].getHandler() == handler
      }
      .foreach { h =>
        rootHandler.removeHandler(h)
        h.stop()
      }

    if (handler.isStarted) {
      handler.stop()
    }
  }

  def stop(): Unit = {
    val threadPool = server.getThreadPool
    threadPool match {
      case pool: QueuedThreadPool =>
        // Workaround for SPARK-30385 to avoid Jetty's acceptor thread shrink.
        // As of Jetty 9.4.21, the implementation of
        // QueuedThreadPool#setIdleTimeout is changed and IllegalStateException
        // will be thrown if we try to set idle timeout after the server has started.
        // But this workaround works for Jetty 9.4.28 by ignoring the exception.
        Try(pool.setIdleTimeout(0))
      case _ =>
    }
    server.stop()
    // Stop the ThreadPool if it supports stop() method (through LifeCycle).
    // It is needed because stopping the Server won't stop the ThreadPool it uses.
    if (threadPool != null && threadPool.isInstanceOf[LifeCycle]) {
      threadPool.asInstanceOf[LifeCycle].stop
    }
  }

  /**
   * Add filters, if any, to the given ServletContextHandlers. Always adds a filter at the end
   * of the chain to perform security-related functions.
   */
  private def addFilters(handler: ServletContextHandler, securityMgr: SecurityManager): Unit = {
    conf.get(UI_FILTERS).foreach { filter =>
      logInfo(s"Adding filter to ${handler.getContextPath()}: $filter")
      val oldParams = conf.getOption(s"spark.$filter.params").toSeq
        .flatMap(Utils.stringToSeq)
        .flatMap { param =>
          val parts = param.split("=")
          if (parts.length == 2) Some(parts(0) -> parts(1)) else None
        }
        .toMap

      val newParams = conf.getAllWithPrefix(s"spark.$filter.param.").toMap

      JettyUtils.addFilter(handler, filter, oldParams ++ newParams)
    }

    // This filter must come after user-installed filters, since that's where authentication
    // filters are installed. This means that custom filters will see the request before it's
    // been validated by the security filter.
    val securityFilter = new HttpSecurityFilter(conf, securityMgr)
    val holder = new FilterHolder(securityFilter)
    handler.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
  }

}

/**
 * A Jetty handler to handle redirects to a proxy server. It intercepts redirects and rewrites the
 * location to point to the proxy server.
 *
 * The handler needs to be set as the server's handler, because Jetty sometimes generates redirects
 * before invoking any servlet handlers or filters. One of such cases is when asking for the root of
 * a servlet context without the trailing slash (e.g. "/jobs") - Jetty will send a redirect to the
 * same URL, but with a trailing slash.
 */
private class ProxyRedirectHandler(_proxyUri: String) extends HandlerWrapper {

  private val proxyUri = _proxyUri.stripSuffix("/")

  override def handle(
      target: String,
      baseRequest: Request,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    super.handle(target, baseRequest, request, new ResponseWrapper(request, response))
  }

  private class ResponseWrapper(
      req: HttpServletRequest,
      res: HttpServletResponse)
    extends HttpServletResponseWrapper(res) {

    override def sendRedirect(location: String): Unit = {
      val newTarget = if (location != null) {
        val target = new URI(location)
        // The target path should already be encoded, so don't re-encode it, just the
        // proxy address part.
        val proxyBase = UIUtils.uiRoot(req)
        val proxyPrefix = if (proxyBase.nonEmpty) s"$proxyUri$proxyBase" else proxyUri
        s"${res.encodeURL(proxyPrefix)}${target.getPath()}"
      } else {
        null
      }
      super.sendRedirect(newTarget)
    }
  }

}
