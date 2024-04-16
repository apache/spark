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

import java.util.EnumSet

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import jakarta.servlet.DispatcherType
import jakarta.servlet.http.{HttpServlet, HttpServletRequest}
import org.eclipse.jetty.servlet.{FilterHolder, FilterMapping, ServletContextHandler, ServletHolder}
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.CLASS_NAME
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,
    val sslOptions: SSLOptions,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "",
    poolSize: Int = 200)
  extends Logging {

  protected val tabs = ArrayBuffer[WebUITab]()
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  protected var serverInfo: Option[ServerInfo] = None
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
  protected val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
  def getTabs: Seq[WebUITab] = tabs.toSeq
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq

  def getDelegatingHandlers: Seq[DelegatingServletContextHandler] = {
    handlers.map(new DelegatingServletContextHandler(_)).toSeq
  }

  /** Attaches a tab to this UI, along with all of its attached pages. */
  def attachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** Detaches a tab from this UI, along with all of its attached pages. */
  def detachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  /** Detaches a page from this UI, along with all of its attached handlers. */
  def detachPage(page: WebUIPage): Unit = {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attaches a page to this UI. */
  def attachPage(page: WebUIPage): Unit = {
    val pagePath = "/" + page.prefix
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
    handlers += renderJsonHandler
  }

  /** Attaches a handler to this UI. */
  def attachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler, securityManager))
  }

  /** Attaches a handler to this UI. */
  def attachHandler(contextPath: String, httpServlet: HttpServlet, pathSpec: String): Unit = {
    val ctx = new ServletContextHandler()
    ctx.setContextPath(contextPath)
    ctx.addServlet(new ServletHolder(httpServlet), pathSpec)
    attachHandler(ctx)
  }

  /** Detaches a handler from this UI. */
  def detachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
   * Detaches the content handler at `path` URI.
   *
   * @param path Path in UI to unmount.
   */
  def detachHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /**
   * Adds a handler for static content.
   *
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  def addStaticHandler(resourceBase: String, path: String = "/static"): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /** A hook to initialize components of the UI */
  def initialize(): Unit

  def initServer(): ServerInfo = {
    val hostName = Option(conf.getenv("SPARK_LOCAL_IP"))
        .getOrElse(if (Utils.preferIPv6) "[::]" else "0.0.0.0")
    val server = startJettyServer(hostName, port, sslOptions, conf, name, poolSize)
    server
  }

  /** Binds to the HTTP server behind this web interface. */
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val server = initServer()
      handlers.foreach(server.addHandler(_, securityManager))
      serverInfo = Some(server)
      val hostName = Option(conf.getenv("SPARK_LOCAL_IP"))
          .getOrElse(if (Utils.preferIPv6) "[::]" else "0.0.0.0")
      logInfo(s"Bound $className to $hostName, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(log"Failed to bind ${MDC(CLASS_NAME, className)}", e)
        System.exit(1)
    }
  }

  /** @return Whether SSL enabled. Only valid after [[bind]]. */
  def isSecure: Boolean = serverInfo.map(_.securePort.isDefined).getOrElse(false)

  /** @return The scheme of web interface. Only valid after [[bind]]. */
  def scheme: String = if (isSecure) "https://" else "http://"

  /** @return The url of web interface. Only valid after [[bind]]. */
  def webUrl: String = s"${scheme}$publicHostName:${boundPort}"

  /** @return The actual port to which this server is bound. Only valid after [[bind]]. */
  def boundPort: Int = serverInfo.map(si => si.securePort.getOrElse(si.boundPort)).getOrElse(-1)

  /** Stops the server behind this web interface. Only valid after [[bind]]. */
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.foreach(_.stop())
  }
}


/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
 */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  val pages = ArrayBuffer[WebUIPage]()
  val name = prefix.capitalize

  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix. */
  def attachPage(page: WebUIPage): Unit = {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI sorted by displayOrder. */
  def headerTabs: Seq[WebUITab] = parent.getTabs.sortBy(_.displayOrder)

  def basePath: String = parent.getBasePath

  def displayOrder: Int = Integer.MIN_VALUE
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  def render(request: HttpServletRequest): Seq[Node]
  def renderJson(request: HttpServletRequest): JValue = JNothing
}

private[spark] class DelegatingServletContextHandler(handler: ServletContextHandler) {

  def prependFilterMapping(
      filterName: String,
      spec: String,
      types: EnumSet[DispatcherType]): Unit = {
    val mapping = new FilterMapping()
    mapping.setFilterName(filterName)
    mapping.setPathSpec(spec)
    mapping.setDispatcherTypes(types)
    handler.getServletHandler.prependFilterMapping(mapping)
  }

  def addFilter(
      filterName: String,
      className: String,
      filterParams: Map[String, String]): Unit = {
    val filterHolder = new FilterHolder()
    filterHolder.setName(filterName)
    filterHolder.setClassName(className)
    filterParams.foreach { case (k, v) => filterHolder.setInitParameter(k, v) }
    handler.getServletHandler.addFilter(filterHolder)
  }

  def filterCount(): Int = {
    handler.getServletHandler.getFilters.length
  }

  def getContextPath(): String = {
    handler.getContextPath
  }
}
