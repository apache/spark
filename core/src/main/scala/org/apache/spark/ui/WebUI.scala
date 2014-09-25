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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
private[spark] abstract class WebUI(
    securityManager: SecurityManager,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "")
  extends Logging {

  protected val tabs = ArrayBuffer[WebUITab]()
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  protected var serverInfo: Option[ServerInfo] = None
  protected val localHostName = Utils.localHostName()
  protected val publicHostName = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(localHostName)
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
  def getTabs: Seq[WebUITab] = tabs.toSeq
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq
  def getSecurityManager: SecurityManager = securityManager

  /** Attach a tab to this UI, along with all of its attached pages. */
  def attachTab(tab: WebUITab) {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** Attach a page to this UI. */
  def attachPage(page: WebUIPage) {
    val pagePath = "/" + page.prefix
    attachHandler(createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, basePath))
    attachHandler(createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, basePath))
  }

  /** Attach a handler to this UI. */
  def attachHandler(handler: ServletContextHandler) {
    handlers += handler
    serverInfo.foreach { info =>
      info.rootHandler.addHandler(handler)
      if (!handler.isStarted) {
        handler.start()
      }
    }
  }

  /** Detach a handler from this UI. */
  def detachHandler(handler: ServletContextHandler) {
    handlers -= handler
    serverInfo.foreach { info =>
      info.rootHandler.removeHandler(handler)
      if (handler.isStarted) {
        handler.stop()
      }
    }
  }

  /** Initialize all components of the server. */
  def initialize()

  /** Bind to the HTTP server behind this web interface. */
  def bind() {
    assert(!serverInfo.isDefined, "Attempted to bind %s more than once!".format(className))
    try {
      serverInfo = Some(startJettyServer("0.0.0.0", port, handlers, conf, name))
      logInfo("Started %s at http://%s:%d".format(className, publicHostName, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to bind %s".format(className), e)
        System.exit(1)
    }
  }

  /** Return the actual port to which this server is bound. Only valid after bind(). */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind(). */
  def stop() {
    assert(serverInfo.isDefined,
      "Attempted to stop %s before binding to a server!".format(className))
    serverInfo.get.server.stop()
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
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI. */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  def basePath: String = parent.getBasePath
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
